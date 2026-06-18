package com.streamflow.producer.rest;

import com.streamflow.producer.chaos.ChaosInjector;
import com.streamflow.producer.chaos.ChaosScenario;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Internal REST endpoint used exclusively by the API gateway to forward chaos commands.
 *
 * <p>SPEC-13 R3: exposed on port 8081 (the producer's {@code server.port}).
 * Not intended for direct client access — treated as an internal service-to-service API.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code POST /internal/chaos} — starts a chaos session; returns {@code 202 Accepted}
 *       with {@code {"chaosId": "...", "startsAt": epochMs}}.</li>
 *   <li>{@code DELETE /internal/chaos/{chaosId}} — cancels chaos; returns {@code 204 No Content}
 *       or {@code 404} if not found.</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/internal/chaos")
@RequiredArgsConstructor
public class InternalChaosController {

    private final ChaosInjector chaosInjector;

    /**
     * Starts a chaos session.
     *
     * @param request chaos parameters (validated)
     * @return 202 Accepted with chaosId + startsAt timestamp
     */
    @PostMapping
    public ResponseEntity<ChaosStartResponse> startChaos(@Valid @RequestBody ChaosStartRequest request) {
        log.info("InternalChaosController.startChaos: stream={} scenario={} duration={}s",
                request.streamId(), request.scenario(), request.durationSeconds());

        String chaosId = chaosInjector.start(
                request.scenario(),
                request.streamId(),
                request.durationSeconds()
        );

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(new ChaosStartResponse(chaosId, System.currentTimeMillis()));
    }

    /**
     * Cancels an active chaos session.
     *
     * @param chaosId the chaos session identifier returned by {@link #startChaos}
     * @return 204 No Content on success; 404 if not found
     */
    @DeleteMapping("/{chaosId}")
    public ResponseEntity<Void> cancelChaos(@PathVariable("chaosId") String chaosId) {
        log.info("InternalChaosController.cancelChaos: chaosId={}", chaosId);
        boolean found = chaosInjector.cancel(chaosId);
        return found
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    // ── exception handling ────────────────────────────────────────────────────

    /**
     * Maps {@link IllegalArgumentException} (e.g. invalid enum value) to 400 Bad Request.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Invalid Chaos Request");
        problem.setProperty("timestamp", Instant.now().toEpochMilli());
        return problem;
    }

    // ── request / response records ────────────────────────────────────────────

    /**
     * Chaos start request body.
     *
     * <p>SPEC-13 R6: validated by Bean Validation:
     * <ul>
     *   <li>{@code scenario} — must be a valid {@link ChaosScenario} name.</li>
     *   <li>{@code durationSeconds} — 1–300.</li>
     *   <li>{@code streamId} — must not be blank.</li>
     * </ul>
     */
    public record ChaosStartRequest(
            @NotBlank(message = "streamId must not be blank")
            String streamId,

            @NotNull(message = "scenario must not be null")
            ChaosScenario scenario,

            @Min(value = 1, message = "durationSeconds must be at least 1")
            @Max(value = 300, message = "durationSeconds must not exceed 300")
            int durationSeconds
    ) {}

    /**
     * Chaos start response body.
     *
     * @param chaosId  unique identifier for this chaos session
     * @param startsAt epoch-millis when the chaos session was started
     */
    public record ChaosStartResponse(
            String chaosId,
            long startsAt
    ) {}
}
