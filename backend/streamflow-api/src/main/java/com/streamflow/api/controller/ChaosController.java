package com.streamflow.api.controller;

import com.streamflow.api.chaos.ChaosRequest;
import com.streamflow.api.chaos.ChaosResponse;
import com.streamflow.api.chaos.ProducerChaosClient;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Public REST endpoint for chaos injection.
 *
 * <p>SPEC-13 R4: forwards requests to the producer's internal API via {@link ProducerChaosClient}.
 *
 * <h3>Endpoints</h3>
 *
 * <ul>
 *   <li>{@code POST /api/v1/streams/{streamId}/chaos} — starts a chaos session; returns {@code 202
 *       Accepted} with a {@link ChaosResponse}.
 *   <li>{@code DELETE /api/v1/streams/{streamId}/chaos/{chaosId}} — cancels an active chaos
 *       session; returns {@code 204 No Content}.
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/streams/{streamId}/chaos")
@RequiredArgsConstructor
@Tag(
    name = "Chaos",
    description = "Chaos injection endpoints — trigger and cancel degradation scenarios")
public class ChaosController {

  private final ProducerChaosClient producerChaosClient;

  /**
   * Starts a chaos scenario for the specified stream.
   *
   * <p>SPEC-13 AC1: returns 202 Accepted with {@code {chaosId, startsAt}}.
   *
   * @param streamId the target stream identifier
   * @param request chaos parameters (validated)
   * @return 202 Accepted with chaosId and startsAt
   */
  @PostMapping
  @Operation(
      summary = "Inject chaos into a stream",
      description =
          "Starts a degradation scenario on the specified stream. "
              + "Chaos auto-reverts after durationSeconds.",
      responses = {
        @ApiResponse(responseCode = "202", description = "Chaos session started"),
        @ApiResponse(responseCode = "400", description = "Invalid scenario or duration")
      })
  public ResponseEntity<ChaosResponse> startChaos(
      @PathVariable("streamId") String streamId, @Valid @RequestBody ChaosRequest request) {

    log.info(
        "ChaosController.startChaos: stream={} scenario={} duration={}s",
        streamId,
        request.scenario(),
        request.durationSeconds());

    ChaosResponse response =
        producerChaosClient.startChaos(streamId, request.scenario(), request.durationSeconds());

    return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
  }

  /**
   * Cancels an active chaos session.
   *
   * <p>SPEC-13 AC4: returns 204 on success; 404 if the session is not found or already expired.
   *
   * @param streamId the stream (for routing; actual lookup is by chaosId)
   * @param chaosId the chaos session identifier
   * @return 204 No Content or 404 Not Found
   */
  @DeleteMapping("/{chaosId}")
  @Operation(
      summary = "Cancel an active chaos session",
      description = "Stops chaos immediately; the stream reverts to normal behaviour.",
      responses = {
        @ApiResponse(responseCode = "204", description = "Chaos cancelled"),
        @ApiResponse(responseCode = "404", description = "Chaos session not found")
      })
  public ResponseEntity<Void> cancelChaos(
      @PathVariable("streamId") String streamId, @PathVariable("chaosId") String chaosId) {

    log.info("ChaosController.cancelChaos: stream={} chaosId={}", streamId, chaosId);

    boolean found = producerChaosClient.cancelChaos(chaosId);
    return found ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
  }
}
