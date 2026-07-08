package com.streamflow.api.controller;

import com.streamflow.api.service.HistoryService;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * REST controller for historical metric and alert queries.
 *
 * <p>SPEC-18 R1/R2: exposes two endpoints:
 * <ul>
 *   <li>{@code GET /api/v1/streams/{streamId}/history} — metric snapshots from Cassandra.</li>
 *   <li>{@code GET /api/v1/streams/{streamId}/alerts}  — alert history from Cassandra.</li>
 * </ul>
 *
 * <p>SPEC-18 R5: Responses include:
 * <ul>
 *   <li>{@code Cache-Control: max-age=30} for all historical queries.</li>
 *   <li>{@code ETag} computed from query parameters + result size (weak ETag).</li>
 *   <li>HTTP 304 Not Modified when the {@code If-None-Match} header matches the current ETag.</li>
 * </ul>
 *
 * <p>SPEC-18 R3: If {@code to - from} exceeds {@code streamflow.history.max-range-hours},
 * the service throws {@link com.streamflow.api.exception.HistoryRangeException}, which the
 * {@link com.streamflow.api.exception.GlobalExceptionHandler} maps to HTTP 400 problem JSON.
 *
 * <p>{@link ConditionalOnBean} ensures the controller is inactive when Cassandra is not configured
 * (e.g. in integration tests that exclude Cassandra auto-configuration).
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/streams")
@RequiredArgsConstructor
@ConditionalOnBean(CassandraOperations.class)
@Tag(name = "History", description = "Historical metric and alert replay endpoints (Cassandra-backed)")
public class HistoryController {

    private final HistoryService historyService;

    /**
     * Returns historical metric snapshots for a stream.
     *
     * <p>SPEC-18 R1: query parameters {@code from}, {@code to} (epoch-ms), optional
     * {@code granularity} ({@code MINUTE} | {@code HOUR}).
     *
     * @param streamId    path variable — the stream ID
     * @param from        start of range (epoch milliseconds, inclusive)
     * @param to          end of range (epoch milliseconds, inclusive)
     * @param granularity resolution ({@code MINUTE} default, {@code HOUR} downsampled)
     * @param ifNoneMatch optional {@code If-None-Match} header for 304 support
     * @return 200 with ordered list, 304 if ETag matches, 400 if range exceeds max
     */
    @GetMapping("/{streamId}/history")
    @Operation(
            summary = "Get historical metric snapshots",
            description = "Returns minute- or hour-granularity metric snapshots from Cassandra " +
                          "for the requested stream and time window. Max range: 24 hours.",
            responses = {
                @ApiResponse(responseCode = "200", description = "Snapshots returned"),
                @ApiResponse(responseCode = "304", description = "Not modified (ETag matched)"),
                @ApiResponse(responseCode = "400", description = "Range exceeds maximum or invalid params")
            }
    )
    public ResponseEntity<List<StreamMetricSnapshotDTO>> getHistory(
            @Parameter(description = "Stream identifier, e.g. stream-001")
            @PathVariable("streamId") String streamId,

            @Parameter(description = "Start of range (epoch milliseconds, inclusive)")
            @RequestParam("from") long from,

            @Parameter(description = "End of range (epoch milliseconds, inclusive)")
            @RequestParam("to") long to,

            @Parameter(description = "Granularity: MINUTE (default) or HOUR")
            @RequestParam(value = "granularity", defaultValue = "MINUTE") String granularity,

            @Parameter(description = "If-None-Match header for conditional requests")
            @RequestParam(value = "If-None-Match", required = false) String ifNoneMatch) {

        log.debug("SPEC-18: GET history stream={} from={} to={} granularity={}", streamId, from, to, granularity);

        List<StreamMetricSnapshotDTO> results = historyService.getHistory(streamId, from, to, granularity);

        String etag = computeEtag(streamId, from, to, granularity, results.size());

        if (etag.equals(ifNoneMatch)) {
            return ResponseEntity.status(304).build();
        }

        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(30, TimeUnit.SECONDS))
                .eTag(etag)
                .body(results);
    }

    /**
     * Returns historical alerts for a stream.
     *
     * <p>SPEC-18 R2: query parameters {@code from}, {@code to} (epoch-ms), optional
     * {@code severity} filter ({@code CRITICAL} | {@code WARNING} | {@code INFO}).
     *
     * @param streamId     path variable — the stream ID
     * @param from         start of range (epoch milliseconds, inclusive)
     * @param to           end of range (epoch milliseconds, inclusive)
     * @param severity     optional severity filter; omit for all severities
     * @param ifNoneMatch  optional {@code If-None-Match} header for 304 support
     * @return 200 with alerts ordered DESC, 304 if ETag matches, 400 if range exceeds max
     */
    @GetMapping("/{streamId}/alerts")
    @Operation(
            summary = "Get historical alerts",
            description = "Returns alert history from Cassandra for the requested stream and " +
                          "time window, ordered newest-first. Optionally filtered by severity.",
            responses = {
                @ApiResponse(responseCode = "200", description = "Alerts returned"),
                @ApiResponse(responseCode = "304", description = "Not modified (ETag matched)"),
                @ApiResponse(responseCode = "400", description = "Range exceeds maximum or invalid params")
            }
    )
    public ResponseEntity<List<AlertEventDTO>> getAlerts(
            @Parameter(description = "Stream identifier, e.g. stream-001")
            @PathVariable("streamId") String streamId,

            @Parameter(description = "Start of range (epoch milliseconds, inclusive)")
            @RequestParam("from") long from,

            @Parameter(description = "End of range (epoch milliseconds, inclusive)")
            @RequestParam("to") long to,

            @Parameter(description = "Optional severity filter: CRITICAL, WARNING, or INFO")
            @RequestParam(value = "severity", required = false) String severity,

            @Parameter(description = "If-None-Match header for conditional requests")
            @RequestParam(value = "If-None-Match", required = false) String ifNoneMatch) {

        log.debug("SPEC-18: GET alerts stream={} from={} to={} severity={}", streamId, from, to, severity);

        List<AlertEventDTO> results = historyService.getAlerts(streamId, from, to, severity);

        String etag = computeEtag(streamId, from, to, severity != null ? severity : "ALL", results.size());

        if (etag.equals(ifNoneMatch)) {
            return ResponseEntity.status(304).build();
        }

        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(30, TimeUnit.SECONDS))
                .eTag(etag)
                .body(results);
    }

    // ── ETag helpers ────────────────────────────────────────────────────────

    /**
     * Computes a weak ETag for a historical query result.
     *
     * <p>SPEC-18 R5: ETag is a 16-character hex prefix of the SHA-256 hash of the
     * concatenation of query parameters and result count. This is cheap to compute
     * and deterministic — the same query with the same result count produces the
     * same ETag, enabling 304 responses for unchanged data.
     *
     * @param streamId    stream ID
     * @param from        start of range (epoch-ms)
     * @param to          end of range (epoch-ms)
     * @param variant     granularity or severity string
     * @param resultCount number of items in the result
     * @return weak ETag string, e.g. {@code W/"a1b2c3d4e5f6a7b8"}
     */
    static String computeEtag(String streamId, long from, long to, String variant, int resultCount) {
        String input = streamId + ":" + from + ":" + to + ":" + variant + ":" + resultCount;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            String hex = HexFormat.of().formatHex(hash);
            return "W/\"" + hex.substring(0, 16) + "\"";
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be present in all Java implementations
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
