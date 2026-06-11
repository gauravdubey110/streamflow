package com.streamflow.api.controller;

import com.streamflow.api.dto.StreamSummaryDTO;
import com.streamflow.api.service.StreamService;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for stream-level operations.
 *
 * <p>SPEC-06 R3/R4: Implements the endpoints:
 * <ul>
 *   <li>{@code GET /api/v1/streams} — list all active streams</li>
 *   <li>{@code GET /api/v1/streams/{streamId}} — full snapshot for one stream</li>
 * </ul>
 *
 * <p>All other stream endpoints ({@code /history}, {@code /alerts}, {@code /chaos})
 * are added in SPEC-11, SPEC-13, and SPEC-18.
 */
@RestController
@RequestMapping("/api/v1/streams")
@RequiredArgsConstructor
@Tag(name = "Streams", description = "Live stream metric snapshot endpoints")
public class StreamController {

    private final StreamService streamService;

    /**
     * Returns a summary of every active stream.
     *
     * <p>SPEC-06 R3: Reads {@code SMEMBERS active_streams} from Redis, then
     * fetches the snapshot JSON for each stream ID.
     *
     * @return HTTP 200 with {@code List<StreamSummaryDTO>}; empty list when no streams are active
     */
    @GetMapping
    @Operation(
            summary = "List all active streams",
            description = "Returns a summary for every stream currently tracked in Redis.",
            responses = @ApiResponse(responseCode = "200", description = "Stream list returned")
    )
    public ResponseEntity<List<StreamSummaryDTO>> listStreams() {
        return ResponseEntity.ok(streamService.listStreams());
    }

    /**
     * Returns the full metric snapshot for a single stream.
     *
     * <p>SPEC-06 R4: Returns HTTP 404 if no snapshot key exists in Redis
     * (mapped by {@link com.streamflow.api.exception.GlobalExceptionHandler}).
     *
     * @param streamId the stream identifier (e.g. {@code stream-001})
     * @return HTTP 200 with {@link StreamMetricSnapshotDTO}
     */
    @GetMapping("/{streamId}")
    @Operation(
            summary = "Get a single stream snapshot",
            description = "Returns the most recent metric snapshot from Redis. " +
                          "Returns 404 if the stream is not active (snapshot expired).",
            responses = {
                @ApiResponse(responseCode = "200", description = "Snapshot returned"),
                @ApiResponse(responseCode = "404", description = "Stream not found or snapshot expired")
            }
    )
    public ResponseEntity<StreamMetricSnapshotDTO> getStream(
            @Parameter(description = "Stream identifier, e.g. stream-001")
            @PathVariable("streamId") String streamId) {
        return ResponseEntity.ok(streamService.getStream(streamId));
    }
}
