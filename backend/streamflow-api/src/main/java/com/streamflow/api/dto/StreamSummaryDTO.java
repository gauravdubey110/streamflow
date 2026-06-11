package com.streamflow.api.dto;

/**
 * Thin projection of a stream's current state, returned by {@code GET /api/v1/streams}.
 *
 * <p>SPEC-06 §4: Built from the {@code stream_snapshot:{streamId}} Redis JSON.
 * {@code streamName} is stubbed to {@code streamId} until stream-metadata
 * persistence is introduced in a later spec.
 *
 * @param streamId            unique stream identifier (e.g. {@code stream-001})
 * @param streamName          human-readable name (stubbed to streamId in MVP)
 * @param liveViewerCount     current live viewer count from Redis snapshot
 * @param healthScore         composite health score (0–100)
 * @param activeAlerts        number of active alerts
 * @param circuitBreakerState Resilience4j CB state: {@code CLOSED}, {@code OPEN},
 *                            or {@code HALF_OPEN}
 */
public record StreamSummaryDTO(
        String streamId,
        String streamName,
        long liveViewerCount,
        double healthScore,
        int activeAlerts,
        String circuitBreakerState
) {}
