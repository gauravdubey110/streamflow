package com.streamflow.common.dto;

import java.util.Map;

/**
 * WebSocket push payload and {@code metrics-aggregated} Kafka message.
 *
 * <p>Matches the JSON schema in StreamFlow Project Plan §5.
 * Camel-case field names are the JSON wire names (Jackson default).
 *
 * <p>{@code qualityDistribution} maps quality tier strings (e.g. {@code "1080p"}) to
 * their percentage of current viewers (e.g. {@code 45.2}).
 *
 * <p>{@code circuitBreakerState} carries the Resilience4j CB state as a string
 * ({@code "CLOSED"}, {@code "OPEN"}, or {@code "HALF_OPEN"}).
 */
public record StreamMetricSnapshotDTO(
        String streamId,
        String streamName,
        long liveViewerCount,
        long viewerDelta,
        double bufferRatePct,
        int p95LatencyMs,
        Map<String, Double> qualityDistribution,
        double healthScore,
        String circuitBreakerState,
        int activeAlerts,
        long snapshotTs
) {}
