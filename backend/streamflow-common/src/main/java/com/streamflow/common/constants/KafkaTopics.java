package com.streamflow.common.constants;

/**
 * Kafka topic name constants shared across producer, processor, and API modules.
 *
 * <p>Topic names match the definitions in the StreamFlow Project Plan §6:
 * <ul>
 *   <li>{@code viewer-events}      — 6 partitions, 2-hour retention</li>
 *   <li>{@code stream-health}      — 3 partitions, 2-hour retention</li>
 *   <li>{@code alerts}             — 3 partitions, 24-hour retention</li>
 *   <li>{@code metrics-aggregated} — 3 partitions, 1-hour retention</li>
 *   <li>{@code cb-events}          — 3 partitions; circuit breaker state transitions
 *                                    (SPEC-14 R2/R3)</li>
 * </ul>
 *
 * <p>Instantiation is prohibited; use the constants directly.
 */
public final class KafkaTopics {

    /** Raw viewer interaction events (JOIN, DROP, QUALITY_SWITCH, etc.). */
    public static final String VIEWER_EVENTS = "viewer-events";

    /** Encoder/CDN health telemetry from the stream producer. */
    public static final String STREAM_HEALTH = "stream-health";

    /** Fired alert events from the alert engine. */
    public static final String ALERTS = "alerts";

    /**
     * Internal topic: aggregated metric snapshots from the stream processor
     * consumed by the API gateway for WebSocket push.
     */
    public static final String METRICS_AGGREGATED = "metrics-aggregated";

    /**
     * Circuit-breaker state-transition events published by the stream processor
     * and consumed by the API gateway for WebSocket push (SPEC-14 R2/R3).
     *
     * <p>3 partitions, keyed by {@code streamId} (or {@code "all"} for the global CB).
     */
    public static final String CB_EVENTS = "cb-events";

    private KafkaTopics() {
        // utility class — no instances
    }
}
