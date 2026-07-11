package com.streamflow.api.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Centralised Micrometer counters for the API gateway module.
 *
 * <p>SPEC-20 R3 — {@code streamflow.events.consumed} counter with a {@code topic} tag:
 * <ul>
 *   <li>tag {@code topic=metrics-aggregated} — incremented by
 *       {@link com.streamflow.api.websocket.MetricsPushConsumer}</li>
 *   <li>tag {@code topic=alerts} — incremented by
 *       {@link com.streamflow.api.websocket.AlertPushConsumer}</li>
 * </ul>
 *
 * <p>SPEC-20 NFR2 — Cardinality is bounded: the {@code topic} tag has a fixed set of values
 * (2 topics) so series cardinality is well within the 5 000-series limit.
 */
@Component
public class ApiMetrics {

    /** Metric name for consumed-events counter (SPEC-20 R3). */
    public static final String EVENTS_CONSUMED = "streamflow.events.consumed";

    /** Tag key distinguishing which Kafka topic the events were consumed from (SPEC-20 R3). */
    public static final String TAG_TOPIC = "topic";

    private final Counter metricsEventsConsumed;
    private final Counter alertEventsConsumed;

    public ApiMetrics(MeterRegistry registry) {
        this.metricsEventsConsumed = Counter.builder(EVENTS_CONSUMED)
                .description("Number of events consumed from Kafka by the API gateway")
                .tag(TAG_TOPIC, "metrics-aggregated")
                .register(registry);

        this.alertEventsConsumed = Counter.builder(EVENTS_CONSUMED)
                .description("Number of events consumed from Kafka by the API gateway")
                .tag(TAG_TOPIC, "alerts")
                .register(registry);
    }

    /**
     * Increments the {@code metrics-aggregated} consumed counter.
     * Called from {@link com.streamflow.api.websocket.MetricsPushConsumer}.
     */
    public void incrementMetricsConsumed() {
        metricsEventsConsumed.increment();
    }

    /**
     * Increments the {@code alerts} consumed counter.
     * Called from {@link com.streamflow.api.websocket.AlertPushConsumer}.
     */
    public void incrementAlertsConsumed() {
        alertEventsConsumed.increment();
    }
}
