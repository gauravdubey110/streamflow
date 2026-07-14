package com.streamflow.producer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Centralised Micrometer counters for the producer module.
 *
 * <p>SPEC-20 R3 — {@code streamflow.events.published} counter with a {@code topic} tag:
 *
 * <ul>
 *   <li>tag {@code topic=viewer-events} — incremented by {@link
 *       com.streamflow.producer.simulator.ViewerEventProducer}
 *   <li>tag {@code topic=stream-health} — incremented by {@link
 *       com.streamflow.producer.simulator.StreamHealthProducer}
 * </ul>
 *
 * <p>SPEC-20 NFR2 — Cardinality is bounded: the {@code topic} tag has a fixed set of values (2
 * topics) so series cardinality is well below the 5 000-series limit.
 */
@Component
public class ProducerMetrics {

  /** Metric name for published-events counter (SPEC-20 R3). */
  public static final String EVENTS_PUBLISHED = "streamflow.events.published";

  /** Tag key distinguishing which Kafka topic the events were sent to (SPEC-20 R3). */
  public static final String TAG_TOPIC = "topic";

  private final Counter viewerEventsPublished;
  private final Counter healthEventsPublished;

  public ProducerMetrics(MeterRegistry registry) {
    this.viewerEventsPublished =
        Counter.builder(EVENTS_PUBLISHED)
            .description("Number of events successfully published to Kafka")
            .tag(TAG_TOPIC, "viewer-events")
            .register(registry);

    this.healthEventsPublished =
        Counter.builder(EVENTS_PUBLISHED)
            .description("Number of events successfully published to Kafka")
            .tag(TAG_TOPIC, "stream-health")
            .register(registry);
  }

  /**
   * Increments the {@code viewer-events} published counter. Called from {@link
   * com.streamflow.producer.simulator.ViewerEventProducer} on each successful Kafka send (SPEC-20
   * R3).
   */
  public void incrementViewerEventsPublished() {
    viewerEventsPublished.increment();
  }

  /**
   * Increments the {@code stream-health} published counter. Called from {@link
   * com.streamflow.producer.simulator.StreamHealthProducer} on each successful Kafka send (SPEC-20
   * R3).
   */
  public void incrementHealthEventsPublished() {
    healthEventsPublished.increment();
  }
}
