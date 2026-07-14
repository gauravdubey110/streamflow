package com.streamflow.processor.metrics;

import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Centralised Micrometer metrics for the processor module.
 *
 * <p>SPEC-20 R3 — exposes:
 *
 * <ul>
 *   <li>{@code streamflow.events.consumed} (counter, tag {@code topic}) — incremented by {@link
 *       com.streamflow.processor.consumer.ViewerEventConsumer} and {@link
 *       com.streamflow.processor.consumer.StreamHealthConsumer}.
 *   <li>{@code streamflow.alerts.fired} (counter, tags {@code severity,alertType}) — incremented by
 *       {@link com.streamflow.processor.alert.AlertEngine}.
 *   <li>{@code streamflow.cb.state} (gauge, 0=CLOSED 1=HALF_OPEN 2=OPEN) — updated by {@link
 *       com.streamflow.processor.circuitbreaker.AlertProcessorCircuitBreaker}.
 * </ul>
 *
 * <p>SPEC-20 NFR2 — Cardinality is bounded:
 *
 * <ul>
 *   <li>{@code topic} tag: 2 values (viewer-events, stream-health).
 *   <li>{@code severity} × {@code alertType}: 3 × 4 = 12 series maximum.
 *   <li>{@code streamflow.cb.state}: 1 series.
 * </ul>
 */
@Component
public class ProcessorMetrics {

  /** Metric name: events consumed from Kafka (SPEC-20 R3). */
  public static final String EVENTS_CONSUMED = "streamflow.events.consumed";

  /** Metric name: alerts fired by the alert engine (SPEC-20 R3). */
  public static final String ALERTS_FIRED = "streamflow.alerts.fired";

  /** Metric name: circuit-breaker state gauge (SPEC-20 R3). */
  public static final String CB_STATE = "streamflow.cb.state";

  /** Tag key for the Kafka topic name. */
  public static final String TAG_TOPIC = "topic";

  /** Tag key for alert severity. */
  public static final String TAG_SEVERITY = "severity";

  /** Tag key for alert type. */
  public static final String TAG_ALERT_TYPE = "alertType";

  /** CB state gauge value: CLOSED = 0, HALF_OPEN = 1, OPEN = 2 (SPEC-20 R3). */
  private final AtomicInteger cbStateValue = new AtomicInteger(0);

  private final Counter viewerEventsConsumed;
  private final Counter healthEventsConsumed;

  private final MeterRegistry registry;

  public ProcessorMetrics(MeterRegistry registry) {
    this.registry = registry;

    this.viewerEventsConsumed =
        Counter.builder(EVENTS_CONSUMED)
            .description("Number of events consumed from Kafka")
            .tag(TAG_TOPIC, "viewer-events")
            .register(registry);

    this.healthEventsConsumed =
        Counter.builder(EVENTS_CONSUMED)
            .description("Number of events consumed from Kafka")
            .tag(TAG_TOPIC, "stream-health")
            .register(registry);

    // SPEC-20 R3: CB state gauge — 0=CLOSED, 1=HALF_OPEN, 2=OPEN
    Gauge.builder(CB_STATE, cbStateValue, AtomicInteger::get)
        .description("Circuit breaker state: 0=CLOSED, 1=HALF_OPEN, 2=OPEN")
        .register(registry);
  }

  /**
   * Increments the {@code viewer-events} consumed counter. Called by {@link
   * com.streamflow.processor.consumer.ViewerEventConsumer}.
   */
  public void incrementViewerEventsConsumed() {
    viewerEventsConsumed.increment();
  }

  /**
   * Increments the {@code stream-health} consumed counter. Called by {@link
   * com.streamflow.processor.consumer.StreamHealthConsumer}.
   */
  public void incrementHealthEventsConsumed() {
    healthEventsConsumed.increment();
  }

  /**
   * Increments the {@code streamflow.alerts.fired} counter for the given severity and alert type
   * (SPEC-20 R3).
   *
   * <p>A new counter series is created per (severity, alertType) pair the first time this method is
   * called with those tags. With 3 severities × 4 alert types the maximum series count is 12, well
   * within the NFR2 cardinality guard.
   *
   * @param severity the alert severity (e.g. {@code AlertSeverity.CRITICAL})
   * @param alertType the alert type (e.g. {@code AlertType.HIGH_BUFFER_RATE})
   */
  public void incrementAlertsFired(AlertSeverity severity, AlertType alertType) {
    Counter.builder(ALERTS_FIRED)
        .description("Number of alerts fired by the alert engine")
        .tag(TAG_SEVERITY, severity.name())
        .tag(TAG_ALERT_TYPE, alertType.name())
        .register(registry)
        .increment();
  }

  /**
   * Updates the circuit-breaker state gauge (SPEC-20 R3).
   *
   * @param state the Resilience4j state string: {@code "CLOSED"}, {@code "HALF_OPEN"}, {@code
   *     "OPEN"}
   */
  public void updateCbState(String state) {
    int value =
        switch (state) {
          case "CLOSED" -> 0;
          case "HALF_OPEN" -> 1;
          case "OPEN" -> 2;
          default -> 0;
        };
    cbStateValue.set(value);
  }
}
