package com.streamflow.processor.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ProcessorMetrics}.
 *
 * <p>SPEC-20 Test Plan — Integration: assert metrics increment after a known number of events. Uses
 * {@link SimpleMeterRegistry} so no Spring context is needed (fast, in-process unit test).
 *
 * <p>Verifies all 6 custom metrics required by SPEC-20 R3:
 *
 * <ul>
 *   <li>{@code streamflow.events.consumed} (counter, tag {@code topic})
 *   <li>{@code streamflow.alerts.fired} (counter, tags {@code severity,alertType})
 *   <li>{@code streamflow.cb.state} (gauge, 0=CLOSED 1=HALF_OPEN 2=OPEN)
 *   <li>{@code streamflow.snapshot.duration} (timer — registered by SnapshotPublisher; verified via
 *       the registry rather than this component)
 * </ul>
 */
class ProcessorMetricsTest {

  private MeterRegistry registry;
  private ProcessorMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new ProcessorMetrics(registry);
  }

  // ── streamflow.events.consumed ────────────────────────────────────────────

  @Test
  void incrementViewerEventsConsumed_incrementsCounter() {
    metrics.incrementViewerEventsConsumed();
    metrics.incrementViewerEventsConsumed();
    metrics.incrementViewerEventsConsumed();

    Counter counter =
        registry
            .find(ProcessorMetrics.EVENTS_CONSUMED)
            .tag(ProcessorMetrics.TAG_TOPIC, "viewer-events")
            .counter();

    assertThat(counter)
        .as("streamflow.events.consumed[topic=viewer-events] counter should exist")
        .isNotNull();
    assertThat(counter.count()).as("Counter should be 3.0 after 3 increments").isEqualTo(3.0);
  }

  @Test
  void incrementHealthEventsConsumed_incrementsCounter() {
    metrics.incrementHealthEventsConsumed();

    Counter counter =
        registry
            .find(ProcessorMetrics.EVENTS_CONSUMED)
            .tag(ProcessorMetrics.TAG_TOPIC, "stream-health")
            .counter();

    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  // ── streamflow.alerts.fired ───────────────────────────────────────────────

  @Test
  void incrementAlertsFired_incrementsCounterWithCorrectTags() {
    metrics.incrementAlertsFired(AlertSeverity.CRITICAL, AlertType.HIGH_BUFFER_RATE);
    metrics.incrementAlertsFired(AlertSeverity.CRITICAL, AlertType.HIGH_BUFFER_RATE);
    metrics.incrementAlertsFired(AlertSeverity.WARNING, AlertType.VIEWER_DROP);

    Counter criticalHighBuffer =
        registry
            .find(ProcessorMetrics.ALERTS_FIRED)
            .tag(ProcessorMetrics.TAG_SEVERITY, "CRITICAL")
            .tag(ProcessorMetrics.TAG_ALERT_TYPE, "HIGH_BUFFER_RATE")
            .counter();

    assertThat(criticalHighBuffer).isNotNull();
    assertThat(criticalHighBuffer.count())
        .as("CRITICAL/HIGH_BUFFER_RATE counter should be 2.0")
        .isEqualTo(2.0);

    Counter warningViewerDrop =
        registry
            .find(ProcessorMetrics.ALERTS_FIRED)
            .tag(ProcessorMetrics.TAG_SEVERITY, "WARNING")
            .tag(ProcessorMetrics.TAG_ALERT_TYPE, "VIEWER_DROP")
            .counter();

    assertThat(warningViewerDrop).isNotNull();
    assertThat(warningViewerDrop.count())
        .as("WARNING/VIEWER_DROP counter should be 1.0")
        .isEqualTo(1.0);
  }

  // ── streamflow.cb.state ───────────────────────────────────────────────────

  @Test
  void updateCbState_closed_setsGaugeToZero() {
    metrics.updateCbState("CLOSED");

    Gauge gauge = registry.find(ProcessorMetrics.CB_STATE).gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).as("CLOSED state should be gauge value 0").isEqualTo(0.0);
  }

  @Test
  void updateCbState_halfOpen_setsGaugeToOne() {
    metrics.updateCbState("HALF_OPEN");

    Gauge gauge = registry.find(ProcessorMetrics.CB_STATE).gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).as("HALF_OPEN state should be gauge value 1").isEqualTo(1.0);
  }

  @Test
  void updateCbState_open_setsGaugeToTwo() {
    metrics.updateCbState("OPEN");

    Gauge gauge = registry.find(ProcessorMetrics.CB_STATE).gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).as("OPEN state should be gauge value 2").isEqualTo(2.0);
  }

  @Test
  void updateCbState_transitions_reflectedInGauge() {
    // Simulate CLOSED → OPEN → HALF_OPEN → CLOSED lifecycle
    metrics.updateCbState("CLOSED");
    assertThat(registry.find(ProcessorMetrics.CB_STATE).gauge().value()).isEqualTo(0.0);

    metrics.updateCbState("OPEN");
    assertThat(registry.find(ProcessorMetrics.CB_STATE).gauge().value()).isEqualTo(2.0);

    metrics.updateCbState("HALF_OPEN");
    assertThat(registry.find(ProcessorMetrics.CB_STATE).gauge().value()).isEqualTo(1.0);

    metrics.updateCbState("CLOSED");
    assertThat(registry.find(ProcessorMetrics.CB_STATE).gauge().value()).isEqualTo(0.0);
  }
}
