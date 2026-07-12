package com.streamflow.producer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProducerMetrics}.
 *
 * <p>SPEC-20 Test Plan — verifies that the {@code streamflow.events.published}
 * counter increments correctly for both topic tags. Uses {@link SimpleMeterRegistry}
 * so no Spring context is needed.
 */
class ProducerMetricsTest {

    private MeterRegistry registry;
    private ProducerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ProducerMetrics(registry);
    }

    @Test
    void incrementViewerEventsPublished_incrementsViewerEventsCounter() {
        metrics.incrementViewerEventsPublished();
        metrics.incrementViewerEventsPublished();

        Counter counter = registry.find(ProducerMetrics.EVENTS_PUBLISHED)
                .tag(ProducerMetrics.TAG_TOPIC, "viewer-events")
                .counter();

        assertThat(counter)
                .as("streamflow.events.published[topic=viewer-events] counter must exist")
                .isNotNull();
        assertThat(counter.count())
                .as("Counter should be 2.0 after 2 increments")
                .isEqualTo(2.0);
    }

    @Test
    void incrementHealthEventsPublished_incrementsHealthCounter() {
        metrics.incrementHealthEventsPublished();

        Counter counter = registry.find(ProducerMetrics.EVENTS_PUBLISHED)
                .tag(ProducerMetrics.TAG_TOPIC, "stream-health")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void viewerAndHealthCountersAreIndependent() {
        metrics.incrementViewerEventsPublished();
        metrics.incrementViewerEventsPublished();
        metrics.incrementHealthEventsPublished();

        double viewerCount = registry.find(ProducerMetrics.EVENTS_PUBLISHED)
                .tag(ProducerMetrics.TAG_TOPIC, "viewer-events")
                .counter().count();

        double healthCount = registry.find(ProducerMetrics.EVENTS_PUBLISHED)
                .tag(ProducerMetrics.TAG_TOPIC, "stream-health")
                .counter().count();

        assertThat(viewerCount).isEqualTo(2.0);
        assertThat(healthCount).isEqualTo(1.0);
    }
}
