package com.streamflow.common;

import com.streamflow.common.constants.KafkaTopics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SPEC-02: AC2 — verifies KafkaTopics constants exactly match the strings from
 * StreamFlow Project Plan §6.
 */
class KafkaTopicsTest {

    @Test
    @DisplayName("AC2: VIEWER_EVENTS matches plan §6")
    void viewerEvents_constantValue() {
        assertEquals("viewer-events", KafkaTopics.VIEWER_EVENTS);
    }

    @Test
    @DisplayName("AC2: STREAM_HEALTH matches plan §6")
    void streamHealth_constantValue() {
        assertEquals("stream-health", KafkaTopics.STREAM_HEALTH);
    }

    @Test
    @DisplayName("AC2: ALERTS matches plan §6")
    void alerts_constantValue() {
        assertEquals("alerts", KafkaTopics.ALERTS);
    }

    @Test
    @DisplayName("AC2: METRICS_AGGREGATED matches plan §6")
    void metricsAggregated_constantValue() {
        assertEquals("metrics-aggregated", KafkaTopics.METRICS_AGGREGATED);
    }

    @Test
    @DisplayName("SPEC-14 R3: CB_EVENTS constant value is 'cb-events'")
    void cbEvents_constantValue() {
        assertEquals("cb-events", KafkaTopics.CB_EVENTS);
    }
}
