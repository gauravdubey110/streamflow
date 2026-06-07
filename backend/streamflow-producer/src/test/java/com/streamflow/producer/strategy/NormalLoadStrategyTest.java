package com.streamflow.producer.strategy;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link NormalLoadStrategy}.
 *
 * <p>SPEC-03 Test Plan — Unit: strategy distribution (Chi-square style sanity check).
 * Generates 10,000 events and asserts each event type falls within a 5%-point
 * tolerance of the expected weight.
 */
class NormalLoadStrategyTest {

    private static final int SAMPLE_SIZE = 10_000;
    private static final String STREAM_ID = "stream-001";
    private static final String STREAM_NAME = "Test Stream";

    private final NormalLoadStrategy strategy = new NormalLoadStrategy();

    @Test
    void generate_producesEventsWithCorrectFieldsPopulated() {
        ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);

        assertThat(event.eventId()).isNotBlank();
        assertThat(event.streamId()).isEqualTo(STREAM_ID);
        assertThat(event.viewerId()).isNotBlank();
        assertThat(event.eventType()).isNotNull();
        assertThat(event.quality()).isNotNull();
        assertThat(event.timestamp()).isGreaterThan(0L);
        assertThat(event.region()).isNotBlank();
        // bufferDurationMs is null for JOIN/DROP/QUALITY_SWITCH (NormalLoadStrategy)
        assertThat(event.bufferDurationMs()).isNull();
    }

    @Test
    void generate_distributionMatchesSpecWeights() {
        Map<EventType, Integer> counts = new EnumMap<>(EventType.class);
        for (EventType t : EventType.values()) counts.put(t, 0);

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
            counts.merge(event.eventType(), 1, Integer::sum);
        }

        double joinPct    = 100.0 * counts.get(EventType.JOIN)           / SAMPLE_SIZE;
        double dropPct    = 100.0 * counts.get(EventType.DROP)           / SAMPLE_SIZE;
        double switchPct  = 100.0 * counts.get(EventType.QUALITY_SWITCH) / SAMPLE_SIZE;

        // SPEC-03 R4: 70% JOIN ±5%, 25% DROP ±5%, 5% QUALITY_SWITCH ±3%
        assertThat(joinPct)
                .as("JOIN percentage should be 70% ±5")
                .isBetween(65.0, 75.0);
        assertThat(dropPct)
                .as("DROP percentage should be 25% ±5")
                .isBetween(20.0, 30.0);
        assertThat(switchPct)
                .as("QUALITY_SWITCH percentage should be 5% ±3")
                .isBetween(2.0, 8.0);

        // BUFFER_START, BUFFER_END, ERROR are not produced by NormalLoadStrategy
        assertThat(counts.get(EventType.BUFFER_START)).isZero();
        assertThat(counts.get(EventType.BUFFER_END)).isZero();
        assertThat(counts.get(EventType.ERROR)).isZero();
    }

    @Test
    void generate_eventIdsAreUnique() {
        // Generate 1000 events — all eventIds should be distinct UUIDs
        long distinctCount = java.util.stream.IntStream.range(0, 1000)
                .mapToObj(i -> strategy.generate(STREAM_ID, STREAM_NAME))
                .map(ViewerEventDTO::eventId)
                .distinct()
                .count();
        assertThat(distinctCount).isEqualTo(1000);
    }

    @Test
    void generate_streamIdMatchesInput() {
        for (int i = 0; i < 100; i++) {
            ViewerEventDTO event = strategy.generate("stream-XYZ", STREAM_NAME);
            assertThat(event.streamId()).isEqualTo("stream-XYZ");
        }
    }
}
