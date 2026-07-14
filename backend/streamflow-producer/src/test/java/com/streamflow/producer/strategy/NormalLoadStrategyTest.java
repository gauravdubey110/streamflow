package com.streamflow.producer.strategy;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link NormalLoadStrategy}.
 *
 * <p>SPEC-10 R1 event distribution (updated from SPEC-03 R4):
 *
 * <ul>
 *   <li>65% JOIN
 *   <li>25% DROP
 *   <li>5% QUALITY_SWITCH
 *   <li>5% BUFFER_START — carries bufferDurationMs in [500, 3000] ms
 * </ul>
 *
 * <p>Generates 10,000 events and asserts each type falls within a 5%-point tolerance of the
 * expected weight.
 */
class NormalLoadStrategyTest {

  private static final int SAMPLE_SIZE = 10_000;
  private static final String STREAM_ID = "stream-001";
  private static final String STREAM_NAME = "Test Stream";

  private final NormalLoadStrategy strategy = new NormalLoadStrategy();

  @Test
  void generate_producesEventsWithCorrectFieldsPopulated() {
    // Generate enough events to ensure we hit a BUFFER_START (5% rate)
    ViewerEventDTO bufferEvent = null;
    ViewerEventDTO nonBufferEvent = null;
    for (int i = 0; i < 500; i++) {
      ViewerEventDTO e = strategy.generate(STREAM_ID, STREAM_NAME);
      if (e.eventType() == EventType.BUFFER_START && bufferEvent == null) {
        bufferEvent = e;
      } else if (e.eventType() != EventType.BUFFER_START && nonBufferEvent == null) {
        nonBufferEvent = e;
      }
      if (bufferEvent != null && nonBufferEvent != null) break;
    }

    // Common field assertions — applies to all event types
    for (ViewerEventDTO event : new ViewerEventDTO[] {bufferEvent, nonBufferEvent}) {
      if (event == null) continue;
      assertThat(event.eventId()).isNotBlank();
      assertThat(event.streamId()).isEqualTo(STREAM_ID);
      assertThat(event.viewerId()).isNotBlank();
      assertThat(event.eventType()).isNotNull();
      assertThat(event.quality()).isNotNull();
      assertThat(event.timestamp()).isGreaterThan(0L);
      assertThat(event.region()).isNotBlank();
    }

    // SPEC-10 R1: BUFFER_START carries bufferDurationMs in [500, 3000] ms
    if (bufferEvent != null) {
      assertThat(bufferEvent.bufferDurationMs())
          .as("BUFFER_START should carry bufferDurationMs in [500, 3000]")
          .isNotNull()
          .isBetween(
              NormalLoadStrategy.BUFFER_DURATION_MIN_MS, NormalLoadStrategy.BUFFER_DURATION_MAX_MS);
    }

    // Non-BUFFER_START events should have null bufferDurationMs
    if (nonBufferEvent != null) {
      assertThat(nonBufferEvent.bufferDurationMs())
          .as("Non-BUFFER_START events should have null bufferDurationMs")
          .isNull();
    }
  }

  @Test
  void generate_distributionMatchesSpecWeights() {
    Map<EventType, Integer> counts = new EnumMap<>(EventType.class);
    for (EventType t : EventType.values()) counts.put(t, 0);

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
      counts.merge(event.eventType(), 1, Integer::sum);
    }

    double joinPct = 100.0 * counts.get(EventType.JOIN) / SAMPLE_SIZE;
    double dropPct = 100.0 * counts.get(EventType.DROP) / SAMPLE_SIZE;
    double switchPct = 100.0 * counts.get(EventType.QUALITY_SWITCH) / SAMPLE_SIZE;
    double bufferStartPct = 100.0 * counts.get(EventType.BUFFER_START) / SAMPLE_SIZE;

    // SPEC-10 R1 distribution: 65% JOIN, 25% DROP, 5% QUALITY_SWITCH, 5% BUFFER_START
    assertThat(joinPct).as("JOIN percentage should be 65% ±5 (SPEC-10 R1)").isBetween(60.0, 70.0);
    assertThat(dropPct).as("DROP percentage should be 25% ±5 (SPEC-10 R1)").isBetween(20.0, 30.0);
    assertThat(switchPct)
        .as("QUALITY_SWITCH percentage should be 5% ±3 (SPEC-10 R1)")
        .isBetween(2.0, 8.0);
    assertThat(bufferStartPct)
        .as("BUFFER_START percentage should be 5% ±3 (SPEC-10 R1)")
        .isBetween(2.0, 8.0);

    // BUFFER_END and ERROR are not produced by NormalLoadStrategy
    assertThat(counts.get(EventType.BUFFER_END)).isZero();
    assertThat(counts.get(EventType.ERROR)).isZero();
  }

  @Test
  void generate_eventIdsAreUnique() {
    // Generate 1000 events — all eventIds should be distinct UUIDs
    long distinctCount =
        java.util.stream.IntStream.range(0, 1000)
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

  /**
   * SPEC-10 R1: every BUFFER_START event must carry a bufferDurationMs value in the range [{@link
   * NormalLoadStrategy#BUFFER_DURATION_MIN_MS}, {@link NormalLoadStrategy#BUFFER_DURATION_MAX_MS}].
   *
   * <p>Generates events until 10 BUFFER_START events are found and asserts the range constraint on
   * each.
   */
  @Test
  void generate_bufferStartCarriesBufferDurationMsInRange() {
    int found = 0;
    int attempts = 0;
    while (found < 10 && attempts < 2_000) {
      ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
      attempts++;
      if (event.eventType() == EventType.BUFFER_START) {
        assertThat(event.bufferDurationMs())
            .as("BUFFER_START must have non-null bufferDurationMs (SPEC-10 R1)")
            .isNotNull();
        assertThat(event.bufferDurationMs())
            .as("bufferDurationMs must be >= %d ms", NormalLoadStrategy.BUFFER_DURATION_MIN_MS)
            .isGreaterThanOrEqualTo(NormalLoadStrategy.BUFFER_DURATION_MIN_MS);
        assertThat(event.bufferDurationMs())
            .as("bufferDurationMs must be <= %d ms", NormalLoadStrategy.BUFFER_DURATION_MAX_MS)
            .isLessThanOrEqualTo(NormalLoadStrategy.BUFFER_DURATION_MAX_MS);
        found++;
      }
    }
    assertThat(found)
        .as("Should find at least 10 BUFFER_START events within 2000 attempts at 5%% rate")
        .isGreaterThanOrEqualTo(10);
  }
}
