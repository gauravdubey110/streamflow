package com.streamflow.producer.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.producer.chaos.ChaosInjector;
import com.streamflow.producer.chaos.ChaosScenario;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChaosAwareStrategy}.
 *
 * <p>SPEC-13 Test Plan: verifies scenario modifiers (STREAM_DOWN → null, VIEWER_DROP → high DROP
 * rate, HIGH_BUFFER → high BUFFER_START rate, BITRATE_SPIKE → delegates).
 */
class ChaosAwareStrategyTest {

  private ChaosInjector mockInjector;
  private NormalLoadStrategy delegate;
  private ChaosAwareStrategy strategy;

  private static final String STREAM_ID = "stream-001";
  private static final String STREAM_NAME = "Test Stream";

  @BeforeEach
  void setUp() {
    mockInjector = mock(ChaosInjector.class);
    delegate = new NormalLoadStrategy();
    strategy = new ChaosAwareStrategy(delegate, mockInjector);
  }

  @Test
  void generate_noActiveChaos_delegatesToNormalStrategy() {
    when(mockInjector.activeScenario(STREAM_ID)).thenReturn(Optional.empty());

    ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);

    assertThat(event).isNotNull();
    assertThat(event.streamId()).isEqualTo(STREAM_ID);
  }

  @Test
  void generate_streamDown_returnsNull() {
    when(mockInjector.activeScenario(STREAM_ID)).thenReturn(Optional.of(ChaosScenario.STREAM_DOWN));

    ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);

    assertThat(event).isNull();
  }

  @Test
  void generate_bitrateSpike_delegatesToNormalStrategy() {
    when(mockInjector.activeScenario(STREAM_ID))
        .thenReturn(Optional.of(ChaosScenario.BITRATE_SPIKE));

    // Should not return null (health producer handles BITRATE_SPIKE)
    ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);

    assertThat(event).isNotNull();
    assertThat(event.streamId()).isEqualTo(STREAM_ID);
  }

  @Test
  void generate_viewerDrop_producesHighDropRate() {
    when(mockInjector.activeScenario(STREAM_ID)).thenReturn(Optional.of(ChaosScenario.VIEWER_DROP));

    // Sample 500 events and assert DROP rate is above 40 % (expected ~50 %)
    long dropCount = 0;
    int samples = 500;
    for (int i = 0; i < samples; i++) {
      ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
      assertThat(event).isNotNull();
      if (event.eventType() == EventType.DROP) {
        dropCount++;
      }
    }
    double dropRate = (double) dropCount / samples;
    assertThat(dropRate)
        .as("VIEWER_DROP chaos should produce > 40%% DROP events (expected ~50%%)")
        .isGreaterThan(0.40);
  }

  @Test
  void generate_highBuffer_producesHighBufferRate() {
    when(mockInjector.activeScenario(STREAM_ID)).thenReturn(Optional.of(ChaosScenario.HIGH_BUFFER));

    // Sample 500 events and assert BUFFER_START rate is above 8 % (expected ~12 %)
    long bufferCount = 0;
    int samples = 500;
    for (int i = 0; i < samples; i++) {
      ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
      assertThat(event).isNotNull();
      if (event.eventType() == EventType.BUFFER_START) {
        bufferCount++;
      }
    }
    double bufferRate = (double) bufferCount / samples;
    assertThat(bufferRate)
        .as("HIGH_BUFFER chaos should produce > 8%% BUFFER_START events (expected ~12%%)")
        .isGreaterThan(0.08);
  }

  @Test
  void generate_highBuffer_bufferEventsHaveBufferDuration() {
    when(mockInjector.activeScenario(STREAM_ID)).thenReturn(Optional.of(ChaosScenario.HIGH_BUFFER));

    // Find at least one BUFFER_START event and verify it has bufferDurationMs
    for (int i = 0; i < 200; i++) {
      ViewerEventDTO event = strategy.generate(STREAM_ID, STREAM_NAME);
      if (event != null && event.eventType() == EventType.BUFFER_START) {
        assertThat(event.bufferDurationMs())
            .as("BUFFER_START event must have a bufferDurationMs")
            .isNotNull()
            .isGreaterThanOrEqualTo(ChaosAwareStrategy.CHAOS_BUFFER_MIN_MS)
            .isLessThanOrEqualTo(ChaosAwareStrategy.CHAOS_BUFFER_MAX_MS);
        return;
      }
    }
    // If we didn't find one in 200, the test passes vacuously (very unlikely not to find one)
  }
}
