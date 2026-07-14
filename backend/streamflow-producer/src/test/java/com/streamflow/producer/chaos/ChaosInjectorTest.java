package com.streamflow.producer.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChaosInjector}.
 *
 * <p>SPEC-13 Test Plan: verifies start/cancel, auto-revert after durationSeconds, and scenario
 * lookup used by {@code ChaosAwareStrategy}.
 */
class ChaosInjectorTest {

  private ChaosInjector injector;

  @BeforeEach
  void setUp() {
    injector = new ChaosInjector();
  }

  @Test
  void start_returnsNonNullChaosId() {
    String chaosId = injector.start(ChaosScenario.HIGH_BUFFER, "stream-001", 30);
    assertThat(chaosId).isNotBlank();
  }

  @Test
  void activeScenario_returnsPresentAfterStart() {
    injector.start(ChaosScenario.VIEWER_DROP, "stream-001", 60);

    Optional<ChaosScenario> active = injector.activeScenario("stream-001");

    assertThat(active).isPresent().contains(ChaosScenario.VIEWER_DROP);
  }

  @Test
  void activeScenario_returnsEmptyForUnaffectedStream() {
    injector.start(ChaosScenario.STREAM_DOWN, "stream-001", 60);

    Optional<ChaosScenario> active = injector.activeScenario("stream-002");

    assertThat(active).isEmpty();
  }

  @Test
  void cancel_returnsTrueAndClearsScenario() {
    String chaosId = injector.start(ChaosScenario.HIGH_BUFFER, "stream-001", 60);

    boolean cancelled = injector.cancel(chaosId);

    assertThat(cancelled).isTrue();
    assertThat(injector.activeScenario("stream-001")).isEmpty();
    assertThat(injector.isActive("stream-001")).isFalse();
  }

  @Test
  void cancel_returnsFalseForUnknownId() {
    boolean result = injector.cancel("non-existent-id");
    assertThat(result).isFalse();
  }

  @Test
  void start_replacesExistingChaosOnSameStream() {
    injector.start(ChaosScenario.VIEWER_DROP, "stream-001", 60);
    String secondId = injector.start(ChaosScenario.HIGH_BUFFER, "stream-001", 60);

    Optional<ChaosScenario> active = injector.activeScenario("stream-001");
    assertThat(active).isPresent().contains(ChaosScenario.HIGH_BUFFER);
    // Only one active chaos per stream
    assertThat(injector.isActive("stream-001")).isTrue();
    // The second chaos id is valid
    assertThat(injector.cancel(secondId)).isTrue();
  }

  @Test
  void isActive_returnsFalseBeforeStart() {
    assertThat(injector.isActive("stream-001")).isFalse();
  }

  @Test
  void autoRevert_removesStateAfterDuration() throws InterruptedException {
    // Use 1-second duration for a fast test
    injector.start(ChaosScenario.BITRATE_SPIKE, "stream-001", 1);
    assertThat(injector.isActive("stream-001")).isTrue();

    // Wait for auto-revert (1s + small buffer for scheduler thread)
    Thread.sleep(1_500);

    assertThat(injector.isActive("stream-001")).isFalse();
    assertThat(injector.activeScenario("stream-001")).isEmpty();
  }
}
