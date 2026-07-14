package com.streamflow.processor.alert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import com.streamflow.processor.consumer.StreamHealthConsumer;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Unit tests for {@link BitrateDegradationRule}.
 *
 * <p>SPEC-11 Test Plan — Unit: 3+ scenarios per rule (below, at, above threshold). Uses a mocked
 * {@link RedisTemplate} to inject the health hash value.
 */
@SuppressWarnings("unchecked")
class BitrateDegradationRuleTest {

  private static final int MIN_BITRATE = 2500;

  private BitrateDegradationRule rule;
  private RedisTemplate<String, String> redis;
  private HashOperations<String, Object, Object> hashOps;

  @BeforeEach
  void setUp() {
    redis = mock(RedisTemplate.class);
    hashOps = mock(HashOperations.class);
    when(redis.opsForHash()).thenReturn(hashOps);

    rule = new BitrateDegradationRule(MIN_BITRATE);
  }

  // ── Health hash absent → no alert ─────────────────────────────────────────

  @ParameterizedTest(name = "healthHash=null → no alert")
  @ValueSource(strings = {"stream-A"})
  void healthHashAbsent_returnsEmpty(String streamId) {
    String key = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;
    when(hashOps.entries(key)).thenReturn(Map.of());

    Optional<AlertEventDTO> result = rule.evaluate(snapshot(streamId), redis);

    assertThat(result).isEmpty();
  }

  // ── Bitrate at or above threshold → no alert ──────────────────────────────

  @ParameterizedTest(name = "bitrateKbps={0} → no alert")
  @ValueSource(ints = {2500, 3000, 5000, 8000})
  void bitrateAtOrAboveThreshold_returnsEmpty(int bitrateKbps) {
    String streamId = "stream-ok";
    stubBitrate(streamId, bitrateKbps);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot(streamId), redis);

    assertThat(result).isEmpty();
  }

  // ── Bitrate below threshold → WARNING ─────────────────────────────────────

  @ParameterizedTest(name = "bitrateKbps={0} → WARNING")
  @ValueSource(ints = {2499, 2000, 1000, 0})
  void bitrateBelowThreshold_returnsWarning(int bitrateKbps) {
    String streamId = "stream-low";
    stubBitrate(streamId, bitrateKbps);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot(streamId), redis);

    assertThat(result).isPresent();
    AlertEventDTO alert = result.get();
    assertThat(alert.alertType()).isEqualTo(AlertType.BITRATE_DEGRADATION);
    assertThat(alert.severity()).isEqualTo(AlertSeverity.WARNING);
    assertThat(alert.threshold()).isEqualTo((double) MIN_BITRATE);
    assertThat(alert.actualValue()).isEqualTo((double) bitrateKbps);
    assertThat(alert.streamId()).isEqualTo(streamId);
    assertThat(alert.alertId()).isNotBlank();
    assertThat(alert.message()).contains(streamId);
  }

  // ── BitrateKbps field missing from hash ───────────────────────────────────

  @ParameterizedTest(name = "bitrateField=null → no alert")
  @ValueSource(strings = {"stream-missing-field"})
  void bitrateFieldMissing_returnsEmpty(String streamId) {
    String key = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;
    when(hashOps.entries(key)).thenReturn(Map.of("frameDropRate", "0.02"));

    Optional<AlertEventDTO> result = rule.evaluate(snapshot(streamId), redis);

    assertThat(result).isEmpty();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private void stubBitrate(String streamId, int bitrateKbps) {
    String key = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;
    when(hashOps.entries(key))
        .thenReturn(
            Map.of(
                StreamHealthConsumer.FIELD_BITRATE, String.valueOf(bitrateKbps),
                StreamHealthConsumer.FIELD_FRAME_DROP, "0.01",
                StreamHealthConsumer.FIELD_LATENCY, "100"));
  }

  private StreamMetricSnapshotDTO snapshot(String streamId) {
    return new StreamMetricSnapshotDTO(
        streamId,
        "Test Stream",
        50_000L,
        100L,
        2.5,
        120,
        Map.of("1080p", 100.0),
        92.0,
        "CLOSED",
        0,
        System.currentTimeMillis());
  }
}
