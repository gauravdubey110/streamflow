package com.streamflow.processor.alert;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertType;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Unit tests for {@link ViewerDropRule} backed by a real Redis (Testcontainers).
 *
 * <p>SPEC-11 Test Plan — Unit: 3+ scenarios per rule (below, at, above threshold).
 *
 * <p>Because ViewerDropRule maintains rolling state in Redis (ZADD / ZREMRANGEBYSCORE), a real
 * Redis instance is required. Mocking a Redis sorted-set would be too complex and would not provide
 * meaningful coverage.
 */
@Testcontainers
class ViewerDropRuleTest {

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

  private ViewerDropRule rule;
  private RedisTemplate<String, String> redisTemplate;

  @BeforeEach
  void setUp() {
    RedisStandaloneConfiguration redisConfig =
        new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
    LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfig);
    connectionFactory.afterPropertiesSet();

    redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(connectionFactory);
    StringRedisSerializer serializer = new StringRedisSerializer();
    redisTemplate.setKeySerializer(serializer);
    redisTemplate.setValueSerializer(serializer);
    redisTemplate.setHashKeySerializer(serializer);
    redisTemplate.setHashValueSerializer(serializer);
    redisTemplate.setDefaultSerializer(serializer);
    redisTemplate.afterPropertiesSet();

    // Flush between tests
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

    // 30s window, 10% drop threshold
    rule = new ViewerDropRule(30L, 10.0);
  }

  // ── No prior deltas → no alert ────────────────────────────────────────────

  @Test
  void noHistory_singleNeutralDelta_noAlert() {
    // With 50,000 viewers and a +100 delta, no drop alert
    StreamMetricSnapshotDTO snapshot = snapshot("stream-neutral", 50_000L, 100L, 1.0);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);

    assertThat(result).isEmpty();
  }

  // ── Small negative delta below threshold ──────────────────────────────────

  @Test
  void smallNegativeDelta_belowThreshold_noAlert() {
    // 10% of 50,000 = 5,000. A single delta of -100 is well below.
    String streamId = "stream-small-drop-" + UUID.randomUUID();
    StreamMetricSnapshotDTO snapshot = snapshot(streamId, 50_000L, -100L, 1.0);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);

    assertThat(result).isEmpty();
  }

  // ── Large negative delta exceeding threshold ──────────────────────────────

  @Test
  void largeSingleNegativeDelta_exceedsThreshold_returnsWarning() {
    // 10% of 50,000 = 5,000. A single delta of -6,000 exceeds 10%.
    String streamId = "stream-large-drop-" + UUID.randomUUID();
    StreamMetricSnapshotDTO snapshot = snapshot(streamId, 50_000L, -6_000L, 1.0);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);

    assertThat(result).isPresent();
    AlertEventDTO alert = result.get();
    assertThat(alert.alertType()).isEqualTo(AlertType.VIEWER_DROP);
    assertThat(alert.streamId()).isEqualTo(streamId);
    assertThat(alert.actualValue()).isGreaterThan(10.0); // actual drop% > 10%
    assertThat(alert.threshold()).isEqualTo(10.0);
  }

  // ── Accumulating negative deltas within window ────────────────────────────

  @Test
  void accumulatedNegativeDeltas_exceedThreshold_returnsWarning() {
    // 50,000 viewers, threshold = 5,000. Push deltas summing to -5,500.
    String streamId = "stream-accum-" + UUID.randomUUID();

    // First call: -1,000
    rule.evaluate(snapshot(streamId, 50_000L, -1_000L, 0.5), redisTemplate);
    // Second call: -1,500
    rule.evaluate(snapshot(streamId, 50_000L, -1_500L, 0.5), redisTemplate);
    // Third call: -3,000 → total = -5,500 → exceeds 10%
    Optional<AlertEventDTO> result =
        rule.evaluate(snapshot(streamId, 50_000L, -3_000L, 0.5), redisTemplate);

    assertThat(result).isPresent();
    assertThat(result.get().alertType()).isEqualTo(AlertType.VIEWER_DROP);
  }

  // ── Zero liveViewerCount → no alert (avoid division by zero) ─────────────

  @Test
  void zeroLiveCount_noAlert() {
    String streamId = "stream-zero-" + UUID.randomUUID();
    StreamMetricSnapshotDTO snapshot = snapshot(streamId, 0L, -1_000L, 5.0);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);

    assertThat(result).isEmpty();
  }

  // ── Positive delta → no alert ─────────────────────────────────────────────

  @Test
  void positiveDelta_noAlert() {
    String streamId = "stream-pos-" + UUID.randomUUID();
    StreamMetricSnapshotDTO snapshot = snapshot(streamId, 50_000L, 3_000L, 1.0);

    Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);

    assertThat(result).isEmpty();
  }

  // ── Dedup key is set in Redis after a positive evaluation ─────────────────

  @Test
  void afterFiring_dedupKeyExists() {
    String streamId = "stream-dedup-" + UUID.randomUUID();
    // Delta of -10,000 on 50,000 viewers = 20% drop → should fire
    rule.evaluate(snapshot(streamId, 50_000L, -10_000L, 2.0), redisTemplate);

    // The ZADD key should exist after the call
    String deltaKey = ViewerDropRule.DELTA_KEY_PREFIX + streamId;
    assertThat(redisTemplate.hasKey(deltaKey)).isTrue();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private StreamMetricSnapshotDTO snapshot(
      String streamId, long liveCount, long delta, double bufferRate) {
    return new StreamMetricSnapshotDTO(
        streamId,
        "Test Stream",
        liveCount,
        delta,
        bufferRate,
        42,
        Map.of("1080p", 100.0),
        90.0,
        "CLOSED",
        0,
        System.currentTimeMillis());
  }
}
