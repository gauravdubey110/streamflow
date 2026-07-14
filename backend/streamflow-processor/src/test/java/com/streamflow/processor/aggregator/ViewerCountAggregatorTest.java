package com.streamflow.processor.aggregator;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Unit tests for {@link ViewerCountAggregator} backed by a real Redis instance in Testcontainers.
 *
 * <p>SPEC-04 Test Plan — Unit: aggregator with embedded Redis.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>AC1 — 1000 JOINs → ZCARD == 1000
 *   <li>AC2 — 1000 JOINs + 200 DROPs → ZCARD == 800
 *   <li>AC3 — same JOIN 5 × → ZCARD == 1 (idempotency)
 *   <li>Eviction — stale entries removed by {@link ViewerCountAggregator#evictStaleEntries}
 * </ul>
 */
@Testcontainers
class ViewerCountAggregatorTest {

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

  private ViewerCountAggregator aggregator;

  @BeforeEach
  void setUp() {
    // Build a fresh RedisTemplate pointing at the Testcontainers Redis instance
    RedisStandaloneConfiguration redisConfig =
        new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
    LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfig);
    connectionFactory.afterPropertiesSet();

    RedisTemplate<String, String> template = new RedisTemplate<>();
    template.setConnectionFactory(connectionFactory);
    StringRedisSerializer serializer = new StringRedisSerializer();
    template.setKeySerializer(serializer);
    template.setValueSerializer(serializer);
    template.setHashKeySerializer(serializer);
    template.setHashValueSerializer(serializer);
    template.setDefaultSerializer(serializer);
    template.afterPropertiesSet();

    aggregator = new ViewerCountAggregator(template);

    // Flush Redis between tests to ensure isolation
    template.getConnectionFactory().getConnection().serverCommands().flushAll();
  }

  // ── AC1: 1000 JOINs ─────────────────────────────────────────────────────

  @Test
  void ac1_thousandJoinsYieldZCardOfThousand() {
    String streamId = "stream-001";
    long ts = System.currentTimeMillis();

    for (int i = 0; i < 1000; i++) {
      aggregator.recordJoin(streamId, "viewer-" + i, ts + i);
    }

    assertThat(aggregator.getLiveCount(streamId))
        .as("ZCARD should equal 1000 after 1000 distinct JOINs")
        .isEqualTo(1000L);
  }

  // ── AC2: 1000 JOINs + 200 DROPs ─────────────────────────────────────────

  @Test
  void ac2_thousandJoinsMinus200DropsYieldEightHundred() {
    String streamId = "stream-001";
    long ts = System.currentTimeMillis();

    for (int i = 0; i < 1000; i++) {
      aggregator.recordJoin(streamId, "viewer-" + i, ts + i);
    }
    for (int i = 0; i < 200; i++) {
      aggregator.recordDrop(streamId, "viewer-" + i);
    }

    assertThat(aggregator.getLiveCount(streamId))
        .as("ZCARD should equal 800 after 200 DROPs from 1000 JOINs")
        .isEqualTo(800L);
  }

  // ── AC3: Idempotency ─────────────────────────────────────────────────────

  @Test
  void ac3_sameJoinFiveTimesYieldsCountOfOne() {
    String streamId = "stream-001";
    String viewerId = "viewer-idempotent";
    long ts = System.currentTimeMillis();

    for (int i = 0; i < 5; i++) {
      aggregator.recordJoin(streamId, viewerId, ts + i);
    }

    assertThat(aggregator.getLiveCount(streamId))
        .as("ZCARD should be 1 for the same viewerId added 5 times")
        .isEqualTo(1L);
  }

  // ── Eviction ─────────────────────────────────────────────────────────────

  @Test
  void evictionRemovesEntriesOlderThanFiveMinutes() {
    String streamId = "stream-evict";
    long fiveMinutesAgoMs = System.currentTimeMillis() - (5L * 60L * 1000L) - 1000L;
    long recentMs = System.currentTimeMillis();

    // Add 5 stale entries (older than 5 min) and 3 fresh entries
    for (int i = 0; i < 5; i++) {
      aggregator.recordJoin(streamId, "stale-" + i, fiveMinutesAgoMs - i);
    }
    for (int i = 0; i < 3; i++) {
      aggregator.recordJoin(streamId, "fresh-" + i, recentMs + i);
    }

    assertThat(aggregator.getLiveCount(streamId)).isEqualTo(8L);

    aggregator.evictStaleEntries(streamId);

    assertThat(aggregator.getLiveCount(streamId))
        .as("Only 3 fresh entries should remain after eviction")
        .isEqualTo(3L);
  }

  // ── getLiveCount: empty key ───────────────────────────────────────────────

  @Test
  void getLiveCount_returnsZeroForUnknownStream() {
    assertThat(aggregator.getLiveCount("non-existent-stream"))
        .as("getLiveCount should return 0 when key does not exist")
        .isZero();
  }

  // ── knownStreamIds tracking ───────────────────────────────────────────────

  @Test
  void knownStreamIds_tracksStreamsOnJoinAndDrop() {
    aggregator.recordJoin("stream-a", "v1", System.currentTimeMillis());
    aggregator.recordDrop("stream-b", "v2");

    assertThat(aggregator.getKnownStreamIds()).containsExactlyInAnyOrder("stream-a", "stream-b");
  }
}
