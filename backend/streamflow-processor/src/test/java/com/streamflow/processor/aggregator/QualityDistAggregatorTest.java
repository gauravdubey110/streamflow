package com.streamflow.processor.aggregator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
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
 * Unit tests for {@link QualityDistAggregator} backed by a real Redis instance in Testcontainers.
 *
 * <p>SPEC-10 Test Plan — Unit: feed a fake Redis with known counts; assert percentages.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>AC1-proxy — percentages sum to 100 ±0.5 and ordering is preserved
 *   <li>AC2-proxy — bufferRatePct ≈ 1.0 ±0.5 under baseline (5% BUFFER_START out of 100%)
 *   <li>Two-bucket merge — counts from previous minute are included in totals
 *   <li>Empty bucket → getDistributionPct returns all-zeros; getBufferRatePct returns 0
 *   <li>JOIN and QUALITY_SWITCH increment quality tier counter
 *   <li>BUFFER_START increments both BUFFER and TOTAL; other events only TOTAL
 * </ul>
 */
@Testcontainers
class QualityDistAggregatorTest {

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

  private QualityDistAggregator aggregator;
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

    aggregator = new QualityDistAggregator(redisTemplate);

    // Flush Redis between tests for isolation
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
  }

  // ── Percentage sum ────────────────────────────────────────────────────────

  /**
   * SPEC-10 AC1 proxy: after populating known quality counts, the returned distribution percentages
   * must sum to 100 ±0.5.
   */
  @Test
  void getDistributionPct_percentagesSumToHundred() {
    String streamId = "stream-dist-" + UUID.randomUUID();
    String bucket = aggregator.currentBucket();

    // Write counts directly: 45 × 1080p, 25 × 720p, 20 × 480p, 7 × 360p, 3 × 144p = 100 total
    String qualityKey = QualityDistAggregator.qualityKey(streamId, bucket);
    redisTemplate.opsForHash().put(qualityKey, "1080p", "45");
    redisTemplate.opsForHash().put(qualityKey, "720p", "25");
    redisTemplate.opsForHash().put(qualityKey, "480p", "20");
    redisTemplate.opsForHash().put(qualityKey, "360p", "7");
    redisTemplate.opsForHash().put(qualityKey, "144p", "3");

    Map<String, Double> pct = aggregator.getDistributionPct(streamId);

    assertThat(pct).containsKeys("1080p", "720p", "480p", "360p", "144p");

    double sum = pct.values().stream().mapToDouble(Double::doubleValue).sum();
    assertThat(sum)
        .as("Quality distribution percentages should sum to 100 ±0.5")
        .isCloseTo(100.0, within(0.5));
  }

  /** SPEC-10 AC1 proxy: ordering — 1080p % > 720p % > 480p % > 360p % > 144p %. */
  @Test
  void getDistributionPct_correctOrdering() {
    String streamId = "stream-order-" + UUID.randomUUID();
    String bucket = aggregator.currentBucket();

    String qualityKey = QualityDistAggregator.qualityKey(streamId, bucket);
    redisTemplate.opsForHash().put(qualityKey, "1080p", "45");
    redisTemplate.opsForHash().put(qualityKey, "720p", "25");
    redisTemplate.opsForHash().put(qualityKey, "480p", "20");
    redisTemplate.opsForHash().put(qualityKey, "360p", "7");
    redisTemplate.opsForHash().put(qualityKey, "144p", "3");

    Map<String, Double> pct = aggregator.getDistributionPct(streamId);

    assertThat(pct.get("1080p")).isGreaterThan(pct.get("720p"));
    assertThat(pct.get("720p")).isGreaterThan(pct.get("480p"));
    assertThat(pct.get("480p")).isGreaterThan(pct.get("360p"));
    assertThat(pct.get("360p")).isGreaterThan(pct.get("144p"));
  }

  // ── Buffer rate ───────────────────────────────────────────────────────────

  /** SPEC-10 AC2 proxy: 5 BUFFER events out of 100 TOTAL events → bufferRatePct = 5.0. */
  @Test
  void getBufferRatePct_correctPercentage() {
    String streamId = "stream-buf-" + UUID.randomUUID();
    String bucket = aggregator.currentBucket();

    String bufferKey = QualityDistAggregator.bufferKey(streamId, bucket);
    redisTemplate.opsForHash().put(bufferKey, "BUFFER", "5");
    redisTemplate.opsForHash().put(bufferKey, "TOTAL", "100");

    double rate = aggregator.getBufferRatePct(streamId);

    assertThat(rate).as("bufferRatePct should be 5.0 (5/100 * 100)").isCloseTo(5.0, within(0.1));
  }

  /** SPEC-10 R3: returns 0 when TOTAL = 0 (no events in either bucket). */
  @Test
  void getBufferRatePct_returnsZeroWhenNoEvents() {
    String streamId = "stream-empty-buf-" + UUID.randomUUID();

    double rate = aggregator.getBufferRatePct(streamId);

    assertThat(rate).as("bufferRatePct should be 0.0 when no events have been recorded").isZero();
  }

  // ── Empty bucket ──────────────────────────────────────────────────────────

  /**
   * SPEC-10 R3: when no events exist for a stream, getDistributionPct returns a map with all five
   * quality tiers set to 0.0.
   */
  @Test
  void getDistributionPct_returnsAllZerosWhenNoData() {
    String streamId = "stream-no-data-" + UUID.randomUUID();

    Map<String, Double> pct = aggregator.getDistributionPct(streamId);

    assertThat(pct).containsKeys("1080p", "720p", "480p", "360p", "144p");
    pct.values().forEach(v -> assertThat(v).as("All tiers should be 0.0 when no data").isZero());
  }

  // ── Two-bucket merge ──────────────────────────────────────────────────────

  /**
   * SPEC-10 R3: counts in the previous minute bucket are merged with the current bucket. After
   * writing to the previous bucket directly, getDistributionPct reflects both.
   */
  @Test
  void getDistributionPct_mergesPreviousMinuteBucket() {
    String streamId = "stream-merge-" + UUID.randomUUID();
    String currentBucket = aggregator.currentBucket();
    String previousBucket = aggregator.previousBucket();

    // 30 events in current bucket
    String currentQualityKey = QualityDistAggregator.qualityKey(streamId, currentBucket);
    redisTemplate.opsForHash().put(currentQualityKey, "1080p", "30");

    // 20 events in previous bucket
    String prevQualityKey = QualityDistAggregator.qualityKey(streamId, previousBucket);
    redisTemplate.opsForHash().put(prevQualityKey, "1080p", "20");

    Map<String, Double> pct = aggregator.getDistributionPct(streamId);

    // Total 1080p = 50, total all = 50 → 100%
    assertThat(pct.get("1080p"))
        .as("Merged 1080p count (30+20=50 out of 50 total) should be 100.0%%")
        .isCloseTo(100.0, within(0.5));
  }

  /** Buffer count: merges previous and current minute buckets correctly. */
  @Test
  void getBufferRatePct_mergesPreviousMinuteBucket() {
    String streamId = "stream-buf-merge-" + UUID.randomUUID();
    String currentBucket = aggregator.currentBucket();
    String previousBucket = aggregator.previousBucket();

    // Current: 3 buffer out of 60 total
    String currentBufKey = QualityDistAggregator.bufferKey(streamId, currentBucket);
    redisTemplate.opsForHash().put(currentBufKey, "BUFFER", "3");
    redisTemplate.opsForHash().put(currentBufKey, "TOTAL", "60");

    // Previous: 2 buffer out of 40 total
    String prevBufKey = QualityDistAggregator.bufferKey(streamId, previousBucket);
    redisTemplate.opsForHash().put(prevBufKey, "BUFFER", "2");
    redisTemplate.opsForHash().put(prevBufKey, "TOTAL", "40");

    // Merged: 5 buffer / 100 total = 5%
    double rate = aggregator.getBufferRatePct(streamId);
    assertThat(rate)
        .as("Merged bufferRatePct (3+2=5 / 60+40=100) should be 5.0%%")
        .isCloseTo(5.0, within(0.1));
  }

  // ── recordEvent routing ───────────────────────────────────────────────────

  /** JOIN events should increment the quality tier counter. */
  @Test
  void recordEvent_joinIncrementsQualityTier() {
    String streamId = "stream-join-" + UUID.randomUUID();
    ViewerEventDTO event = joinEvent(streamId, VideoQuality.Q_1080P);

    aggregator.recordEvent(event);

    String key = QualityDistAggregator.qualityKey(streamId, aggregator.currentBucket());
    Object raw = redisTemplate.opsForHash().get(key, "1080p");
    assertThat(raw).isNotNull();
    assertThat(Long.parseLong(raw.toString()))
        .as("JOIN should increment 1080p quality counter by 1")
        .isEqualTo(1L);
  }

  /** QUALITY_SWITCH events should increment the quality tier counter. */
  @Test
  void recordEvent_qualitySwitchIncrementsQualityTier() {
    String streamId = "stream-qswitch-" + UUID.randomUUID();
    ViewerEventDTO event = eventOf(streamId, EventType.QUALITY_SWITCH, VideoQuality.Q_720P, null);

    aggregator.recordEvent(event);

    String key = QualityDistAggregator.qualityKey(streamId, aggregator.currentBucket());
    Object raw = redisTemplate.opsForHash().get(key, "720p");
    assertThat(raw).isNotNull();
    assertThat(Long.parseLong(raw.toString()))
        .as("QUALITY_SWITCH should increment 720p quality counter by 1")
        .isEqualTo(1L);
  }

  /** BUFFER_START should increment BUFFER and TOTAL counters; quality tier should NOT change. */
  @Test
  void recordEvent_bufferStartIncrementsBufferAndTotal_notQuality() {
    String streamId = "stream-bufstart-" + UUID.randomUUID();
    ViewerEventDTO event = eventOf(streamId, EventType.BUFFER_START, VideoQuality.Q_1080P, 1200L);

    aggregator.recordEvent(event);

    String bufferKey = QualityDistAggregator.bufferKey(streamId, aggregator.currentBucket());
    assertThat(redisTemplate.opsForHash().get(bufferKey, "BUFFER"))
        .as("BUFFER counter should be 1 after one BUFFER_START")
        .isEqualTo("1");
    assertThat(redisTemplate.opsForHash().get(bufferKey, "TOTAL"))
        .as("TOTAL counter should be 1 after one BUFFER_START")
        .isEqualTo("1");

    // Quality tier should NOT be incremented for BUFFER_START
    String qualityKey = QualityDistAggregator.qualityKey(streamId, aggregator.currentBucket());
    assertThat(redisTemplate.opsForHash().get(qualityKey, "1080p"))
        .as("BUFFER_START should NOT increment quality tier counter")
        .isNull();
  }

  /** DROP events should only increment TOTAL, not quality tier or BUFFER. */
  @Test
  void recordEvent_dropIncrementsOnlyTotal() {
    String streamId = "stream-drop-" + UUID.randomUUID();
    ViewerEventDTO event = eventOf(streamId, EventType.DROP, VideoQuality.Q_480P, null);

    aggregator.recordEvent(event);

    String bufferKey = QualityDistAggregator.bufferKey(streamId, aggregator.currentBucket());
    assertThat(redisTemplate.opsForHash().get(bufferKey, "TOTAL"))
        .as("TOTAL counter should be 1 after one DROP")
        .isEqualTo("1");
    assertThat(redisTemplate.opsForHash().get(bufferKey, "BUFFER"))
        .as("BUFFER counter should remain null after DROP")
        .isNull();
  }

  /**
   * Multiple BUFFER_START events drive the buffer rate above 5% threshold (AC3 proxy). Here: 200
   * BUFFER_START + 800 JOIN events = 200/1000 = 20% buffer rate.
   */
  @Test
  void recordEvent_highBufferStartCount_raisesBufferRatePct() {
    String streamId = "stream-highbuf-" + UUID.randomUUID();

    // Record 800 JOIN events
    for (int i = 0; i < 800; i++) {
      aggregator.recordEvent(joinEvent(streamId, VideoQuality.Q_1080P));
    }
    // Record 200 BUFFER_START events
    for (int i = 0; i < 200; i++) {
      aggregator.recordEvent(
          eventOf(streamId, EventType.BUFFER_START, VideoQuality.Q_1080P, 1500L));
    }

    double rate = aggregator.getBufferRatePct(streamId);

    assertThat(rate)
        .as("bufferRatePct should be ~20%% (200 BUFFER / 1000 TOTAL)")
        .isCloseTo(20.0, within(0.5));
    assertThat(rate)
        .as("bufferRatePct should exceed 5%% threshold (SPEC-10 AC3 proxy)")
        .isGreaterThan(5.0);
  }

  // ── TTL applied ───────────────────────────────────────────────────────────

  /** After recordEvent, both quality_dist and buffer_count keys must have a TTL set. */
  @Test
  void recordEvent_ttlAppliedToBothKeys() {
    String streamId = "stream-ttl-" + UUID.randomUUID();
    aggregator.recordEvent(joinEvent(streamId, VideoQuality.Q_720P));

    String bucket = aggregator.currentBucket();
    Long qualityTtl = redisTemplate.getExpire(QualityDistAggregator.qualityKey(streamId, bucket));
    Long bufferTtl = redisTemplate.getExpire(QualityDistAggregator.bufferKey(streamId, bucket));

    assertThat(qualityTtl).as("quality_dist key should have a TTL > 0").isGreaterThan(0L);
    assertThat(bufferTtl).as("buffer_count key should have a TTL > 0").isGreaterThan(0L);
  }

  // ── Bucket helpers ────────────────────────────────────────────────────────

  @Test
  void currentBucket_formatIsYyyyMMddHHmm() {
    String bucket = aggregator.currentBucket();
    // Must be exactly 12 digits: yyyyMMddHHmm
    assertThat(bucket).as("currentBucket should be 12 digits (yyyyMMddHHmm)").matches("\\d{12}");
  }

  @Test
  void previousBucket_isOneMinuteBehindCurrent() {
    // Parse both and verify the gap is exactly 60 seconds
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);
    Instant current = fmt.parse(aggregator.currentBucket(), Instant::from);
    Instant previous = fmt.parse(aggregator.previousBucket(), Instant::from);

    assertThat(current.minus(1, ChronoUnit.MINUTES))
        .as("previousBucket should be exactly 1 minute behind currentBucket")
        .isEqualTo(previous);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private ViewerEventDTO joinEvent(String streamId, VideoQuality quality) {
    return eventOf(streamId, EventType.JOIN, quality, null);
  }

  private ViewerEventDTO eventOf(
      String streamId, EventType type, VideoQuality quality, Long bufferDurationMs) {
    return new ViewerEventDTO(
        UUID.randomUUID().toString(),
        streamId,
        UUID.randomUUID().toString(),
        type,
        quality,
        bufferDurationMs,
        System.currentTimeMillis(),
        "IN-MH");
  }
}
