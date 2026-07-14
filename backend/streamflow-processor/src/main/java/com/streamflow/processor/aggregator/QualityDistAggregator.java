package com.streamflow.processor.aggregator;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Aggregates per-stream video quality distribution and buffer rate using Redis Hashes.
 *
 * <p>SPEC-10 R2 — writes:
 *
 * <ul>
 *   <li>{@code HINCRBY quality_dist:{streamId}:{minuteBucket} <quality> 1} on JOIN and
 *       QUALITY_SWITCH events.
 *   <li>{@code HINCRBY buffer_count:{streamId}:{minuteBucket} TOTAL 1} on every event.
 *   <li>{@code HINCRBY buffer_count:{streamId}:{minuteBucket} BUFFER 1} on BUFFER_START events.
 *   <li>TTL 120 s on both keys after every write.
 * </ul>
 *
 * <p>SPEC-10 R3 — reads (current + previous minute buckets merged):
 *
 * <ul>
 *   <li>{@code getDistributionPct(streamId)} — returns quality tier percentages summing to 100
 *       ±0.1; returns all-zero map when no data exists.
 *   <li>{@code getBufferRatePct(streamId)} — returns {@code 100 * BUFFER / TOTAL}; returns 0 when
 *       TOTAL = 0.
 * </ul>
 *
 * <p>NFR1: reads use two HGETALL commands (one per bucket) via the same Redis connection — p99 &lt;
 * 5 ms for the arithmetic operation.
 */
@Slf4j
@Component
public class QualityDistAggregator {

  /** Redis key prefix for quality-distribution hashes (SPEC-10 R2). */
  static final String QUALITY_KEY_PREFIX = "quality_dist:";

  /** Redis key prefix for buffer-count hashes (SPEC-10 R2). */
  static final String BUFFER_KEY_PREFIX = "buffer_count:";

  /** Hash field name for total event count (SPEC-10 R2). */
  static final String FIELD_TOTAL = "TOTAL";

  /** Hash field name for buffer-start event count (SPEC-10 R2). */
  static final String FIELD_BUFFER = "BUFFER";

  /** TTL applied to both hash keys after each write (SPEC-10 R2: 120 s). */
  static final Duration KEY_TTL = Duration.ofSeconds(120);

  /** ISO-free minute-bucket format: {@code yyyyMMddHHmm}. */
  private static final DateTimeFormatter BUCKET_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);

  private final RedisTemplate<String, String> redisTemplate;

  public QualityDistAggregator(RedisTemplate<String, String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  // ── Write path ─────────────────────────────────────────────────────────────

  /**
   * Records a viewer event in the quality-distribution and buffer-count Redis hashes.
   *
   * <p>Routing rules (SPEC-10 R2):
   *
   * <ul>
   *   <li>JOIN, QUALITY_SWITCH → increment the quality tier counter.
   *   <li>BUFFER_START → increment the BUFFER counter.
   *   <li>All events (JOIN, DROP, QUALITY_SWITCH, BUFFER_START, BUFFER_END, ERROR) → increment the
   *       TOTAL counter.
   * </ul>
   *
   * @param event the viewer event to record
   */
  public void recordEvent(ViewerEventDTO event) {
    String streamId = event.streamId();
    String bucket = currentBucket();

    EventType type = event.eventType();

    // Increment quality tier on JOIN and QUALITY_SWITCH
    if (type == EventType.JOIN || type == EventType.QUALITY_SWITCH) {
      String qualityKey = qualityKey(streamId, bucket);
      String qualityField =
          event.quality() != null
              ? event.quality().getWireValue()
              : VideoQuality.Q_1080P.getWireValue();
      redisTemplate.opsForHash().increment(qualityKey, qualityField, 1L);
      redisTemplate.expire(qualityKey, KEY_TTL);
      log.trace(
          "quality_dist incremented: stream={} bucket={} quality={}",
          streamId,
          bucket,
          qualityField);
    }

    // Increment buffer counter on BUFFER_START
    String bufferKey = bufferKey(streamId, bucket);
    if (type == EventType.BUFFER_START) {
      redisTemplate.opsForHash().increment(bufferKey, FIELD_BUFFER, 1L);
    }
    // Increment total counter on every event
    redisTemplate.opsForHash().increment(bufferKey, FIELD_TOTAL, 1L);
    redisTemplate.expire(bufferKey, KEY_TTL);
  }

  // ── Read path ──────────────────────────────────────────────────────────────

  /**
   * Returns the quality distribution as percentages for the current stream.
   *
   * <p>Merges the current and previous minute buckets to smooth minute-boundary effects (SPEC-10
   * R3). The returned map always contains all five quality tiers (even if 0%), and the values sum
   * to 100 ±0.1 when there is data; returns all-zero map when there is no data in either bucket.
   *
   * @param streamId the stream to query
   * @return map from quality-tier wire value (e.g. {@code "1080p"}) to percentage (0–100)
   */
  public Map<String, Double> getDistributionPct(String streamId) {
    Map<String, Long> merged = mergeQualityCounts(streamId);

    long total = merged.values().stream().mapToLong(Long::longValue).sum();
    Map<String, Double> pct = new HashMap<>();

    for (VideoQuality q : VideoQuality.values()) {
      long count = merged.getOrDefault(q.getWireValue(), 0L);
      pct.put(q.getWireValue(), total == 0 ? 0.0 : round1dp(100.0 * count / total));
    }

    // Adjust for floating-point rounding so total is exactly 100.0 when data exists
    if (total > 0) {
      normalizeToHundred(pct);
    }

    log.trace("getDistributionPct: stream={} total={} pct={}", streamId, total, pct);
    return pct;
  }

  /**
   * Returns the buffer rate as a percentage for the current stream.
   *
   * <p>Merges the current and previous minute buckets (SPEC-10 R3). Returns 0.0 when TOTAL = 0 (no
   * events in either bucket).
   *
   * @param streamId the stream to query
   * @return {@code 100 * BUFFER / TOTAL}, in range [0, 100]; 0 if no data
   */
  public double getBufferRatePct(String streamId) {
    long[] counts = mergeBufferCounts(streamId);
    long bufferCount = counts[0];
    long totalCount = counts[1];

    if (totalCount == 0) {
      return 0.0;
    }
    double rate = round1dp(100.0 * bufferCount / totalCount);
    log.trace(
        "getBufferRatePct: stream={} buffer={} total={} rate={}",
        streamId,
        bufferCount,
        totalCount,
        rate);
    return rate;
  }

  // ── Bucket helpers ─────────────────────────────────────────────────────────

  /**
   * Returns the current minute bucket string, e.g. {@code "202406151430"}. Format is {@code
   * yyyyMMddHHmm} in UTC (SPEC-10 §4).
   */
  String currentBucket() {
    return BUCKET_FORMATTER.format(Instant.now().truncatedTo(ChronoUnit.MINUTES));
  }

  /**
   * Returns the previous minute bucket string (current − 1 minute). Used by the read path to smooth
   * minute-boundary effects (SPEC-10 R3).
   */
  String previousBucket() {
    return BUCKET_FORMATTER.format(
        Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(1, ChronoUnit.MINUTES));
  }

  // ── Internal helpers ───────────────────────────────────────────────────────

  /**
   * Merges quality counts from the current and previous minute bucket hashes.
   *
   * @param streamId the stream to merge
   * @return combined counts per quality tier
   */
  private Map<String, Long> mergeQualityCounts(String streamId) {
    Map<String, Long> merged = new HashMap<>();

    for (String bucket : new String[] {currentBucket(), previousBucket()}) {
      Map<Object, Object> raw = redisTemplate.opsForHash().entries(qualityKey(streamId, bucket));
      for (Map.Entry<Object, Object> entry : raw.entrySet()) {
        String field = entry.getKey().toString();
        long count = parseLong(entry.getValue());
        merged.merge(field, count, Long::sum);
      }
    }
    return merged;
  }

  /**
   * Merges buffer counts from the current and previous minute bucket hashes.
   *
   * @param streamId the stream to merge
   * @return {@code long[]{bufferCount, totalCount}}
   */
  private long[] mergeBufferCounts(String streamId) {
    long bufferTotal = 0;
    long eventTotal = 0;

    for (String bucket : new String[] {currentBucket(), previousBucket()}) {
      Map<Object, Object> raw = redisTemplate.opsForHash().entries(bufferKey(streamId, bucket));
      bufferTotal += parseLong(raw.get(FIELD_BUFFER));
      eventTotal += parseLong(raw.get(FIELD_TOTAL));
    }
    return new long[] {bufferTotal, eventTotal};
  }

  /**
   * Adjusts the largest quality-tier percentage so that all values sum exactly to 100.0 (corrects
   * floating-point rounding error).
   */
  private void normalizeToHundred(Map<String, Double> pct) {
    double sum = pct.values().stream().mapToDouble(Double::doubleValue).sum();
    double diff = round1dp(100.0 - sum);
    if (diff == 0.0) {
      return;
    }
    // Add the remainder to the largest bucket
    String largestKey =
        pct.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(VideoQuality.Q_1080P.getWireValue());
    pct.put(largestKey, round1dp(pct.get(largestKey) + diff));
  }

  private static double round1dp(double v) {
    return Math.round(v * 10.0) / 10.0;
  }

  private static long parseLong(Object val) {
    if (val == null) return 0L;
    try {
      return Long.parseLong(val.toString());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  static String qualityKey(String streamId, String bucket) {
    return QUALITY_KEY_PREFIX + streamId + ":" + bucket;
  }

  static String bufferKey(String streamId, String bucket) {
    return BUFFER_KEY_PREFIX + streamId + ":" + bucket;
  }

  /**
   * Returns all quality-tier wire values as an ordered array. Used in tests to verify complete
   * coverage.
   */
  public static String[] qualityWireValues() {
    return Arrays.stream(VideoQuality.values())
        .map(VideoQuality::getWireValue)
        .toArray(String[]::new);
  }
}
