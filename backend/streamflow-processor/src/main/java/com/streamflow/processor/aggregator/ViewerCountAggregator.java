package com.streamflow.processor.aggregator;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Maintains per-stream live viewer counts in a Redis Sorted Set.
 *
 * <p>SPEC-04 design:
 *
 * <ul>
 *   <li>Key schema: {@code viewer_count:{streamId}} (single sliding window per stream)
 *   <li>Score = event timestamp in ms (enables ZREMRANGEBYSCORE eviction)
 *   <li>Member = viewerId (string UUID — idempotent ZADD is naturally idempotent)
 *   <li>TTL = 600 s (10 min) — set on first ZADD; refreshed on every write
 * </ul>
 *
 * <p>Thread-safety: Redis operations are inherently atomic per command. The {@code knownStreamIds}
 * set is a {@link ConcurrentHashMap} key-set.
 */
@Slf4j
@Component
public class ViewerCountAggregator {

  /** Redis key prefix for viewer-count sorted sets. */
  static final String KEY_PREFIX = "viewer_count:";

  /** TTL of the sorted-set key (SPEC-04 §4: 10 min to cover idle streams). */
  private static final Duration KEY_TTL = Duration.ofSeconds(600);

  /** Sliding-window duration: 5 minutes in milliseconds (SPEC-04 R3). */
  private static final long WINDOW_MS = 5L * 60L * 1_000L;

  private final RedisTemplate<String, String> redisTemplate;

  /**
   * Tracks all stream IDs seen so far — used by the eviction scheduler ({@link
   * com.streamflow.processor.scheduler.StaleViewerEvictionTask}) to know which keys to clean up.
   */
  private final Set<String> knownStreamIds = ConcurrentHashMap.newKeySet();

  public ViewerCountAggregator(RedisTemplate<String, String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  /**
   * Records a viewer JOIN event.
   *
   * <p>SPEC-04 R2: {@code ZADD viewer_count:{streamId} <timestampMs> <viewerId>}. If the key is
   * new, also sets its TTL to avoid leaking idle-stream keys.
   *
   * @param streamId target stream
   * @param viewerId unique viewer identifier (UUID string)
   * @param timestampMs event timestamp in epoch milliseconds (used as the score)
   */
  public void recordJoin(String streamId, String viewerId, long timestampMs) {
    String key = buildKey(streamId);
    knownStreamIds.add(streamId);

    // ZADD: idempotent — if viewerId already exists, score is updated in place
    redisTemplate.opsForZSet().add(key, viewerId, (double) timestampMs);
    // Refresh TTL on every write so the key survives active streams
    redisTemplate.expire(key, KEY_TTL);

    log.trace("JOIN recorded: stream={} viewer={} ts={}", streamId, viewerId, timestampMs);
  }

  /**
   * Records a viewer DROP event.
   *
   * <p>SPEC-04 R2: {@code ZREM viewer_count:{streamId} <viewerId>}.
   *
   * @param streamId target stream
   * @param viewerId viewer that left
   */
  public void recordDrop(String streamId, String viewerId) {
    String key = buildKey(streamId);
    knownStreamIds.add(streamId);

    redisTemplate.opsForZSet().remove(key, (Object) viewerId);
    log.trace("DROP recorded: stream={} viewer={}", streamId, viewerId);
  }

  /**
   * Returns the current live viewer count for a stream.
   *
   * <p>SPEC-04 R4: {@code ZCARD viewer_count:{streamId}}.
   *
   * @param streamId target stream
   * @return number of viewers currently in the sorted set; 0 if key does not exist
   */
  public long getLiveCount(String streamId) {
    Long count = redisTemplate.opsForZSet().size(buildKey(streamId));
    return count == null ? 0L : count;
  }

  /**
   * Removes sorted-set members whose score (join timestamp) is older than 5 minutes.
   *
   * <p>SPEC-04 R3: called every 10 s by {@link
   * com.streamflow.processor.scheduler.StaleViewerEvictionTask}. Handles missing DROP events for
   * viewers who disconnected without sending a DROP.
   *
   * @param streamId target stream
   */
  public void evictStaleEntries(String streamId) {
    long cutoffMs = System.currentTimeMillis() - WINDOW_MS;
    String key = buildKey(streamId);

    Long removed =
        redisTemplate
            .opsForZSet()
            .removeRangeByScore(key, Double.NEGATIVE_INFINITY, (double) cutoffMs);

    if (removed != null && removed > 0) {
      log.debug("Evicted {} stale entries from stream={}", removed, streamId);
    }
  }

  /**
   * Returns the set of all stream IDs for which this aggregator has seen events. Used by the
   * eviction scheduler to iterate over active streams.
   */
  public Set<String> getKnownStreamIds() {
    return Set.copyOf(knownStreamIds);
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  static String buildKey(String streamId) {
    return KEY_PREFIX + streamId;
  }
}
