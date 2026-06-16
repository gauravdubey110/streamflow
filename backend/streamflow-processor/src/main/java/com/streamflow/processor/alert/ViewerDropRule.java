package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Alert rule that fires when the rolling 30-second viewer delta drops below
 * {@code -10%} of the current mean viewer count.
 *
 * <p>SPEC-11 R2 — ViewerDropRule:
 * <ul>
 *   <li>On each evaluation, pushes the current {@code viewerDelta} into the Redis
 *       Sorted Set {@code viewer_delta:{streamId}} scored by the current epoch-ms.</li>
 *   <li>Prunes entries older than {@code streamflow.alerts.viewer-drop.window-seconds}
 *       (default 30 s) via {@code ZREMRANGEBYSCORE}.</li>
 *   <li>Computes the sum of all deltas in the window.  If
 *       {@code sum < -(dropThresholdPct/100) * liveViewerCount}, fires WARNING.</li>
 *   <li>Severity is always WARNING (spec does not specify CRITICAL for this rule).</li>
 * </ul>
 *
 * <p>The sorted-set key has a TTL of {@code window + 30 s} refreshed on each write.
 * This rule is otherwise stateless.
 */
@Slf4j
@Component
public class ViewerDropRule implements AlertRule {

    /** Redis key prefix for rolling viewer-delta sorted sets (SPEC-11 R2). */
    static final String DELTA_KEY_PREFIX = "viewer_delta:";

    private final long windowSeconds;
    private final double dropThresholdPct;

    public ViewerDropRule(
            @Value("${streamflow.alerts.viewer-drop.window-seconds:30}") long windowSeconds,
            @Value("${streamflow.alerts.viewer-drop.drop-threshold-pct:10.0}") double dropThresholdPct) {
        this.windowSeconds = windowSeconds;
        this.dropThresholdPct = dropThresholdPct;
    }

    @Override
    public Optional<AlertEventDTO> evaluate(StreamMetricSnapshotDTO snapshot,
                                            RedisTemplate<String, String> redisTemplate) {
        String streamId = snapshot.streamId();
        String key = DELTA_KEY_PREFIX + streamId;
        long nowMs = System.currentTimeMillis();
        long cutoffMs = nowMs - (windowSeconds * 1_000L);

        // Push the current viewerDelta into the sorted set (score = timestamp)
        redisTemplate.opsForZSet().add(key, String.valueOf(snapshot.viewerDelta()), (double) nowMs);

        // Prune entries older than the window
        redisTemplate.opsForZSet().removeRangeByScore(key, Double.NEGATIVE_INFINITY, (double) cutoffMs);

        // Refresh TTL: window + 30 s buffer so the key survives idle streams
        redisTemplate.expire(key, Duration.ofSeconds(windowSeconds + 30));

        // Sum all deltas in the window
        Set<ZSetOperations.TypedTuple<String>> entries =
                redisTemplate.opsForZSet().rangeByScoreWithScores(
                        key, (double) cutoffMs, (double) nowMs);

        if (entries == null || entries.isEmpty()) {
            return Optional.empty();
        }

        long deltaSum = entries.stream()
                .mapToLong(e -> parseLong(e.getValue()))
                .sum();

        long liveCount = snapshot.liveViewerCount();
        if (liveCount <= 0) {
            return Optional.empty();
        }

        double dropPct = dropThresholdPct / 100.0;
        double threshold = -(dropPct * liveCount);

        if (deltaSum >= threshold) {
            return Optional.empty();
        }

        // Rule fired: viewer loss in window exceeds threshold
        double actualDropPct = -100.0 * deltaSum / liveCount;

        String message = String.format(
                "Viewer drop %.1f%% (delta=%d) exceeds threshold %.1f%% over %ds window on %s",
                actualDropPct, deltaSum, dropThresholdPct, windowSeconds, streamId);

        AlertEventDTO alert = new AlertEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                AlertSeverity.WARNING,
                AlertType.VIEWER_DROP,
                dropThresholdPct,
                actualDropPct,
                message,
                nowMs
        );

        log.debug("ViewerDropRule triggered: stream={} deltaSum={} threshold={} actualDropPct={}",
                streamId, deltaSum, threshold, actualDropPct);
        return Optional.of(alert);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static long parseLong(String s) {
        if (s == null) return 0L;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
