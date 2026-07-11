package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertType;
import com.streamflow.processor.metrics.ProcessorMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Evaluates all registered {@link AlertRule}s for every active stream once per second.
 *
 * <p>SPEC-11 R3 — AlertEngine responsibilities:
 * <ul>
 *   <li>Reads the {@code active_streams} Redis Set (same set maintained by
 *       {@link com.streamflow.processor.consumer.ViewerEventConsumer}) to discover streams.</li>
 *   <li>Reads the current snapshot from {@code stream_snapshot:{streamId}} in Redis.</li>
 *   <li>Runs each {@link AlertRule} on the snapshot.</li>
 *   <li>Deduplicates: maintains a {@code last_alert:{streamId}:{alertType}} key in Redis
 *       (TTL = {@code streamflow.alerts.cooldown-seconds}, default 60 s).
 *       If the key is present, the alert is suppressed.
 *       If the rule clears, the key is deleted.</li>
 *   <li>When an alert fires (and is not deduped), delegates to {@link AlertPublisher}.</li>
 * </ul>
 *
 * <p>NFR1: with N=10 streams and 3 rules, one full evaluation cycle must complete in &lt;200 ms.
 * The implementation is single-threaded (shared {@code @Scheduled} executor) and performs
 * only Redis reads/writes (no Cassandra, no heavy computation) — well within the budget.
 *
 * <p>Shutdown: {@link #destroy()} calls {@link AlertPublisher#flush()} to drain in-flight
 * Kafka sends before the JVM exits (SPEC-11 §4).
 */
@Slf4j
@Component
public class AlertEngine {

    /** Redis key for the set of active stream IDs (written by ViewerEventConsumer). */
    private static final String ACTIVE_STREAMS_KEY = "active_streams";

    /** Redis key prefix for current metric snapshot strings (written by SnapshotPublisher). */
    private static final String SNAPSHOT_KEY_PREFIX = "stream_snapshot:";

    /** Redis key prefix for per-stream per-type dedup keys (SPEC-11 R3). */
    static final String DEDUP_KEY_PREFIX = "last_alert:";

    private final RedisTemplate<String, String> redisTemplate;
    private final List<AlertRule> rules;
    private final AlertPublisher alertPublisher;
    private final long cooldownSeconds;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;
    private final ProcessorMetrics processorMetrics;

    public AlertEngine(
            RedisTemplate<String, String> redisTemplate,
            List<AlertRule> rules,
            AlertPublisher alertPublisher,
            @Value("${streamflow.alerts.cooldown-seconds:60}") long cooldownSeconds,
            com.fasterxml.jackson.databind.ObjectMapper objectMapper,
            ProcessorMetrics processorMetrics) {
        this.redisTemplate = redisTemplate;
        this.rules = rules;
        this.alertPublisher = alertPublisher;
        this.cooldownSeconds = cooldownSeconds;
        this.objectMapper = objectMapper;
        this.processorMetrics = processorMetrics;
    }

    // ── Scheduled evaluation ──────────────────────────────────────────────────

    /**
     * Runs every second. Evaluates all rules for all active streams.
     *
     * <p>A per-stream exception is caught so that one failing stream cannot
     * abort the entire cycle (defensive, same pattern as SnapshotPublisher).
     */
    @Scheduled(fixedRate = 1000)
    public void evaluateAll() {
        long startMs = System.currentTimeMillis();

        Set<String> activeStreams = redisTemplate.opsForSet().members(ACTIVE_STREAMS_KEY);
        if (activeStreams == null || activeStreams.isEmpty()) {
            log.trace("AlertEngine: no active streams — skipping evaluation cycle");
            return;
        }

        for (String streamId : activeStreams) {
            try {
                evaluateStream(streamId);
            } catch (Exception e) {
                log.error("AlertEngine: error evaluating stream={}: {}", streamId, e.getMessage(), e);
            }
        }

        long elapsed = System.currentTimeMillis() - startMs;
        if (elapsed > 200) {
            log.warn("AlertEngine: evaluation cycle took {}ms — exceeds 200ms NFR1 budget", elapsed);
        } else {
            log.trace("AlertEngine: evaluation cycle took {}ms for {} streams", elapsed, activeStreams.size());
        }
    }

    /**
     * Returns the number of currently active dedup keys for the given stream.
     *
     * <p>SPEC-11 R5: used by {@link com.streamflow.processor.snapshot.SnapshotPublisher}
     * to populate {@code activeAlerts} in the snapshot.
     *
     * @param streamId the stream to query
     * @return count of active (non-expired) dedup keys for this stream
     */
    public int getActiveAlertCount(String streamId) {
        int count = 0;
        for (AlertType type : AlertType.values()) {
            String dedupKey = dedupKey(streamId, type);
            if (Boolean.TRUE.equals(redisTemplate.hasKey(dedupKey))) {
                count++;
            }
        }
        return count;
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    /**
     * Called on Spring context shutdown. Flushes the alert Kafka producer so that
     * in-flight sends complete before the JVM exits (SPEC-11 §4).
     */
    @PreDestroy
    public void destroy() {
        log.info("AlertEngine shutting down — flushing publisher");
        alertPublisher.flush();
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    /**
     * Evaluates all rules for a single stream.
     *
     * <p>The snapshot is read from Redis as JSON. If the snapshot key is absent
     * (stream active but snapshot not yet written), evaluation is skipped silently.
     *
     * @param streamId the stream to evaluate
     */
    private void evaluateStream(String streamId) {
        StreamMetricSnapshotDTO snapshot = readSnapshot(streamId);
        if (snapshot == null) {
            log.trace("AlertEngine: no snapshot found for stream={} — skipping", streamId);
            return;
        }

        for (AlertRule rule : rules) {
            try {
                Optional<AlertEventDTO> result = rule.evaluate(snapshot, redisTemplate);
                handleResult(streamId, result, alertTypeFor(rule));
            } catch (Exception e) {
                log.error("AlertEngine: rule {} failed for stream={}: {}",
                        rule.getClass().getSimpleName(), streamId, e.getMessage(), e);
            }
        }
    }

    /**
     * Handles the result of a rule evaluation: publishes, suppresses, or clears.
     *
     * @param streamId  the stream being evaluated
     * @param result    the rule's output (non-empty = fired, empty = clear)
     * @param alertType the type that corresponds to the rule (for dedup key construction)
     */
    private void handleResult(String streamId, Optional<AlertEventDTO> result, AlertType alertType) {
        String dedupKey = dedupKey(streamId, alertType);

        if (result.isEmpty()) {
            // Rule cleared — delete the dedup key so future firings are not suppressed
            Boolean deleted = redisTemplate.delete(dedupKey);
            if (Boolean.TRUE.equals(deleted)) {
                log.debug("AlertEngine: cleared dedup key={}", dedupKey);
            }
            return;
        }

        // Rule fired — check dedup
        if (Boolean.TRUE.equals(redisTemplate.hasKey(dedupKey))) {
            log.trace("AlertEngine: alert suppressed (cooldown active): key={}", dedupKey);
            return;
        }

        // Not deduped — publish and set dedup key
        AlertEventDTO alert = result.get();
        alertPublisher.publish(alert);

        // SPEC-20 R3: increment alerts.fired counter tagged by severity + alertType
        processorMetrics.incrementAlertsFired(alert.severity(), alert.alertType());

        // Set dedup key with cooldown TTL
        redisTemplate.opsForValue().set(dedupKey, alert.alertId(), Duration.ofSeconds(cooldownSeconds));
        log.info("AlertEngine: alert published and dedup key set: key={} alertId={} severity={}",
                dedupKey, alert.alertId(), alert.severity());
    }

    /**
     * Reads and deserializes the metric snapshot from Redis.
     *
     * @param streamId the stream to look up
     * @return the deserialized snapshot, or {@code null} if absent or unparseable
     */
    private StreamMetricSnapshotDTO readSnapshot(String streamId) {
        String json = redisTemplate.opsForValue().get(SNAPSHOT_KEY_PREFIX + streamId);
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, StreamMetricSnapshotDTO.class);
        } catch (Exception e) {
            log.warn("AlertEngine: failed to parse snapshot JSON for stream={}: {}", streamId, e.getMessage());
            return null;
        }
    }

    /**
     * Maps a rule implementation to its corresponding {@link AlertType}.
     *
     * <p>This mapping is used to build the dedup key without requiring rules to
     * expose their type. Adding a new rule requires adding a branch here.
     *
     * @param rule the rule to map
     * @return the {@link AlertType} for dedup key construction
     */
    private AlertType alertTypeFor(AlertRule rule) {
        if (rule instanceof HighBufferRateRule) {
            return AlertType.HIGH_BUFFER_RATE;
        } else if (rule instanceof ViewerDropRule) {
            return AlertType.VIEWER_DROP;
        } else if (rule instanceof BitrateDegradationRule) {
            return AlertType.BITRATE_DEGRADATION;
        }
        // Fallback: use the class name as a pseudo-type key (safe for unknown future rules)
        log.warn("AlertEngine: unknown rule type {}, using STREAM_DOWN as fallback dedup type",
                rule.getClass().getSimpleName());
        return AlertType.STREAM_DOWN;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Builds the Redis dedup key for a stream + alert-type combination.
     *
     * <p>Key format: {@code last_alert:{streamId}:{alertType}}  (SPEC-11 R3).
     */
    static String dedupKey(String streamId, AlertType alertType) {
        return DEDUP_KEY_PREFIX + streamId + ":" + alertType.name();
    }

    /**
     * Builds the dedup key map used by external components (e.g. SnapshotPublisher)
     * to count active alerts. Exposed for testing.
     */
    Map<AlertType, String> dedupKeys(String streamId) {
        return Map.of(
                AlertType.HIGH_BUFFER_RATE,    dedupKey(streamId, AlertType.HIGH_BUFFER_RATE),
                AlertType.VIEWER_DROP,         dedupKey(streamId, AlertType.VIEWER_DROP),
                AlertType.BITRATE_DEGRADATION, dedupKey(streamId, AlertType.BITRATE_DEGRADATION)
        );
    }
}
