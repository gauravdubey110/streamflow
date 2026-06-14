package com.streamflow.processor.snapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.processor.aggregator.HealthScoreCalculator;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
import com.streamflow.processor.consumer.StreamHealthConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes a {@link StreamMetricSnapshotDTO} for each active stream every second.
 *
 * <p>SPEC-05 requirements implemented here:
 * <ul>
 *   <li>R1 – Runs {@code @Scheduled(fixedRate = 1000)}; discovers active streams
 *             via {@code SMEMBERS active_streams} in Redis.</li>
 *   <li>R2 – Builds a snapshot with {@code liveViewerCount} ({@code ZCARD}) and
 *             {@code viewerDelta}; all other numeric fields default to 0/empty.</li>
 *   <li>R3 – Writes JSON to {@code stream_snapshot:{streamId}} with a 30-second TTL.</li>
 *   <li>R4 – Publishes snapshot to the {@code metrics-aggregated} Kafka topic,
 *             keyed by {@code streamId}.</li>
 *   <li>R5 – Logs a WARN if the scheduled run takes more than 800ms.</li>
 * </ul>
 *
 * <p>SPEC-09 additions:
 * <ul>
 *   <li>R4 – Reads {@code stream_health:{streamId}} hash written by
 *             {@link StreamHealthConsumer} and populates {@code healthScore}
 *             (via {@link HealthScoreCalculator}) and {@code p95LatencyMs}
 *             (= {@code encoderLatencyMs}) in the snapshot.</li>
 *   <li>AC4 – If the health hash has expired (TTL exceeded 60 s), falls back
 *             to a default health score of 50.0 and logs a WARN.</li>
 * </ul>
 *
 * <p><b>Scaling note (NFR1):</b> the single-threaded {@code @Scheduled} task is
 * sufficient for ≤ 10 streams. Beyond that, the task should be partitioned by
 * stream range or replaced with a reactive pipeline (e.g. Project Reactor with a
 * bounded parallel scheduler) and the {@code active_streams} SMEMBERS scan moved
 * to a cursor-based SSCAN to avoid blocking Redis.
 */
@Slf4j
@Component
public class SnapshotPublisher {

    /** Redis key prefix for current snapshot strings (SPEC-05 R3). */
    private static final String SNAPSHOT_KEY_PREFIX = "stream_snapshot:";

    /** Redis key for the active-stream set maintained by {@link com.streamflow.processor.consumer.ViewerEventConsumer}. */
    private static final String ACTIVE_STREAMS_KEY = "active_streams";

    /** TTL applied to each snapshot key on write (SPEC-05 R3). */
    private static final Duration SNAPSHOT_TTL = Duration.ofSeconds(30);

    /** Threshold above which the scheduler emits a WARN (SPEC-05 R5). */
    private static final long WARN_THRESHOLD_MS = 800L;

    /**
     * Health score returned when the {@code stream_health:{streamId}} hash has expired
     * (SPEC-09 AC4). Represents a degraded-but-not-zero state so the dashboard does
     * not show a misleading 100.
     */
    static final double DEFAULT_HEALTH_SCORE_ON_EXPIRY = 50.0;

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, StreamMetricSnapshotDTO> snapshotKafkaTemplate;
    private final ViewerCountAggregator viewerCountAggregator;
    private final HealthScoreCalculator healthScoreCalculator;
    private final ObjectMapper objectMapper;
    private final Timer snapshotTimer;

    /**
     * In-memory map of previous-second viewer counts per stream.
     * Used to compute {@code viewerDelta = current − previous} (SPEC-05 R2).
     */
    private final Map<String, Long> previousCounts = new ConcurrentHashMap<>();

    public SnapshotPublisher(
            RedisTemplate<String, String> redisTemplate,
            KafkaTemplate<String, StreamMetricSnapshotDTO> snapshotKafkaTemplate,
            ViewerCountAggregator viewerCountAggregator,
            HealthScoreCalculator healthScoreCalculator,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.redisTemplate = redisTemplate;
        this.snapshotKafkaTemplate = snapshotKafkaTemplate;
        this.viewerCountAggregator = viewerCountAggregator;
        this.healthScoreCalculator = healthScoreCalculator;
        this.objectMapper = objectMapper;

        // SPEC-05 task §5: Micrometer timer around the scheduled run
        this.snapshotTimer = Timer.builder("streamflow.snapshot.publish")
                .description("Time taken for the snapshot-publish scheduled run")
                .register(meterRegistry);
    }

    /**
     * Runs every second. For each active stream:
     * <ol>
     *   <li>Reads live viewer count from Redis (ZCARD).</li>
     *   <li>Computes delta vs. previous run.</li>
     *   <li>Writes JSON snapshot to Redis with a 30 s TTL.</li>
     *   <li>Publishes snapshot to {@code metrics-aggregated} Kafka topic.</li>
     * </ol>
     *
     * <p>Emits a WARN log if the entire run takes more than 800ms (SPEC-05 R5).
     */
    @Scheduled(fixedRate = 1000)
    public void publishSnapshots() {
        long startMs = System.currentTimeMillis();

        snapshotTimer.record(() -> {
            Set<String> activeStreams = redisTemplate.opsForSet().members(ACTIVE_STREAMS_KEY);
            if (activeStreams == null || activeStreams.isEmpty()) {
                log.debug("No active streams — skipping snapshot publish cycle");
                return;
            }

            log.debug("Publishing snapshots for {} active stream(s)", activeStreams.size());

            for (String streamId : activeStreams) {
                try {
                    publishSnapshotForStream(streamId);
                } catch (Exception e) {
                    // SPEC-05: do not let one bad stream abort the whole cycle
                    log.error("Failed to publish snapshot for stream={}: {}", streamId, e.getMessage(), e);
                }
            }
        });

        long elapsedMs = System.currentTimeMillis() - startMs;
        if (elapsedMs > WARN_THRESHOLD_MS) {
            log.warn("Snapshot publish cycle took {}ms — exceeds {}ms threshold (SPEC-05 R5)",
                    elapsedMs, WARN_THRESHOLD_MS);
        }
    }

    // ── internal ──────────────────────────────────────────────────────────────

    /**
     * Builds and distributes the snapshot for a single stream.
     *
     * <p>SPEC-09: reads the {@code stream_health:{streamId}} Redis Hash to populate
     * {@code healthScore} and {@code p95LatencyMs}. Falls back to
     * {@link #DEFAULT_HEALTH_SCORE_ON_EXPIRY} with a WARN log when the hash has
     * expired (SPEC-09 AC4).
     *
     * @param streamId the stream to snapshot
     * @throws JsonProcessingException if Jackson fails to serialize the snapshot
     */
    private void publishSnapshotForStream(String streamId) throws JsonProcessingException {
        long currentCount = viewerCountAggregator.getLiveCount(streamId);
        long previous = previousCounts.getOrDefault(streamId, currentCount);
        long delta = currentCount - previous;
        previousCounts.put(streamId, currentCount);

        // SPEC-09 R4: read health hash and compute health score
        HealthFields health = readHealthFields(streamId);

        StreamMetricSnapshotDTO snapshot = new StreamMetricSnapshotDTO(
                streamId,
                null,                 // streamName — populated in later specs
                currentCount,
                delta,
                0.0,                  // bufferRatePct — SPEC-10
                health.p95LatencyMs,  // p95LatencyMs  = encoderLatencyMs (SPEC-09 R4)
                Map.of(),             // qualityDistribution — SPEC-10
                health.healthScore,   // healthScore   (SPEC-09 R3)
                "CLOSED",             // circuitBreakerState placeholder — SPEC-12
                0,                    // activeAlerts  — SPEC-11
                System.currentTimeMillis()
        );

        // SPEC-05 R3: SET stream_snapshot:{streamId} <json> EX 30
        String snapshotJson = objectMapper.writeValueAsString(snapshot);
        redisTemplate.opsForValue().set(
                SNAPSHOT_KEY_PREFIX + streamId,
                snapshotJson,
                SNAPSHOT_TTL
        );

        // SPEC-05 R4: publish to metrics-aggregated topic, key = streamId
        snapshotKafkaTemplate.send(KafkaTopics.METRICS_AGGREGATED, streamId, snapshot);

        log.trace("Snapshot published: stream={} count={} delta={} health={}",
                streamId, currentCount, delta, health.healthScore);
    }

    /**
     * Reads the {@code stream_health:{streamId}} Redis Hash and returns the derived
     * health fields.
     *
     * <p>If the hash is absent (expired after 60 s without a health event), returns
     * a fallback with {@link #DEFAULT_HEALTH_SCORE_ON_EXPIRY} and 0 latency, and
     * logs a WARN (SPEC-09 AC4 / Q1 resolution).
     *
     * @param streamId the stream to look up
     * @return a {@link HealthFields} value object
     */
    @SuppressWarnings("unchecked")
    private HealthFields readHealthFields(String streamId) {
        String key = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;
        Map<Object, Object> raw = redisTemplate.opsForHash().entries(key);

        if (raw == null || raw.isEmpty()) {
            log.warn("Health hash '{}' is absent (expired or never written). " +
                    "Defaulting healthScore={} for stream={} (SPEC-09 AC4)",
                    key, DEFAULT_HEALTH_SCORE_ON_EXPIRY, streamId);
            return new HealthFields(DEFAULT_HEALTH_SCORE_ON_EXPIRY, 0);
        }

        try {
            double bufferRatePct  = 0.0; // SPEC-10 will provide this; use 0 for now
            double frameDropRate  = parseDouble(raw, StreamHealthConsumer.FIELD_FRAME_DROP);
            int encoderLatencyMs  = parseInt(raw, StreamHealthConsumer.FIELD_LATENCY);
            int bitrateKbps       = parseInt(raw, StreamHealthConsumer.FIELD_BITRATE);

            double score = healthScoreCalculator.compute(
                    bufferRatePct, frameDropRate, encoderLatencyMs, bitrateKbps);

            return new HealthFields(score, encoderLatencyMs);
        } catch (Exception e) {
            log.warn("Failed to parse health hash for stream={}: {}. Defaulting to {}.",
                    streamId, e.getMessage(), DEFAULT_HEALTH_SCORE_ON_EXPIRY);
            return new HealthFields(DEFAULT_HEALTH_SCORE_ON_EXPIRY, 0);
        }
    }

    private double parseDouble(Map<Object, Object> map, String field) {
        Object val = map.get(field);
        return val != null ? Double.parseDouble(val.toString()) : 0.0;
    }

    private int parseInt(Map<Object, Object> map, String field) {
        Object val = map.get(field);
        return val != null ? (int) Double.parseDouble(val.toString()) : 0;
    }

    /**
     * Lightweight value object carrying the two health-derived fields that feed
     * into the snapshot (SPEC-09 R4).
     *
     * @param healthScore    computed composite health score [0, 100]
     * @param p95LatencyMs   encoder latency used as p95 proxy (SPEC-09 R4)
     */
    private record HealthFields(double healthScore, int p95LatencyMs) {}
}
