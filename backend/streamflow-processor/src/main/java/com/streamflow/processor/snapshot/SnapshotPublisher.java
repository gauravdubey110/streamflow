package com.streamflow.processor.snapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
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

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, StreamMetricSnapshotDTO> snapshotKafkaTemplate;
    private final ViewerCountAggregator viewerCountAggregator;
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
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.redisTemplate = redisTemplate;
        this.snapshotKafkaTemplate = snapshotKafkaTemplate;
        this.viewerCountAggregator = viewerCountAggregator;
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
     * @param streamId the stream to snapshot
     * @throws JsonProcessingException if Jackson fails to serialize the snapshot
     */
    private void publishSnapshotForStream(String streamId) throws JsonProcessingException {
        long currentCount = viewerCountAggregator.getLiveCount(streamId);
        long previous = previousCounts.getOrDefault(streamId, currentCount);
        long delta = currentCount - previous;
        previousCounts.put(streamId, currentCount);

        StreamMetricSnapshotDTO snapshot = new StreamMetricSnapshotDTO(
                streamId,
                null,            // streamName — populated in later specs
                currentCount,
                delta,
                0.0,             // bufferRatePct — SPEC-10
                0,               // p95LatencyMs  — SPEC-09
                Map.of(),        // qualityDistribution — SPEC-10
                0.0,             // healthScore   — SPEC-09
                "CLOSED",        // circuitBreakerState placeholder — SPEC-12
                0,               // activeAlerts  — SPEC-11
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

        log.trace("Snapshot published: stream={} count={} delta={}", streamId, currentCount, delta);
    }
}
