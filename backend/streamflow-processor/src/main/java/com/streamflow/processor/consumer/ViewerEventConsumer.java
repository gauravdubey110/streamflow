package com.streamflow.processor.consumer;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.processor.aggregator.QualityDistAggregator;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka consumer for the {@code viewer-events} topic.
 *
 * <p>SPEC-04 requirements:
 * <ul>
 *   <li>R1 – {@code @KafkaListener} on {@code viewer-events}, group
 *             {@code stream-processor-group}, concurrency = 3 (set on the factory
 *             bean in {@link com.streamflow.processor.config.KafkaConsumerConfig}).</li>
 *   <li>R2 – JOIN → {@code ZADD}; DROP → {@code ZREM}; other types ignored.</li>
 *   <li>R5 – Idempotency is guaranteed by Redis Sorted-Set semantics:
 *             re-delivering the same viewerId ZADD only updates the score.</li>
 *   <li>R6 – Manual ack after successful Redis write. On Redis failure the
 *             exception propagates and the error handler in
 *             {@link com.streamflow.processor.config.KafkaConsumerConfig}
 *             retries up to 3 times then dead-letters to
 *             {@code viewer-events.DLT}.</li>
 * </ul>
 *
 * <p>SPEC-05 addition:
 * <ul>
 *   <li>R1 – On first event for a stream, {@code SADD active_streams <streamId>}
 *             so that {@link com.streamflow.processor.snapshot.SnapshotPublisher}
 *             can discover active streams via {@code SMEMBERS active_streams}.</li>
 * </ul>
 */
@Slf4j
@Component
public class ViewerEventConsumer {

    /** Redis key for the set of active stream IDs (SPEC-05 R1). */
    static final String ACTIVE_STREAMS_KEY = "active_streams";

    /** TTL for the active-streams set — refreshed on every event (SPEC-05 §4). */
    private static final Duration ACTIVE_STREAMS_TTL = Duration.ofSeconds(300);

    private final ViewerCountAggregator viewerCountAggregator;
    private final QualityDistAggregator qualityDistAggregator;
    private final RedisTemplate<String, String> redisTemplate;

    public ViewerEventConsumer(ViewerCountAggregator viewerCountAggregator,
                               QualityDistAggregator qualityDistAggregator,
                               RedisTemplate<String, String> redisTemplate) {
        this.viewerCountAggregator = viewerCountAggregator;
        this.qualityDistAggregator = qualityDistAggregator;
        this.redisTemplate = redisTemplate;
    }

    /**
     * Processes a single viewer event from the {@code viewer-events} topic.
     *
     * <p>The container factory is {@code viewerEventListenerContainerFactory}
     * defined in {@link com.streamflow.processor.config.KafkaConsumerConfig},
     * which sets concurrency = 3 and ack mode = MANUAL_IMMEDIATE.
     *
     * @param event           the deserialized {@link ViewerEventDTO}
     * @param acknowledgment  manual acknowledgment handle; called after successful
     *                        Redis write to commit the Kafka offset
     * @throws RuntimeException if the Redis write fails — the error handler
     *                          will retry and eventually dead-letter the record
     */
    @KafkaListener(
            topics = "${streamflow.kafka.topics.viewer-events:viewer-events}",
            groupId = "${spring.kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "viewerEventListenerContainerFactory"
    )
    public void consume(@Payload ViewerEventDTO event, Acknowledgment acknowledgment) {
        try {
            processEvent(event);
            // SPEC-04 R6: ack only after Redis write succeeds
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // Let the exception propagate so the error handler can retry / DLT
            log.error("Failed to process viewer event for stream={} viewer={}: {}",
                    event.streamId(), event.viewerId(), e.getMessage());
            throw e;
        }
    }

    // ── internal ───────────────────────────────────────────────────────────────

    private void processEvent(ViewerEventDTO event) {
        EventType type = event.eventType();

        if (type == EventType.JOIN) {
            viewerCountAggregator.recordJoin(event.streamId(), event.viewerId(), event.timestamp());
            trackActiveStream(event.streamId());
            // SPEC-10 R2: JOIN events increment quality distribution counters
            qualityDistAggregator.recordEvent(event);
            log.trace("Processed JOIN: stream={} viewer={}", event.streamId(), event.viewerId());

        } else if (type == EventType.DROP) {
            viewerCountAggregator.recordDrop(event.streamId(), event.viewerId());
            trackActiveStream(event.streamId());
            // SPEC-10 R2: DROP events increment only the TOTAL buffer counter
            qualityDistAggregator.recordEvent(event);
            log.trace("Processed DROP: stream={} viewer={}", event.streamId(), event.viewerId());

        } else if (type == EventType.QUALITY_SWITCH || type == EventType.BUFFER_START) {
            // SPEC-10 R2: QUALITY_SWITCH increments quality tier; BUFFER_START increments BUFFER + TOTAL
            qualityDistAggregator.recordEvent(event);
            log.trace("Processed {} for quality/buffer aggregation: stream={}", type, event.streamId());

        } else {
            // BUFFER_END, ERROR — not aggregated in this spec
            log.trace("Skipping event type {} for stream={}", type, event.streamId());
        }
    }

    /**
     * Registers {@code streamId} in the {@code active_streams} Redis Set and
     * refreshes the set's TTL (SPEC-05 R1).
     *
     * <p>{@code SADD} is idempotent — adding an already-present member is a no-op.
     * The TTL is refreshed on every call so active streams never expire while
     * traffic keeps flowing.
     *
     * @param streamId the stream to register
     */
    void trackActiveStream(String streamId) {
        redisTemplate.opsForSet().add(ACTIVE_STREAMS_KEY, streamId);
        redisTemplate.expire(ACTIVE_STREAMS_KEY, ACTIVE_STREAMS_TTL);
    }
}
