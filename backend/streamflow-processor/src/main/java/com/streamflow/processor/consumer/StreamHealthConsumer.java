package com.streamflow.processor.consumer;

import com.streamflow.common.dto.StreamHealthEventDTO;
import com.streamflow.processor.metrics.ProcessorMetrics;
import java.time.Duration;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for the {@code stream-health} topic.
 *
 * <p>SPEC-09 R2 — on each event:
 *
 * <ol>
 *   <li>Deserializes a {@link StreamHealthEventDTO} from JSON.
 *   <li>Stores the latest value for each field in the Redis Hash {@code stream_health:{streamId}}
 *       (TTL 60 s, refreshed on every write).
 *       <ul>
 *         <li>{@code bitrateKbps} → String integer
 *         <li>{@code frameDropRate} → String double
 *         <li>{@code encoderLatencyMs} → String integer
 *         <li>{@code cdnEdgeNode} → String
 *         <li>{@code timestamp} → String long
 *       </ul>
 * </ol>
 *
 * <p>No manual ack is used here because stream-health events are idempotent (the latest value
 * always overwrites the previous) and the consumer group does not require exactly-once delivery
 * semantics. Auto-commit is acceptable. The container factory uses auto-ack (BATCH) mode.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamHealthConsumer {

  /** Redis key prefix for the per-stream health hash (SPEC-09 R2). */
  public static final String HEALTH_KEY_PREFIX = "stream_health:";

  /** Hash field names stored in the health hash. */
  public static final String FIELD_BITRATE = "bitrateKbps";

  public static final String FIELD_FRAME_DROP = "frameDropRate";
  public static final String FIELD_LATENCY = "encoderLatencyMs";
  public static final String FIELD_CDN_EDGE = "cdnEdgeNode";
  public static final String FIELD_TIMESTAMP = "timestamp";

  /** TTL for the health hash (SPEC-09 R2 — 60 s). Refreshed on every event. */
  public static final Duration HEALTH_TTL = Duration.ofSeconds(60);

  private final RedisTemplate<String, String> redisTemplate;
  private final ProcessorMetrics processorMetrics;

  /**
   * Processes a single stream-health event.
   *
   * <p>The container factory is {@code streamHealthListenerContainerFactory} defined in {@link
   * com.streamflow.processor.config.KafkaConsumerConfig}.
   *
   * @param event the deserialized {@link StreamHealthEventDTO}
   */
  @KafkaListener(
      topics = "${streamflow.kafka.topics.stream-health:stream-health}",
      groupId = "${spring.kafka.consumer.group-id:stream-processor-group}",
      containerFactory = "streamHealthListenerContainerFactory")
  public void consume(@Payload StreamHealthEventDTO event) {
    String key = HEALTH_KEY_PREFIX + event.streamId();

    Map<String, String> fields =
        Map.of(
            FIELD_BITRATE, String.valueOf(event.bitrateKbps()),
            FIELD_FRAME_DROP, String.valueOf(event.frameDropRate()),
            FIELD_LATENCY, String.valueOf(event.encoderLatencyMs()),
            FIELD_CDN_EDGE, event.cdnEdgeNode(),
            FIELD_TIMESTAMP, String.valueOf(event.timestamp()));

    redisTemplate.opsForHash().putAll(key, fields);
    redisTemplate.expire(key, HEALTH_TTL);

    // SPEC-20 R3: count successfully processed health events
    processorMetrics.incrementHealthEventsConsumed();

    log.trace(
        "Health event cached: stream={} bitrate={} frameDrop={} latency={}",
        event.streamId(),
        event.bitrateKbps(),
        event.frameDropRate(),
        event.encoderLatencyMs());
  }
}
