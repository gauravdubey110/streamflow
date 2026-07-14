package com.streamflow.api.websocket;

import com.streamflow.api.metrics.ApiMetrics;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that forwards metric snapshots to WebSocket subscribers.
 *
 * <p>SPEC-06 R2: Consumes {@link StreamMetricSnapshotDTO} from the {@code metrics-aggregated} topic
 * (group {@code api-gateway-group}) and broadcasts each snapshot to the STOMP destination {@code
 * /topic/streams/{streamId}/metrics}.
 *
 * <p>Clients subscribe to {@code /topic/streams/{streamId}/metrics} and receive a new message
 * approximately every second (driven by {@code SnapshotPublisher} in the processor module —
 * SPEC-05).
 *
 * <p>Thread-safety: {@link SimpMessagingTemplate} is thread-safe; the Kafka listener container
 * dispatches on its own thread pool. Increasing concurrency on the container factory ({@code
 * snapshotListenerContainerFactory}) is safe here as a result.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsPushConsumer {

  /** STOMP destination prefix for per-stream metric updates (SPEC-06 R2). */
  private static final String METRICS_DESTINATION_PREFIX = "/topic/streams/";

  private static final String METRICS_DESTINATION_SUFFIX = "/metrics";

  private final SimpMessagingTemplate messagingTemplate;
  private final ApiMetrics apiMetrics;

  /**
   * Receives a metric snapshot from Kafka and broadcasts it over STOMP.
   *
   * @param snapshot the deserialized snapshot from {@code metrics-aggregated}
   */
  @KafkaListener(
      topics =
          "${streamflow.kafka.topics.metrics-aggregated:" + KafkaTopics.METRICS_AGGREGATED + "}",
      groupId = "${spring.kafka.consumer.group-id:api-gateway-group}",
      containerFactory = "snapshotListenerContainerFactory")
  public void consume(@Payload StreamMetricSnapshotDTO snapshot) {
    String destination =
        METRICS_DESTINATION_PREFIX + snapshot.streamId() + METRICS_DESTINATION_SUFFIX;
    messagingTemplate.convertAndSend(destination, snapshot);
    // SPEC-20 R3: count events consumed from metrics-aggregated topic
    apiMetrics.incrementMetricsConsumed();
    log.trace("Pushed snapshot to {} — viewers={}", destination, snapshot.liveViewerCount());
  }
}
