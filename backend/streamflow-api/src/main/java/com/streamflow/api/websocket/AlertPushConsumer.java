package com.streamflow.api.websocket;

import com.streamflow.api.metrics.ApiMetrics;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.AlertEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that forwards alert events to WebSocket subscribers (SPEC-14 R1).
 *
 * <p>Consumes {@link AlertEventDTO} from the {@code alerts} topic (group {@code api-gateway-group})
 * and broadcasts each alert to the STOMP destination {@code /topic/streams/{streamId}/alerts}.
 *
 * <p>The outbound message is wrapped in an {@link AlertWsMessage} envelope that adds the {@code
 * type} discriminator field ({@code "ALERT_FIRED"}) so that the React client can handle multiple
 * message types on the same subscription (SPEC-14 R4).
 *
 * <p>Thread-safety: {@link SimpMessagingTemplate} is thread-safe; Kafka listener container
 * dispatches on its own thread pool.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertPushConsumer {

  /** STOMP destination prefix for per-stream alert notifications. */
  private static final String ALERTS_DESTINATION_PREFIX = "/topic/streams/";

  private static final String ALERTS_DESTINATION_SUFFIX = "/alerts";

  private final SimpMessagingTemplate messagingTemplate;
  private final ApiMetrics apiMetrics;

  /**
   * Receives an alert from the {@code alerts} Kafka topic and broadcasts it to the STOMP
   * destination for the alert's stream.
   *
   * @param alert the deserialized alert event from the {@code alerts} topic
   */
  @KafkaListener(
      topics = "${streamflow.kafka.topics.alerts:" + KafkaTopics.ALERTS + "}",
      groupId = "${spring.kafka.consumer.group-id:api-gateway-group}",
      containerFactory = "alertListenerContainerFactory")
  public void consume(@Payload AlertEventDTO alert) {
    String destination = ALERTS_DESTINATION_PREFIX + alert.streamId() + ALERTS_DESTINATION_SUFFIX;

    AlertWsMessage message =
        new AlertWsMessage(
            AlertWsMessage.TYPE,
            alert.alertId(),
            alert.streamId(),
            alert.severity(),
            alert.alertType(),
            alert.message(),
            alert.timestamp());

    messagingTemplate.convertAndSend(destination, message);
    // SPEC-20 R3: count events consumed from alerts topic
    apiMetrics.incrementAlertsConsumed();
    log.info(
        "SPEC-14: alert pushed to {}: alertId={} severity={} type={}",
        destination,
        alert.alertId(),
        alert.severity(),
        alert.alertType());
  }
}
