package com.streamflow.api.websocket;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.CbStateEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that forwards circuit-breaker state changes to WebSocket subscribers (SPEC-14 R2).
 *
 * <p>Consumes {@link CbStateEventDTO} from the {@code cb-events} topic (group {@code
 * api-gateway-group}) and broadcasts each event to the STOMP destination {@code
 * /topic/streams/{streamId}/circuit-breaker}.
 *
 * <p>Because the current circuit breaker is a single named instance (not per-stream), the {@code
 * streamId} in the event is {@code "all"} and the broadcast destination is {@code
 * /topic/streams/all/circuit-breaker}. Future per-stream circuit breakers only require publishing
 * with a real {@code streamId} — no API changes are needed.
 *
 * <p>The outbound message is wrapped in a {@link CbWsMessage} envelope that adds the {@code type}
 * discriminator field ({@code "CIRCUIT_BREAKER_STATE_CHANGE"}) per SPEC-14 R4.
 *
 * <p>Thread-safety: {@link SimpMessagingTemplate} is thread-safe.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CircuitBreakerPushConsumer {

  /** STOMP destination prefix for per-stream circuit-breaker notifications. */
  private static final String CB_DESTINATION_PREFIX = "/topic/streams/";

  private static final String CB_DESTINATION_SUFFIX = "/circuit-breaker";

  private final SimpMessagingTemplate messagingTemplate;

  /**
   * Receives a CB state event from the {@code cb-events} Kafka topic and broadcasts it to the STOMP
   * destination for the event's stream.
   *
   * @param event the deserialized CB state-change event
   */
  @KafkaListener(
      topics = "${streamflow.kafka.topics.cb-events:" + KafkaTopics.CB_EVENTS + "}",
      groupId = "${spring.kafka.consumer.group-id:api-gateway-group}",
      containerFactory = "cbListenerContainerFactory")
  public void consume(@Payload CbStateEventDTO event) {
    String destination = CB_DESTINATION_PREFIX + event.streamId() + CB_DESTINATION_SUFFIX;

    CbWsMessage message =
        new CbWsMessage(
            CbWsMessage.TYPE,
            event.streamId(),
            event.previousState(),
            event.currentState(),
            event.reason(),
            event.ts());

    messagingTemplate.convertAndSend(destination, message);
    log.info(
        "SPEC-14: CB state change pushed to {}: {} → {}",
        destination,
        event.previousState(),
        event.currentState());
  }
}
