package com.streamflow.api.websocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.streamflow.common.dto.CbStateEventDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

/**
 * Unit test for {@link CircuitBreakerPushConsumer} (SPEC-14 R2 / AC3 — schema validation).
 *
 * <p>Verifies that:
 *
 * <ol>
 *   <li>The correct STOMP destination is used ({@code /topic/streams/{streamId}/circuit-breaker}).
 *   <li>The {@link CbWsMessage} envelope has {@code type = "CIRCUIT_BREAKER_STATE_CHANGE"}.
 *   <li>All payload fields are correctly mapped from {@link CbStateEventDTO}.
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
class CircuitBreakerPushConsumerTest {

  @Mock private SimpMessagingTemplate messagingTemplate;

  @InjectMocks private CircuitBreakerPushConsumer consumer;

  @Test
  void consume_broadcastsToCorrectDestination() {
    long now = System.currentTimeMillis();
    CbStateEventDTO event =
        new CbStateEventDTO(
            "all", "CLOSED", "OPEN", "Failure rate 60% exceeded threshold 50%", now);

    consumer.consume(event);

    ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
    verify(messagingTemplate)
        .convertAndSend(eq("/topic/streams/all/circuit-breaker"), payloadCaptor.capture());

    Object sent = payloadCaptor.getValue();
    assertThat(sent).isInstanceOf(CbWsMessage.class);
    CbWsMessage msg = (CbWsMessage) sent;

    // AC3: schema matches plan §10 CIRCUIT_BREAKER_STATE_CHANGE
    assertThat(msg.type()).isEqualTo("CIRCUIT_BREAKER_STATE_CHANGE");
    assertThat(msg.streamId()).isEqualTo("all");
    assertThat(msg.previousState()).isEqualTo("CLOSED");
    assertThat(msg.currentState()).isEqualTo("OPEN");
    assertThat(msg.reason()).isEqualTo("Failure rate 60% exceeded threshold 50%");
    assertThat(msg.ts()).isEqualTo(now);
  }

  @Test
  void consume_halfOpenTransition_broadcastsCorrectly() {
    long now = System.currentTimeMillis();
    CbStateEventDTO event = new CbStateEventDTO("all", "OPEN", "HALF_OPEN", "wait elapsed", now);

    consumer.consume(event);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(messagingTemplate)
        .convertAndSend(eq("/topic/streams/all/circuit-breaker"), captor.capture());
    CbWsMessage msg = (CbWsMessage) captor.getValue();
    assertThat(msg.currentState()).isEqualTo("HALF_OPEN");
    assertThat(msg.type()).isEqualTo("CIRCUIT_BREAKER_STATE_CHANGE");
  }

  @Test
  void consume_usesCircuitBreakerStateChangeTypeConstant() {
    // Verify the TYPE constant matches what plan §10 specifies
    assertThat(CbWsMessage.TYPE).isEqualTo("CIRCUIT_BREAKER_STATE_CHANGE");
  }
}
