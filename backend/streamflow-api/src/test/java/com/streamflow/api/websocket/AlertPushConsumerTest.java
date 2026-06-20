package com.streamflow.api.websocket;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Unit test for {@link AlertPushConsumer} (SPEC-14 R1 / AC3 — schema validation).
 *
 * <p>Verifies that:
 * <ol>
 *   <li>The correct STOMP destination is used ({@code /topic/streams/{streamId}/alerts}).</li>
 *   <li>The {@link AlertWsMessage} envelope has {@code type = "ALERT_FIRED"}.</li>
 *   <li>All payload fields are correctly mapped from {@link AlertEventDTO}.</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
class AlertPushConsumerTest {

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    @InjectMocks
    private AlertPushConsumer consumer;

    @Test
    void consume_broadcastsToCorrectDestination() {
        String streamId = "stream-001";
        AlertEventDTO alert = new AlertEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                AlertSeverity.CRITICAL,
                AlertType.HIGH_BUFFER_RATE,
                5.0,
                8.3,
                "Buffer rate 8.3% exceeds threshold 5.0%",
                System.currentTimeMillis()
        );

        consumer.consume(alert);

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(
                eq("/topic/streams/stream-001/alerts"),
                payloadCaptor.capture()
        );

        Object sent = payloadCaptor.getValue();
        assertThat(sent).isInstanceOf(AlertWsMessage.class);
        AlertWsMessage msg = (AlertWsMessage) sent;

        // AC3: schema matches plan §10 ALERT_FIRED
        assertThat(msg.type()).isEqualTo("ALERT_FIRED");
        assertThat(msg.alertId()).isEqualTo(alert.alertId());
        assertThat(msg.streamId()).isEqualTo(streamId);
        assertThat(msg.severity()).isEqualTo(AlertSeverity.CRITICAL);
        assertThat(msg.alertType()).isEqualTo(AlertType.HIGH_BUFFER_RATE);
        assertThat(msg.message()).isEqualTo(alert.message());
        assertThat(msg.ts()).isEqualTo(alert.timestamp());
    }

    @Test
    void consume_usesAlertFiredTypeConstant() {
        // Verify the TYPE constant matches what plan §10 specifies
        assertThat(AlertWsMessage.TYPE).isEqualTo("ALERT_FIRED");
    }
}
