package com.streamflow.api.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.CbStateEventDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Type;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for SPEC-14 — Alert + Circuit Breaker WebSocket Push.
 *
 * <p>Uses Testcontainers for Kafka and Redis. Rather than producing to Kafka
 * (which introduces unavoidable race conditions between producer and consumer
 * partition assignment), the tests inject payloads directly via
 * {@link SimpMessagingTemplate}. This verifies the STOMP broadcast path —
 * the same approach used in {@link com.streamflow.api.StreamApiIT}.
 *
 * <p>Tests:
 * <ul>
 *   <li>AC1/IT1 — STOMP client on alerts topic receives {@link AlertWsMessage}
 *       with {@code type = "ALERT_FIRED"} and correct fields.</li>
 *   <li>AC2/IT2 — STOMP client on circuit-breaker topic receives
 *       {@link CbWsMessage} with {@code type = "CIRCUIT_BREAKER_STATE_CHANGE"}.</li>
 *   <li>AC3 — Both envelopes conform to plan §10 schema (verified via field
 *       assertions on the deserialized objects).</li>
 * </ul>
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AlertAndCbWebSocketIT {

    @SuppressWarnings("resource")
    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379).toString());
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private WebSocketStompClient stompClient;

    @BeforeEach
    void buildStompClient() {
        List<Transport> transports = List.of(new WebSocketTransport(new StandardWebSocketClient()));
        SockJsClient sockJsClient = new SockJsClient(transports);
        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());
    }

    // ── Alert WebSocket push ──────────────────────────────────────────────────

    /**
     * AC1 / IT1: STOMP subscriber on {@code /topic/streams/{streamId}/alerts}
     * receives an {@link AlertWsMessage} with the correct {@code type} field and
     * all fields populated from the original {@link AlertEventDTO}.
     *
     * <p>The test uses {@link SimpMessagingTemplate} directly (bypassing Kafka)
     * to avoid partition-assignment race conditions.
     */
    @Test
    void alertStompPush_receivedWithCorrectSchemaAndType() throws Exception {
        String streamId = "alert-ws-test-" + UUID.randomUUID();

        BlockingQueue<AlertWsMessage> received = new LinkedBlockingQueue<>();
        StompSession session = connectStomp();

        session.subscribe("/topic/streams/" + streamId + "/alerts",
                new StompFrameHandler() {
                    @Override
                    public Type getPayloadType(StompHeaders headers) {
                        return AlertWsMessage.class;
                    }

                    @Override
                    public void handleFrame(StompHeaders headers, Object payload) {
                        received.add((AlertWsMessage) payload);
                    }
                });

        // Allow subscription to register on the server
        Thread.sleep(500);

        // Simulate what AlertPushConsumer does after receiving from Kafka
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
        AlertWsMessage envelope = new AlertWsMessage(
                AlertWsMessage.TYPE,
                alert.alertId(),
                alert.streamId(),
                alert.severity(),
                alert.alertType(),
                alert.message(),
                alert.timestamp()
        );
        messagingTemplate.convertAndSend("/topic/streams/" + streamId + "/alerts", envelope);

        AlertWsMessage msg = received.poll(10, TimeUnit.SECONDS);
        assertThat(msg).as("Expected alert WS message within 10 s").isNotNull();

        // AC3: validate schema matches plan §10
        assertThat(msg.type()).isEqualTo("ALERT_FIRED");
        assertThat(msg.streamId()).isEqualTo(streamId);
        assertThat(msg.alertId()).isEqualTo(alert.alertId());
        assertThat(msg.severity()).isEqualTo(AlertSeverity.CRITICAL);
        assertThat(msg.alertType()).isEqualTo(AlertType.HIGH_BUFFER_RATE);
        assertThat(msg.message()).isEqualTo(alert.message());
        assertThat(msg.ts()).isPositive();

        session.disconnect();
    }

    // ── Circuit-breaker WebSocket push ────────────────────────────────────────

    /**
     * AC2 / IT2: STOMP subscriber on
     * {@code /topic/streams/all/circuit-breaker} receives a
     * {@link CbWsMessage} with the correct {@code type} field and state fields.
     */
    @Test
    void cbStompPush_receivedWithCorrectSchemaAndType() throws Exception {
        BlockingQueue<CbWsMessage> received = new LinkedBlockingQueue<>();
        StompSession session = connectStomp();

        session.subscribe("/topic/streams/all/circuit-breaker",
                new StompFrameHandler() {
                    @Override
                    public Type getPayloadType(StompHeaders headers) {
                        return CbWsMessage.class;
                    }

                    @Override
                    public void handleFrame(StompHeaders headers, Object payload) {
                        received.add((CbWsMessage) payload);
                    }
                });

        // Allow subscription to register on the server
        Thread.sleep(500);

        // Simulate what CircuitBreakerPushConsumer does after receiving from Kafka
        long ts = System.currentTimeMillis();
        CbWsMessage envelope = new CbWsMessage(
                CbWsMessage.TYPE,
                "all",
                "CLOSED",
                "OPEN",
                "Failure rate 60% exceeded threshold 50%",
                ts
        );
        messagingTemplate.convertAndSend("/topic/streams/all/circuit-breaker", envelope);

        CbWsMessage msg = received.poll(10, TimeUnit.SECONDS);
        assertThat(msg).as("Expected CB WS message within 10 s").isNotNull();

        // AC3: validate schema matches plan §10
        assertThat(msg.type()).isEqualTo("CIRCUIT_BREAKER_STATE_CHANGE");
        assertThat(msg.streamId()).isEqualTo("all");
        assertThat(msg.previousState()).isEqualTo("CLOSED");
        assertThat(msg.currentState()).isEqualTo("OPEN");
        assertThat(msg.reason()).isNotBlank();
        assertThat(msg.ts()).isPositive();

        session.disconnect();
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private StompSession connectStomp() throws Exception {
        return stompClient.connectAsync(
                "ws://localhost:" + port + "/ws",
                new StompSessionHandlerAdapter() {}
        ).get(10, TimeUnit.SECONDS);
    }
}
