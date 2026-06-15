package com.streamflow.processor.consumer;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test: produce {@link ViewerEventDTO} events to Kafka,
 * assert the {@link ViewerEventConsumer} processes them and updates Redis via
 * {@link ViewerCountAggregator}.
 *
 * <p>SPEC-04 Test Plan — Integration: end-to-end produce → consume → assert ZCARD.
 *
 * <p>Containers:
 * <ul>
 *   <li>{@code KafkaContainer} (Confluent 7.5.3) — matches dev docker-compose</li>
 *   <li>{@code GenericContainer} with {@code redis:7.2-alpine}</li>
 * </ul>
 *
 * <p>Cassandra auto-configuration is excluded via {@code spring.autoconfigure.exclude}
 * because SPEC-17 implements the Cassandra schema — there is no Cassandra in this spec.
 *
 * <p>Each test method uses a unique stream ID (UUID suffix) to prevent Redis key
 * collisions across tests sharing the same containers. Redis is flushed before
 * each test to avoid cross-test contamination.
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ViewerEventConsumerIT {

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
    }

    @Autowired
    private ViewerCountAggregator viewerCountAggregator;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    /**
     * Flush Redis before each test to prevent cross-test contamination.
     * Each test also uses a unique stream ID, so this is belt-and-suspenders.
     */
    @BeforeEach
    void flushRedis() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private KafkaTemplate<String, ViewerEventDTO> buildProducerTemplate() {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS, false
        );
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    private ViewerEventDTO joinEvent(String streamId, String viewerId) {
        return new ViewerEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                viewerId,
                EventType.JOIN,
                VideoQuality.Q_1080P,
                null,
                System.currentTimeMillis(),
                "IN-MH"
        );
    }

    private ViewerEventDTO dropEvent(String streamId, String viewerId) {
        return new ViewerEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                viewerId,
                EventType.DROP,
                VideoQuality.Q_1080P,
                null,
                System.currentTimeMillis(),
                "IN-MH"
        );
    }

    // ── AC1: 1000 JOINs → ZCARD == 1000 ─────────────────────────────────────

    @Test
    void ac1_thousandJoinEventsYieldZCardOfThousand() {
        String streamId = "it-stream-ac1-" + UUID.randomUUID();
        KafkaTemplate<String, ViewerEventDTO> producer = buildProducerTemplate();

        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>(KafkaTopics.VIEWER_EVENTS, streamId,
                    joinEvent(streamId, "viewer-" + i)));
        }
        producer.flush();

        // SPEC-10: timeout increased from 60s to 120s to accommodate the additional
        // Redis writes added by QualityDistAggregator (2 HINCRBY per JOIN event).
        await().atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(viewerCountAggregator.getLiveCount(streamId))
                                .as("ZCARD should reach 1000 after 1000 JOIN events")
                                .isEqualTo(1000L));
    }

    // ── AC2: 1000 JOINs + 200 DROPs → ZCARD == 800 ──────────────────────────

    @Test
    void ac2_thousandJoinsMinus200DropsYieldEightHundred() {
        String streamId = "it-stream-ac2-" + UUID.randomUUID();
        KafkaTemplate<String, ViewerEventDTO> producer = buildProducerTemplate();

        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>(KafkaTopics.VIEWER_EVENTS, streamId,
                    joinEvent(streamId, "viewer-" + i)));
        }
        // Drop the first 200 viewers (subset)
        for (int i = 0; i < 200; i++) {
            producer.send(new ProducerRecord<>(KafkaTopics.VIEWER_EVENTS, streamId,
                    dropEvent(streamId, "viewer-" + i)));
        }
        producer.flush();

        // Wait for all 1200 messages to be consumed. Since we use a unique streamId,
        // we wait for the count to reach exactly 800 (JOINs - DROPs).
        // SPEC-10: timeout increased from 60s to 120s to accommodate the additional
        // Redis writes added by QualityDistAggregator (2+ HINCRBY per JOIN/DROP event).
        await().atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(viewerCountAggregator.getLiveCount(streamId))
                                .as("ZCARD should stabilise at 800 after 200 DROPs from 1000 JOINs")
                                .isEqualTo(800L));
    }

    // ── AC3: Idempotency — same JOIN 5× → ZCARD == 1 ────────────────────────

    @Test
    void ac3_sameJoinFiveTimesIsIdempotent() {
        String streamId = "it-stream-ac3-" + UUID.randomUUID();
        String viewerId = "viewer-idempotent";
        KafkaTemplate<String, ViewerEventDTO> producer = buildProducerTemplate();

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(KafkaTopics.VIEWER_EVENTS, streamId,
                    joinEvent(streamId, viewerId)));
        }
        producer.flush();

        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertThat(viewerCountAggregator.getLiveCount(streamId))
                                .as("ZCARD should be exactly 1 for the same viewerId sent 5 times")
                                .isEqualTo(1L));
    }
}
