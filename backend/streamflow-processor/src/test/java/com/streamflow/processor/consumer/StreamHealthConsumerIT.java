package com.streamflow.processor.consumer;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamHealthEventDTO;
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
 * Integration test for {@link StreamHealthConsumer}.
 *
 * <p>SPEC-09 Test Plan — Integration: produce a degraded health event to the
 * {@code stream-health} topic and assert:
 * <ol>
 *   <li>AC1 — {@code HGETALL stream_health:{streamId}} returns the correct field values.</li>
 *   <li>Test-plan requirement — degraded event produces a computed score &lt; 60
 *       (verified via {@link com.streamflow.processor.aggregator.HealthScoreCalculator}
 *       in the unit tests; here we verify the Redis hash fields are written correctly
 *       so the end-to-end chain would compute the expected score).</li>
 * </ol>
 *
 * <p>Containers: Kafka (Confluent 7.5.3) + Redis 7.2-alpine — matches dev stack.
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StreamHealthConsumerIT {

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
    private RedisTemplate<String, String> redisTemplate;

    @BeforeEach
    void flushRedis() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ── AC1: health hash is populated in Redis ────────────────────────────────

    /**
     * Produce a normal health event and assert that all five hash fields appear
     * in {@code stream_health:{streamId}} within 15 seconds.
     *
     * <p>SPEC-09 AC1: {@code redis-cli HGETALL stream_health:stream-001} returns
     * recent bitrate/frame-drop/latency.
     */
    @Test
    void ac1_healthHashPopulatedInRedis() {
        String streamId = "it-health-ac1-" + UUID.randomUUID();
        StreamHealthEventDTO event = new StreamHealthEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                4_500,   // bitrateKbps
                0.02,    // frameDropRate
                120,     // encoderLatencyMs
                "edge-mumbai-01",
                System.currentTimeMillis()
        );

        sendHealthEvent(streamId, event);

        String redisKey = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Map<Object, Object> hash = redisTemplate.opsForHash().entries(redisKey);
                    assertThat(hash)
                            .as("stream_health hash should be non-empty after consuming the event")
                            .isNotEmpty();
                    assertThat(hash.get(StreamHealthConsumer.FIELD_BITRATE))
                            .as("bitrateKbps field should be '4500'")
                            .isEqualTo("4500");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_FRAME_DROP))
                            .as("frameDropRate field should be '0.02'")
                            .isEqualTo("0.02");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_LATENCY))
                            .as("encoderLatencyMs field should be '120'")
                            .isEqualTo("120");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_CDN_EDGE))
                            .as("cdnEdgeNode field should be 'edge-mumbai-01'")
                            .isEqualTo("edge-mumbai-01");
                });
    }

    // ── Degraded event: fields written correctly (score < 60 chain) ──────────

    /**
     * SPEC-09 Test Plan: push a degraded health event and assert the hash fields
     * that would cause a {@link com.streamflow.processor.aggregator.HealthScoreCalculator}
     * computation of score &lt; 60.
     *
     * <p>Degraded inputs: bitrate=2000 kbps, frameDrop=0.025, latency=300 ms.
     * Expected score = 100 − 40(buffer,0) − 25(frameDrop) − 15(latency) − 10(bitrate) = 10.
     * (bufferRatePct is 0.0 in SPEC-09; SPEC-10 provides it later.)
     * Actual score via calculator = 100 − 0 − 25 − 15 − 10 = 50 → below 60.
     */
    @Test
    void degradedEvent_hashFieldsWrittenCorrectly() {
        String streamId = "it-health-degraded-" + UUID.randomUUID();
        StreamHealthEventDTO degraded = new StreamHealthEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                2_000,   // bitrateKbps — below free threshold (3000)
                0.025,   // frameDropRate — penalty = 25
                300,     // encoderLatencyMs — excess = 150, penalty = 15
                "edge-london-01",
                System.currentTimeMillis()
        );

        sendHealthEvent(streamId, degraded);

        String redisKey = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Map<Object, Object> hash = redisTemplate.opsForHash().entries(redisKey);
                    assertThat(hash).isNotEmpty();

                    // Verify each field is written correctly for the degraded scenario
                    assertThat(hash.get(StreamHealthConsumer.FIELD_BITRATE))
                            .as("bitrateKbps should be 2000")
                            .isEqualTo("2000");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_FRAME_DROP))
                            .as("frameDropRate should be 0.025")
                            .isEqualTo("0.025");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_LATENCY))
                            .as("encoderLatencyMs should be 300")
                            .isEqualTo("300");
                });
    }

    // ── Latest value wins (idempotency) ──────────────────────────────────────

    /**
     * Sending two events for the same stream should result in the second event's
     * values being present in the hash (latest-write-wins semantics).
     */
    @Test
    void latestEvent_overwritesPreviousHashValues() {
        String streamId = "it-health-overwrite-" + UUID.randomUUID();

        StreamHealthEventDTO first = new StreamHealthEventDTO(
                UUID.randomUUID().toString(), streamId,
                4_500, 0.02, 120, "edge-mumbai-01", System.currentTimeMillis());

        StreamHealthEventDTO second = new StreamHealthEventDTO(
                UUID.randomUUID().toString(), streamId,
                3_000, 0.01, 100, "edge-delhi-01", System.currentTimeMillis() + 2_000);

        KafkaTemplate<String, StreamHealthEventDTO> producer = buildHealthProducer();
        producer.send(new ProducerRecord<>(KafkaTopics.STREAM_HEALTH, streamId, first));
        producer.send(new ProducerRecord<>(KafkaTopics.STREAM_HEALTH, streamId, second));
        producer.flush();

        String redisKey = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Map<Object, Object> hash = redisTemplate.opsForHash().entries(redisKey);
                    assertThat(hash).isNotEmpty();
                    // The second event's bitrate (3000) must be present
                    assertThat(hash.get(StreamHealthConsumer.FIELD_BITRATE))
                            .as("Latest event's bitrateKbps (3000) should overwrite first (4500)")
                            .isEqualTo("3000");
                    assertThat(hash.get(StreamHealthConsumer.FIELD_CDN_EDGE))
                            .as("Latest event's cdnEdgeNode (edge-delhi-01) should overwrite first")
                            .isEqualTo("edge-delhi-01");
                });
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void sendHealthEvent(String streamId, StreamHealthEventDTO event) {
        KafkaTemplate<String, StreamHealthEventDTO> producer = buildHealthProducer();
        producer.send(new ProducerRecord<>(KafkaTopics.STREAM_HEALTH, streamId, event));
        producer.flush();
    }

    private KafkaTemplate<String, StreamHealthEventDTO> buildHealthProducer() {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS, false
        );
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
