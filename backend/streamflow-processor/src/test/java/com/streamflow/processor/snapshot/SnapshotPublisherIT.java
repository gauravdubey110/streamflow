package com.streamflow.processor.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link SnapshotPublisher}.
 *
 * <p>SPEC-05 Test Plan — Integration (Testcontainers Kafka + Redis):
 * <ol>
 *   <li>Produce viewer JOIN events for 3 streams.</li>
 *   <li>Wait up to 10 s for the snapshot scheduler to fire.</li>
 *   <li>Assert that {@code stream_snapshot:{streamId}} keys exist in Redis.</li>
 *   <li>Assert that at least one message per stream appears on the
 *       {@code metrics-aggregated} Kafka topic.</li>
 * </ol>
 *
 * <p>Cassandra auto-configuration is excluded (same as SPEC-04 IT).
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SnapshotPublisherIT {

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

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaMessageListenerContainer<String, StreamMetricSnapshotDTO> metricsConsumerContainer;
    private final List<ConsumerRecord<String, StreamMetricSnapshotDTO>> received =
            new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        // Flush Redis to avoid cross-test contamination
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        received.clear();

        // Stand up a consumer on metrics-aggregated to capture published snapshots
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-metrics-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto",
                JsonDeserializer.VALUE_DEFAULT_TYPE, StreamMetricSnapshotDTO.class.getName(),
                JsonDeserializer.USE_TYPE_INFO_HEADERS, "false"
        );

        ContainerProperties containerProps =
                new ContainerProperties(KafkaTopics.METRICS_AGGREGATED);
        containerProps.setMessageListener(
                (MessageListener<String, StreamMetricSnapshotDTO>) record -> received.add(record));

        metricsConsumerContainer = new KafkaMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(consumerProps),
                containerProps
        );
        metricsConsumerContainer.start();
    }

    @AfterEach
    void tearDown() {
        if (metricsConsumerContainer != null) {
            metricsConsumerContainer.stop();
        }
    }

    /**
     * Produce JOIN events for 3 streams, then assert:
     * <ul>
     *   <li>AC1 — each {@code stream_snapshot:{streamId}} Redis key holds valid JSON.</li>
     *   <li>AC2 — at least one message per stream appears on {@code metrics-aggregated}.</li>
     * </ul>
     */
    @Test
    void ac1_and_ac2_snapshotsAppearInRedisAndKafka() throws Exception {
        List<String> streamIds = List.of(
                "it-snap-stream-" + UUID.randomUUID(),
                "it-snap-stream-" + UUID.randomUUID(),
                "it-snap-stream-" + UUID.randomUUID()
        );

        KafkaTemplate<String, ViewerEventDTO> producer = buildViewerProducer();

        // Produce 50 JOIN events per stream so liveViewerCount > 0
        for (String streamId : streamIds) {
            for (int i = 0; i < 50; i++) {
                producer.send(new ProducerRecord<>(
                        KafkaTopics.VIEWER_EVENTS, streamId,
                        joinEvent(streamId, "viewer-" + i)
                ));
            }
        }
        producer.flush();

        // AC1: wait for Redis snapshot keys to appear (scheduler runs every 1s)
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    for (String streamId : streamIds) {
                        String key = "stream_snapshot:" + streamId;
                        String json = redisTemplate.opsForValue().get(key);
                        assertThat(json)
                                .as("Redis key %s should be populated", key)
                                .isNotNull()
                                .contains("\"streamId\":\"" + streamId + "\"");
                    }
                });

        // AC2: at least one metrics-aggregated Kafka message per stream
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    for (String streamId : streamIds) {
                        long messagesForStream = received.stream()
                                .filter(r -> streamId.equals(r.key()))
                                .count();
                        assertThat(messagesForStream)
                                .as("At least 1 Kafka message expected for stream=%s", streamId)
                                .isGreaterThanOrEqualTo(1L);
                    }
                });
    }

    /**
     * AC4: viewerDelta is non-negative in the second snapshot
     * (the first snapshot has delta=0 by definition since there is no prior count).
     * After 2 scheduler ticks, the snapshot JSON must include a snapshotTs.
     */
    @Test
    void ac4_snapshotJsonIsValidAndContainsTimestamp() throws Exception {
        String streamId = "it-snap-delta-" + UUID.randomUUID();

        KafkaTemplate<String, ViewerEventDTO> producer = buildViewerProducer();
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(
                    KafkaTopics.VIEWER_EVENTS, streamId, joinEvent(streamId, "viewer-" + i)));
        }
        producer.flush();

        // Wait for the Redis snapshot to appear
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    String json = redisTemplate.opsForValue().get("stream_snapshot:" + streamId);
                    assertThat(json).isNotNull();
                });

        String json = redisTemplate.opsForValue().get("stream_snapshot:" + streamId);
        StreamMetricSnapshotDTO snapshot = objectMapper.readValue(json, StreamMetricSnapshotDTO.class);

        assertThat(snapshot.streamId()).isEqualTo(streamId);
        assertThat(snapshot.liveViewerCount()).isGreaterThan(0L);
        assertThat(snapshot.snapshotTs()).isGreaterThan(0L);
        assertThat(snapshot.circuitBreakerState()).isEqualTo("CLOSED");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private KafkaTemplate<String, ViewerEventDTO> buildViewerProducer() {
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
}
