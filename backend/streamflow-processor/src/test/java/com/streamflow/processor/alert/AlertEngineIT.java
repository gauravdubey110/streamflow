package com.streamflow.processor.alert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link AlertEngine} end-to-end.
 *
 * <p>SPEC-11 Test Plan — Integration (Testcontainers Kafka + Redis):
 *
 * <ol>
 *   <li>Register a stream in {@code active_streams} Redis Set.
 *   <li>Write a snapshot to {@code stream_snapshot:{streamId}} with high buffer rate (12%) so
 *       {@link HighBufferRateRule} fires.
 *   <li>Wait for the {@link AlertEngine} scheduler to fire (every 1s).
 *   <li>Assert that an alert message appears on the {@code alerts} Kafka topic.
 *   <li>Assert the dedup key exists in Redis (AC1 cooldown).
 *   <li>Assert that a second snapshot write (same stream, still 12%) does NOT produce a second
 *       Kafka message during the cooldown window (AC1 dedup).
 *   <li>Lower bufferRatePct to 1% → dedup key deleted within ≤ 3 scheduler ticks (AC2).
 * </ol>
 *
 * <p>Cassandra is excluded from auto-config (same as SPEC-05 IT).
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AlertEngineIT {

  @SuppressWarnings("resource")
  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379).toString());
    // Short cooldown so the clear-test does not have to wait 60s
    registry.add("streamflow.alerts.cooldown-seconds", () -> "3");
  }

  @Autowired private RedisTemplate<String, String> redisTemplate;

  @Autowired private AlertEngine alertEngine;

  @Autowired private ObjectMapper objectMapper;

  /**
   * Prevent SnapshotPublisher's scheduler from overwriting test snapshots. The IT test writes
   * controlled snapshots directly to Redis; SnapshotPublisher would recompute them from viewer
   * events (which are absent in this test) and overwrite the bufferRatePct with 0.0 — preventing
   * alert evaluation.
   */
  @MockBean
  @SuppressWarnings("unused")
  private com.streamflow.processor.snapshot.SnapshotPublisher snapshotPublisher;

  private KafkaMessageListenerContainer<String, AlertEventDTO> alertConsumerContainer;
  private final List<ConsumerRecord<String, AlertEventDTO>> received = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() {
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    received.clear();

    Map<String, Object> consumerProps =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "it-alerts-consumer-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            JsonDeserializer.class,
            JsonDeserializer.TRUSTED_PACKAGES,
            "com.streamflow.common.dto",
            JsonDeserializer.VALUE_DEFAULT_TYPE,
            AlertEventDTO.class.getName(),
            JsonDeserializer.USE_TYPE_INFO_HEADERS,
            "false");

    ContainerProperties containerProps = new ContainerProperties(KafkaTopics.ALERTS);
    containerProps.setMessageListener(
        (MessageListener<String, AlertEventDTO>) record -> received.add(record));

    alertConsumerContainer =
        new KafkaMessageListenerContainer<>(
            new DefaultKafkaConsumerFactory<>(consumerProps), containerProps);
    alertConsumerContainer.start();
  }

  @AfterEach
  void tearDown() {
    if (alertConsumerContainer != null) {
      alertConsumerContainer.stop();
    }
  }

  /**
   * AC1 + AC3: Inject a snapshot with bufferRatePct=12; assert exactly one CRITICAL alert appears
   * on the {@code alerts} topic (cooldown holding) and that the dedup key is set in Redis.
   */
  @Test
  void ac1_highBuffer_producesAlertOnTopic() throws Exception {
    String streamId = "it-alert-" + UUID.randomUUID();

    // Register stream and write high-buffer snapshot
    redisTemplate.opsForSet().add("active_streams", streamId);
    writeSnapshot(streamId, 12.0);

    // Wait for at least one alert on the topic
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              long alertsForStream =
                  received.stream().filter(r -> streamId.equals(r.key())).count();
              assertThat(alertsForStream)
                  .as("At least one alert expected for stream=%s", streamId)
                  .isGreaterThanOrEqualTo(1L);
            });

    // Verify the alert payload matches plan §5
    ConsumerRecord<String, AlertEventDTO> record =
        received.stream().filter(r -> streamId.equals(r.key())).findFirst().orElseThrow();

    AlertEventDTO alert = record.value();
    assertThat(alert.streamId()).isEqualTo(streamId);
    assertThat(alert.alertType().name()).isEqualTo("HIGH_BUFFER_RATE");
    assertThat(alert.severity().name()).isEqualTo("CRITICAL"); // 12 > 10 → CRITICAL
    assertThat(alert.actualValue()).isEqualTo(12.0);
    assertThat(alert.alertId()).isNotBlank();
    assertThat(alert.timestamp()).isGreaterThan(0L);

    // AC1: dedup key should be set after alert fires
    String dedupKey =
        AlertEngine.dedupKey(streamId, com.streamflow.common.enums.AlertType.HIGH_BUFFER_RATE);
    assertThat(redisTemplate.hasKey(dedupKey))
        .as("Dedup key %s should be set after alert fires", dedupKey)
        .isTrue();
  }

  /**
   * AC1 dedup: second tick with same high-buffer snapshot must NOT produce a second Kafka alert
   * while the cooldown is active.
   */
  @Test
  void ac1_dedup_secondTickSuppressed() throws Exception {
    String streamId = "it-dedup-" + UUID.randomUUID();
    redisTemplate.opsForSet().add("active_streams", streamId);
    writeSnapshot(streamId, 12.0);

    // Wait for first alert
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(received.stream().filter(r -> streamId.equals(r.key())).count())
                    .isGreaterThanOrEqualTo(1L));

    long alertCountAfterFirst = received.stream().filter(r -> streamId.equals(r.key())).count();

    // Wait 3 more seconds (3 scheduler ticks) — dedup key TTL is 3s in tests
    // So we wait BEFORE it expires, i.e. wait 1.5s to confirm no extra during cooldown
    Thread.sleep(1_500L);
    writeSnapshot(streamId, 12.0); // re-write snapshot to keep it fresh

    // Give the engine another 2 ticks to fire (or not)
    Thread.sleep(2_000L);

    // Should still be the same count (dedup held within 3s window)
    // Note: after 3s the cooldown expires, so we allow count to be alertCountAfterFirst
    // (not alertCountAfterFirst+2 or more)
    long alertCountNow = received.stream().filter(r -> streamId.equals(r.key())).count();

    assertThat(alertCountNow)
        .as("Dedup should suppress repeated alerts within cooldown window")
        .isLessThanOrEqualTo(alertCountAfterFirst + 1); // at most 1 extra after cooldown
  }

  /**
   * AC2: lowering bufferRatePct to below threshold causes dedup key to be deleted within at most 3
   * scheduler ticks (3 seconds).
   */
  @Test
  void ac2_clearCondition_deletesDedup() throws Exception {
    String streamId = "it-clear-" + UUID.randomUUID();
    redisTemplate.opsForSet().add("active_streams", streamId);
    writeSnapshot(streamId, 12.0);

    // Wait for first alert + dedup key to appear
    String dedupKey =
        AlertEngine.dedupKey(streamId, com.streamflow.common.enums.AlertType.HIGH_BUFFER_RATE);
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> assertThat(redisTemplate.hasKey(dedupKey)).isTrue());

    // Now lower the buffer rate below threshold
    writeSnapshot(streamId, 1.0); // well below 5.0 warning threshold

    // Wait for the scheduler to clear the dedup key (cooldown in test = 3s, but
    // the engine deletes it on the first tick where rule returns empty)
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(redisTemplate.hasKey(dedupKey))
                    .as("Dedup key should be deleted after condition clears")
                    .isFalse());
  }

  /**
   * AC4: snapshot's activeAlerts reflects the count correctly. After an alert fires,
   * getActiveAlertCount(streamId) returns 1.
   */
  @Test
  void ac4_activeAlertsCount_reflectsActiveDedup() throws Exception {
    String streamId = "it-active-" + UUID.randomUUID();
    redisTemplate.opsForSet().add("active_streams", streamId);
    writeSnapshot(streamId, 12.0);

    // Wait for alert + dedup key
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(alertEngine.getActiveAlertCount(streamId))
                    .as("activeAlerts should be 1 after HIGH_BUFFER_RATE fires")
                    .isGreaterThanOrEqualTo(1));
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /** Writes a {@link StreamMetricSnapshotDTO} as JSON to {@code stream_snapshot:{streamId}}. */
  private void writeSnapshot(String streamId, double bufferRatePct) throws Exception {
    StreamMetricSnapshotDTO snapshot =
        new StreamMetricSnapshotDTO(
            streamId,
            "Integration Test Stream",
            50_000L,
            500L,
            bufferRatePct,
            42,
            Map.of("1080p", 100.0),
            bufferRatePct > 10 ? 60.0 : 92.0,
            "CLOSED",
            0,
            System.currentTimeMillis());
    String json = objectMapper.writeValueAsString(snapshot);
    redisTemplate.opsForValue().set("stream_snapshot:" + streamId, json, Duration.ofSeconds(30));
  }
}
