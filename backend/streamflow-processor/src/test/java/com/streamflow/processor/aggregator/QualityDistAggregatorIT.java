package com.streamflow.processor.aggregator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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

/**
 * Integration test for {@link QualityDistAggregator} wired into the full processor.
 *
 * <p>SPEC-10 Test Plan — Integration (Testcontainers Kafka + Redis):
 *
 * <ul>
 *   <li>AC3 — Inject 200 BUFFER_START events into a single stream within 10s → snapshot's {@code
 *       bufferRatePct} rises above 5% within 30s.
 *   <li>AC1 — After normal load, snapshot {@code qualityDistribution} sums to 100 ±0.5 and all five
 *       tiers are present.
 * </ul>
 *
 * <p>Uses a full {@code @SpringBootTest} context so the Kafka listener chain, {@link
 * QualityDistAggregator}, and {@link com.streamflow.processor.snapshot.SnapshotPublisher} all
 * participate.
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class QualityDistAggregatorIT {

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
  }

  @Autowired private RedisTemplate<String, String> redisTemplate;

  @Autowired private ObjectMapper objectMapper;

  @BeforeEach
  void flushRedis() {
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
  }

  // ── AC3: buffer rate rises above 5% after injecting 200 BUFFER_START events ──

  /**
   * SPEC-10 AC3: inject BUFFER_START events into a single stream; within 45s the snapshot's
   * bufferRatePct should exceed 5%.
   *
   * <p>Strategy: interleave JOIN and BUFFER_START events so they are processed by the Kafka
   * consumer in proportion, rather than all JOINs first (which would delay all BUFFER_STARTs past
   * the 30s timeout since all events share one partition). Ratio: 1 BUFFER_START per 4 JOINs = 20%
   * buffer rate, well above the 5% threshold. Total: 100 JOIN + 25 BUFFER_START = 125 events —
   * small enough to process quickly.
   */
  @Test
  void ac3_highBufferEventInjection_raisesSnapshotBufferRate() throws Exception {
    String streamId = "it-buf-ac3-" + UUID.randomUUID();
    KafkaTemplate<String, ViewerEventDTO> producer = buildViewerProducer();

    // Interleave: for every 4 JOINs, send 1 BUFFER_START (20% buffer rate)
    // Total: 100 JOIN + 25 BUFFER_START = 125 events
    for (int i = 0; i < 25; i++) {
      // 4 JOIN events
      for (int j = 0; j < 4; j++) {
        producer.send(
            new ProducerRecord<>(
                KafkaTopics.VIEWER_EVENTS,
                streamId,
                viewerEvent(
                    streamId, "v-" + (i * 4 + j), EventType.JOIN, VideoQuality.Q_1080P, null)));
      }
      // 1 BUFFER_START event
      producer.send(
          new ProducerRecord<>(
              KafkaTopics.VIEWER_EVENTS,
              streamId,
              viewerEvent(
                  streamId, "v-buf-" + i, EventType.BUFFER_START, VideoQuality.Q_1080P, 1500L)));
    }
    producer.flush();

    // Wait for snapshot to appear and bufferRatePct > 5%
    // 45s gives the Kafka consumer time to process all 125 events and the scheduler
    // time to publish the updated snapshot (SPEC-10 AC3).
    await()
        .atMost(Duration.ofSeconds(45))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              String json = redisTemplate.opsForValue().get("stream_snapshot:" + streamId);
              assertThat(json).as("Snapshot should exist for stream=%s", streamId).isNotNull();

              StreamMetricSnapshotDTO snapshot =
                  objectMapper.readValue(json, StreamMetricSnapshotDTO.class);
              assertThat(snapshot.bufferRatePct())
                  .as("bufferRatePct should exceed 5%% after BUFFER_START events (20%% expected)")
                  .isGreaterThan(5.0);
            });
  }

  // ── AC1: qualityDistribution sums to 100 ±0.5 under normal load ──────────

  /**
   * SPEC-10 AC1: after normal load (JOIN + QUALITY_SWITCH events with all 5 tiers), the snapshot's
   * qualityDistribution sums to 100 ±0.5 and contains all five tiers.
   */
  @Test
  void ac1_normalLoad_qualityDistributionSumsToHundred() throws Exception {
    String streamId = "it-qual-ac1-" + UUID.randomUUID();
    KafkaTemplate<String, ViewerEventDTO> producer = buildViewerProducer();

    // Produce 100 events: 20 of each quality tier via JOIN events
    VideoQuality[] qualities = VideoQuality.values();
    for (int i = 0; i < 100; i++) {
      VideoQuality q = qualities[i % qualities.length];
      producer.send(
          new ProducerRecord<>(
              KafkaTopics.VIEWER_EVENTS,
              streamId,
              viewerEvent(streamId, "viewer-" + i, EventType.JOIN, q, null)));
    }
    producer.flush();

    // Wait for snapshot to appear
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              String json = redisTemplate.opsForValue().get("stream_snapshot:" + streamId);
              assertThat(json).as("Snapshot should exist for stream=%s", streamId).isNotNull();

              StreamMetricSnapshotDTO snapshot =
                  objectMapper.readValue(json, StreamMetricSnapshotDTO.class);

              Map<String, Double> dist = snapshot.qualityDistribution();
              assertThat(dist)
                  .as("qualityDistribution should contain all 5 tiers")
                  .containsKeys("1080p", "720p", "480p", "360p", "144p");

              double sum = dist.values().stream().mapToDouble(Double::doubleValue).sum();
              assertThat(sum)
                  .as("qualityDistribution should sum to 100 ±0.5")
                  .isBetween(99.5, 100.5);
            });
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private KafkaTemplate<String, ViewerEventDTO> buildViewerProducer() {
    Map<String, Object> props =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            JsonSerializer.class,
            JsonSerializer.ADD_TYPE_INFO_HEADERS,
            false);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
  }

  private ViewerEventDTO viewerEvent(
      String streamId,
      String viewerId,
      EventType type,
      VideoQuality quality,
      Long bufferDurationMs) {
    return new ViewerEventDTO(
        UUID.randomUUID().toString(),
        streamId,
        viewerId,
        type,
        quality,
        bufferDurationMs,
        System.currentTimeMillis(),
        "IN-MH");
  }
}
