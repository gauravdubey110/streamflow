package com.streamflow.producer.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.ViewerEventDTO;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link ViewerEventProducer} via {@link StreamSimulator}.
 *
 * <p>SPEC-03 Test Plan — Integration (Testcontainers): Boot Spring with {@code tps=200}, consume
 * {@code viewer-events}, assert ≥ 100 messages arrive within 2 seconds (generous budget for CI
 * variability).
 *
 * <p>Uses {@link KafkaContainer} (Confluent 7.5.3) matching the dev docker-compose.
 */
@Testcontainers
@SpringBootTest(
    properties = {
      "streamflow.simulation.tps=200",
      "streamflow.simulation.enabled=true",
      "streamflow.simulation.streams[0].id=stream-001",
      "streamflow.simulation.streams[0].name=Test Stream",
      "streamflow.simulation.streams[0].base-viewers=1000"
    })
class ViewerEventProducerIT {

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

  @DynamicPropertySource
  static void kafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired private StreamSimulator streamSimulator;

  @Test
  void producerSendsAtLeast100MessagesWithin2Seconds() throws Exception {
    // Allow simulator to produce for 2 seconds
    Thread.sleep(2_000);

    KafkaConsumer<String, String> consumer = createRawConsumer();
    consumer.subscribe(List.of(KafkaTopics.VIEWER_EVENTS));

    List<String> received = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 3_000; // 3s poll window
    while (System.currentTimeMillis() < deadline && received.size() < 100) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      records.forEach(r -> received.add(r.value()));
    }
    consumer.close();

    assertThat(received)
        .as("Expected ≥ 100 viewer-events from the producer within the poll window")
        .hasSizeGreaterThanOrEqualTo(100);
  }

  @Test
  void producedMessagesAreKeyedByStreamId() throws Exception {
    Thread.sleep(1_000);

    KafkaConsumer<String, String> consumer = createRawConsumer();
    consumer.subscribe(List.of(KafkaTopics.VIEWER_EVENTS));

    List<String> keys = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 3_000;
    while (System.currentTimeMillis() < deadline && keys.size() < 50) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      records.forEach(r -> keys.add(r.key()));
    }
    consumer.close();

    assertThat(keys)
        .as("All message keys should equal the configured stream ID")
        .isNotEmpty()
        .allMatch(key -> "stream-001".equals(key));
  }

  @Test
  void producedMessagesDeserializeToViewerEventDTO() throws Exception {
    Thread.sleep(1_000);

    KafkaConsumer<String, String> consumer = createRawConsumer();
    consumer.subscribe(List.of(KafkaTopics.VIEWER_EVENTS));

    ObjectMapper mapper = new ObjectMapper();
    List<ViewerEventDTO> events = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 3_000;
    while (System.currentTimeMillis() < deadline && events.size() < 10) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      for (var record : records) {
        events.add(mapper.readValue(record.value(), ViewerEventDTO.class));
      }
    }
    consumer.close();

    assertThat(events).as("Messages should deserialise cleanly to ViewerEventDTO").isNotEmpty();

    ViewerEventDTO sample = events.get(0);
    assertThat(sample.streamId()).isEqualTo("stream-001");
    assertThat(sample.eventId()).isNotBlank();
    assertThat(sample.viewerId()).isNotBlank();
    assertThat(sample.eventType()).isNotNull();
    assertThat(sample.quality()).isNotNull();
    assertThat(sample.timestamp()).isGreaterThan(0L);
  }

  @Test
  void streamSimulatorReportsHealthy() throws Exception {
    // After 2s, at least one producer should have published
    Thread.sleep(2_000);

    assertThat(streamSimulator.getProducers())
        .as("StreamSimulator should have one producer per configured stream")
        .hasSize(1);

    assertThat(streamSimulator.getProducers().get(0).isHealthy())
        .as("Producer should be healthy after publishing events for 2s")
        .isTrue();
  }

  private KafkaConsumer<String, String> createRawConsumer() {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "test-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class));
  }
}
