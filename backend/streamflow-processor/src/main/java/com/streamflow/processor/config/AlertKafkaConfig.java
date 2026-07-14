package com.streamflow.processor.config;

import com.streamflow.common.dto.AlertEventDTO;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Kafka producer configuration for {@link com.streamflow.processor.alert.AlertPublisher}.
 *
 * <p>SPEC-11 R4: a dedicated {@link ProducerFactory} for {@link AlertEventDTO} payloads is used to
 * keep alert publishing separate from the DLT ({@link DltKafkaConfig}) and snapshot ({@link
 * SnapshotKafkaConfig}) templates. This avoids generic-type ambiguity and keeps each producer's
 * purpose explicit.
 *
 * <p>Type-info headers are suppressed ({@code ADD_TYPE_INFO_HEADERS=false}) to keep the wire format
 * clean and compatible with the API gateway deserialiser.
 */
@Configuration
public class AlertKafkaConfig {

  @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
  private String bootstrapServers;

  /**
   * Producer factory for {@link AlertEventDTO} payloads destined for the {@code alerts} Kafka
   * topic.
   */
  @Bean
  public ProducerFactory<String, AlertEventDTO> alertProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    // At-least-once with broker-level dedup
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new DefaultKafkaProducerFactory<>(props);
  }

  /**
   * {@link KafkaTemplate} injected into {@link com.streamflow.processor.alert.AlertPublisher}.
   *
   * <p>Named {@code alertKafkaTemplate} to avoid ambiguity with the {@code kafkaTemplate} (DLT) and
   * {@code snapshotKafkaTemplate} beans.
   */
  @Bean
  public KafkaTemplate<String, AlertEventDTO> alertKafkaTemplate() {
    return new KafkaTemplate<>(alertProducerFactory());
  }
}
