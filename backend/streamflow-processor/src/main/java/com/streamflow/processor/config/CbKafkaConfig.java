package com.streamflow.processor.config;

import com.streamflow.common.dto.CbStateEventDTO;
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
 * Kafka producer configuration for circuit-breaker state events (SPEC-14 R2).
 *
 * <p>A dedicated {@link ProducerFactory} is used for {@link CbStateEventDTO} payloads to keep this
 * topic's producer separate from the alert ({@link AlertKafkaConfig}) and snapshot ({@link
 * SnapshotKafkaConfig}) producers and to avoid generic-type ambiguity on the shared {@link
 * KafkaTemplate}.
 *
 * <p>Type-info headers are suppressed ({@code ADD_TYPE_INFO_HEADERS=false}) so the wire format is
 * clean JSON that the API gateway deserialiser can handle without class-name coupling.
 */
@Configuration
public class CbKafkaConfig {

  @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
  private String bootstrapServers;

  /**
   * Producer factory for {@link CbStateEventDTO} payloads destined for the {@code cb-events} Kafka
   * topic.
   */
  @Bean
  public ProducerFactory<String, CbStateEventDTO> cbProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    // At-least-once with broker-level dedup (SPEC-14 NFR1: low latency, not zero loss)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new DefaultKafkaProducerFactory<>(props);
  }

  /**
   * {@link KafkaTemplate} injected into {@link
   * com.streamflow.processor.circuitbreaker.CircuitBreakerStatePublisher}.
   *
   * <p>Named {@code cbKafkaTemplate} to avoid ambiguity with the {@code alertKafkaTemplate} and
   * {@code snapshotKafkaTemplate} beans.
   */
  @Bean
  public KafkaTemplate<String, CbStateEventDTO> cbKafkaTemplate() {
    return new KafkaTemplate<>(cbProducerFactory());
  }
}
