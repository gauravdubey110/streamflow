package com.streamflow.api.config;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.CbStateEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Kafka consumer configuration for the API gateway.
 *
 * <p>Three consumer factories are declared — one per consumed topic type:
 *
 * <ul>
 *   <li>SPEC-06 R2: {@link StreamMetricSnapshotDTO} from {@code metrics-aggregated} → {@code
 *       snapshotListenerContainerFactory}
 *   <li>SPEC-14 R1: {@link AlertEventDTO} from {@code alerts} → {@code
 *       alertListenerContainerFactory}
 *   <li>SPEC-14 R2: {@link CbStateEventDTO} from {@code cb-events} → {@code
 *       cbListenerContainerFactory}
 * </ul>
 *
 * <p>Concurrency is set to 1 for each factory — topics have at most 6 partitions but a single
 * consumer thread is sufficient for WebSocket push at current scale.
 *
 * <p>Auto-commit is enabled (default) since message loss on restart is acceptable for live-push
 * paths: the next snapshot arrives within 1 s; the next alert fires only if the condition persists
 * past the cooldown window.
 */
@Configuration
public class KafkaConsumerConfig {

  @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id:api-gateway-group}")
  private String groupId;

  // ── Snapshot (SPEC-06) ────────────────────────────────────────────────────

  /**
   * Consumer factory for {@link StreamMetricSnapshotDTO} payloads from the {@code
   * metrics-aggregated} topic.
   */
  @Bean
  public ConsumerFactory<String, StreamMetricSnapshotDTO> snapshotConsumerFactory() {
    Map<String, Object> props = baseConsumerProps();
    return new DefaultKafkaConsumerFactory<>(
        props,
        new StringDeserializer(),
        new JsonDeserializer<>(StreamMetricSnapshotDTO.class, false));
  }

  /** Container factory for the {@code metrics-aggregated} listener (SPEC-06 R2). */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, StreamMetricSnapshotDTO>
      snapshotListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, StreamMetricSnapshotDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(snapshotConsumerFactory());
    factory.setConcurrency(1);
    return factory;
  }

  // ── Alert (SPEC-14 R1) ────────────────────────────────────────────────────

  /** Consumer factory for {@link AlertEventDTO} payloads from the {@code alerts} topic. */
  @Bean
  public ConsumerFactory<String, AlertEventDTO> alertConsumerFactory() {
    Map<String, Object> props = baseConsumerProps();
    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new JsonDeserializer<>(AlertEventDTO.class, false));
  }

  /**
   * Container factory for the {@code alerts} listener ({@link
   * com.streamflow.api.websocket.AlertPushConsumer} — SPEC-14 R1).
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, AlertEventDTO>
      alertListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, AlertEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(alertConsumerFactory());
    factory.setConcurrency(1);
    return factory;
  }

  // ── Circuit-breaker events (SPEC-14 R2) ──────────────────────────────────

  /** Consumer factory for {@link CbStateEventDTO} payloads from the {@code cb-events} topic. */
  @Bean
  public ConsumerFactory<String, CbStateEventDTO> cbConsumerFactory() {
    Map<String, Object> props = baseConsumerProps();
    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new JsonDeserializer<>(CbStateEventDTO.class, false));
  }

  /**
   * Container factory for the {@code cb-events} listener ({@link
   * com.streamflow.api.websocket.CircuitBreakerPushConsumer} — SPEC-14 R2).
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, CbStateEventDTO>
      cbListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, CbStateEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(cbConsumerFactory());
    factory.setConcurrency(1);
    return factory;
  }

  // ── Shared helper ─────────────────────────────────────────────────────────

  /**
   * Returns the common consumer properties shared by all three factories.
   *
   * <p>Type-info headers are ignored ({@code USE_TYPE_INFO_HEADERS=false}) because the processor
   * serializers suppress them ({@code ADD_TYPE_INFO_HEADERS=false}). Each factory's {@link
   * JsonDeserializer} is constructed with the exact target type, so no header-based type resolution
   * is needed.
   */
  private Map<String, Object> baseConsumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto");
    props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
    return props;
  }
}
