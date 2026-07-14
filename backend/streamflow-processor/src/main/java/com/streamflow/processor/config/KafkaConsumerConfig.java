package com.streamflow.processor.config;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamHealthEventDTO;
import com.streamflow.common.dto.ViewerEventDTO;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka consumer configuration for the StreamFlow Processor.
 *
 * <p>SPEC-04 requirements implemented here:
 *
 * <ul>
 *   <li>R1 – concurrency = 3 on the {@code viewer-events} listener container.
 *   <li>R6 – manual-ack mode ({@link ContainerProperties.AckMode#MANUAL_IMMEDIATE}); error handler
 *       retries with exponential back-off (3 attempts, 1s base), then dead-letters to {@code
 *       viewer-events.DLT} via {@link DeadLetterPublishingRecoverer}.
 * </ul>
 *
 * <p>SPEC-09 additions:
 *
 * <ul>
 *   <li>Adds {@link #streamHealthConsumerFactory()} and {@link
 *       #streamHealthListenerContainerFactory()} for the {@code stream-health} topic. Health events
 *       are idempotent (latest overwrites), so auto-ack (BATCH mode) is used.
 * </ul>
 *
 * <p>SPEC-17 additions:
 *
 * <ul>
 *   <li>Adds {@link #alertConsumerFactory()} and {@link #alertListenerContainerFactory()} for the
 *       {@code alerts} topic. The {@link com.streamflow.processor.consumer.AlertCassandraConsumer}
 *       uses group {@code alert-cassandra-group} to persist alerts to Cassandra independently of
 *       the API gateway consumer group.
 * </ul>
 *
 * <p>The deserialiser trusts {@code com.streamflow.common.dto} to avoid {@code
 * IllegalStateException} on type mismatch (per application.properties).
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

  @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id:stream-processor-group}")
  private String groupId;

  /** Consumer factory that produces {@link ViewerEventDTO} values from JSON. */
  @Bean
  public ConsumerFactory<String, ViewerEventDTO> viewerEventConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto");
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ViewerEventDTO.class.getName());
    props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new JsonDeserializer<>(ViewerEventDTO.class, false));
  }

  /**
   * Container factory for {@code viewer-events} listeners.
   *
   * <p>Key settings:
   *
   * <ul>
   *   <li>Concurrency = 3 (SPEC-04 R1)
   *   <li>Ack mode = MANUAL_IMMEDIATE (SPEC-04 R6)
   *   <li>Error handler: 3 retries at 1 s intervals, then DLT (SPEC-04 R6)
   * </ul>
   *
   * @param kafkaTemplate used by {@link DeadLetterPublishingRecoverer} to publish failed records to
   *     {@code viewer-events.DLT}.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, ViewerEventDTO>
      viewerEventListenerContainerFactory(KafkaTemplate<String, Object> kafkaTemplate) {

    ConcurrentKafkaListenerContainerFactory<String, ViewerEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(viewerEventConsumerFactory());
    factory.setConcurrency(3);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    // SPEC-04 R6: 3 retries × 1 s back-off, then dead-letter
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
    DefaultErrorHandler errorHandler =
        new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
    factory.setCommonErrorHandler(errorHandler);

    return factory;
  }

  // ── SPEC-09: stream-health consumer ───────────────────────────────────────

  /**
   * Consumer factory for {@link StreamHealthEventDTO} values from the {@code stream-health} topic.
   *
   * <p>Uses the same group-id and bootstrap servers as the viewer-event consumer so that both
   * listeners share the same consumer group ({@code stream-processor-group}).
   */
  @Bean
  public ConsumerFactory<String, StreamHealthEventDTO> streamHealthConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto");
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, StreamHealthEventDTO.class.getName());
    props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new JsonDeserializer<>(StreamHealthEventDTO.class, false));
  }

  /**
   * Container factory for the {@code stream-health} {@link KafkaListener}.
   *
   * <p>Health events are idempotent — the latest Redis Hash value always wins — so no manual ack or
   * DLT is required. Concurrency = 1 because there are only 3 partitions on {@code stream-health}
   * and 3 streams; a single thread is sufficient at 1 event/stream/2 s.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, StreamHealthEventDTO>
      streamHealthListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, StreamHealthEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(streamHealthConsumerFactory());
    factory.setConcurrency(1);
    // Auto-ack (BATCH) — acceptable for idempotent health events (SPEC-09 R2)
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

    return factory;
  }

  // ── SPEC-17: alerts consumer (Cassandra persistence) ──────────────────────

  /**
   * Consumer factory for {@link AlertEventDTO} values from the {@code alerts} topic.
   *
   * <p>Uses group {@code alert-cassandra-group} so this consumer is independent of the API
   * gateway's {@code api-gateway-group} consumer on the same topic.
   */
  @Bean
  public ConsumerFactory<String, AlertEventDTO> alertConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-cassandra-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto");
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AlertEventDTO.class.getName());
    props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new JsonDeserializer<>(AlertEventDTO.class, false));
  }

  /**
   * Container factory for the {@code alerts} listener in {@link
   * com.streamflow.processor.consumer.AlertCassandraConsumer}.
   *
   * <p>Concurrency = 1: alert throughput is low (a few per minute per stream). Manual ack ensures
   * Cassandra dispatch is recorded before offset is committed.
   *
   * @param kafkaTemplate used by the DLT recoverer for failed alert writes
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, AlertEventDTO>
      alertListenerContainerFactory(KafkaTemplate<String, Object> kafkaTemplate) {

    ConcurrentKafkaListenerContainerFactory<String, AlertEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(alertConsumerFactory());
    factory.setConcurrency(1);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
    DefaultErrorHandler errorHandler =
        new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
    factory.setCommonErrorHandler(errorHandler);

    return factory;
  }
}
