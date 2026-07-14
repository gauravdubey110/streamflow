package com.streamflow.processor.config;

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
 * Minimal Kafka producer configuration used exclusively by the {@link
 * org.springframework.kafka.listener.DeadLetterPublishingRecoverer} in {@link KafkaConsumerConfig}.
 *
 * <p>SPEC-04 R6: failed records (after 3 retries) are published to the {@code viewer-events.DLT}
 * topic. The DLT template must handle {@code Object} values because the recoverer may publish
 * records of various types (raw bytes or serialisable objects).
 *
 * <p>This is the only producer in the processor service; it is kept minimal (no batching tuning)
 * since DLT traffic is tiny.
 */
@Configuration
public class DltKafkaConfig {

  @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
  private String bootstrapServers;

  @Bean
  public ProducerFactory<String, Object> dltProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    return new DefaultKafkaProducerFactory<>(props);
  }

  /**
   * {@link KafkaTemplate} injected into {@link
   * KafkaConsumerConfig#viewerEventListenerContainerFactory(KafkaTemplate)}.
   */
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate() {
    return new KafkaTemplate<>(dltProducerFactory());
  }
}
