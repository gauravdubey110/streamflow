package com.streamflow.api.config;

import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the API gateway.
 *
 * <p>SPEC-06 R2: Consumes {@link StreamMetricSnapshotDTO} objects from the
 * {@code metrics-aggregated} topic under consumer group {@code api-gateway-group}.
 *
 * <p>Concurrency is set to 1 — the topic has 3 partitions, but for MVP
 * a single consumer thread is sufficient. Increase via config if needed.
 *
 * <p>Auto-commit is enabled (the default) since message loss on restart is
 * acceptable for WebSocket push (the next snapshot arrives within 1 second).
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:api-gateway-group}")
    private String groupId;

    /**
     * Consumer factory for {@link StreamMetricSnapshotDTO} payloads.
     */
    @Bean
    public ConsumerFactory<String, StreamMetricSnapshotDTO> snapshotConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.streamflow.common.dto");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, StreamMetricSnapshotDTO.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(StreamMetricSnapshotDTO.class, false));
    }

    /**
     * Container factory for the {@code metrics-aggregated} listener.
     *
     * <p>Concurrency = 1 (single thread) — the API gateway pushes via
     * {@link org.springframework.messaging.simp.SimpMessagingTemplate}
     * which is thread-safe, so concurrency can be increased if throughput
     * requires it in later specs.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StreamMetricSnapshotDTO>
    snapshotListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, StreamMetricSnapshotDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(snapshotConsumerFactory());
        factory.setConcurrency(1);
        return factory;
    }
}
