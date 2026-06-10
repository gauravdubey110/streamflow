package com.streamflow.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for the snapshot publisher.
 *
 * <p>SPEC-05 R4: publishes {@link StreamMetricSnapshotDTO} objects to the
 * {@code metrics-aggregated} topic. A dedicated {@link ProducerFactory} is
 * used (rather than reusing the DLT factory from {@link DltKafkaConfig})
 * so that the generic type parameter is exact and avoids unchecked-cast warnings.
 *
 * <p>Type-info headers are suppressed ({@code ADD_TYPE_INFO_HEADERS=false}) to
 * keep the wire format clean and compatible with the API gateway deserialiser.
 *
 * <p>An {@link ObjectMapper} bean is declared here because the processor module
 * uses {@code spring-boot-starter} (not {@code spring-boot-starter-web}), which
 * does not activate {@code JacksonAutoConfiguration} by default. This single
 * instance is shared by {@link com.streamflow.processor.snapshot.SnapshotPublisher}.
 */
@Configuration
public class SnapshotKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    /**
     * Jackson {@link ObjectMapper} with default configuration.
     *
     * <p>Placed in this config class (not a separate one) to keep SPEC-05 additions
     * co-located. Later specs may customise this bean (e.g. JavaTimeModule in SPEC-17).
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    /**
     * Producer factory for {@link StreamMetricSnapshotDTO} payloads.
     */
    @Bean
    public ProducerFactory<String, StreamMetricSnapshotDTO> snapshotProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        // Idempotent producer: at-least-once with dedup at the broker level
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * {@link KafkaTemplate} injected into
     * {@link com.streamflow.processor.snapshot.SnapshotPublisher}.
     *
     * <p>Named {@code snapshotKafkaTemplate} to avoid ambiguity with the
     * {@code kafkaTemplate} bean in {@link DltKafkaConfig}.
     */
    @Bean
    public KafkaTemplate<String, StreamMetricSnapshotDTO> snapshotKafkaTemplate() {
        return new KafkaTemplate<>(snapshotProducerFactory());
    }
}
