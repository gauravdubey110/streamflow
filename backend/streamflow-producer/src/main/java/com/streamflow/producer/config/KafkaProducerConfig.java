package com.streamflow.producer.config;

import com.streamflow.common.dto.StreamHealthEventDTO;
import com.streamflow.common.dto.ViewerEventDTO;
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
 * Kafka producer factory and template configuration for the StreamFlow Producer service.
 *
 * <p>SPEC-03 R5 producer settings (viewer events):
 * <ul>
 *   <li>{@code acks=1} — leader acknowledgement only (throughput over durability for simulation)
 *   <li>{@code linger.ms=10} — allows micro-batching at high TPS
 *   <li>{@code batch.size=32768} — 32 KiB batches
 *   <li>{@code compression.type=lz4} — reduces network overhead
 *   <li>Key: {@link StringSerializer} (streamId)
 *   <li>Value: {@link JsonSerializer} (ViewerEventDTO, no type headers)
 * </ul>
 *
 * <p>SPEC-09: adds a dedicated producer for {@link StreamHealthEventDTO} messages
 * sent to the {@code stream-health} topic every 2 seconds. Lower throughput than
 * viewer events so no batching tuning is applied.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    /**
     * Producer factory with explicit tuning for high-throughput simulation.
     * The {@code spring.json.add.type.headers=false} setting ensures
     * consumers without this library on the classpath can still deserialise
     * messages using only the schema defined in {@code streamflow-common}.
     */
    @Bean
    public ProducerFactory<String, ViewerEventDTO> viewerEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Throughput tuning (SPEC-03 R5)
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        // No Spring type headers — consumers rely on streamflow-common schema only
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * {@link KafkaTemplate} used by {@link com.streamflow.producer.simulator.ViewerEventProducer}
     * to send viewer events keyed by {@code streamId}.
     */
    @Bean
    public KafkaTemplate<String, ViewerEventDTO> viewerEventKafkaTemplate() {
        return new KafkaTemplate<>(viewerEventProducerFactory());
    }

    // ── SPEC-09: stream-health producer ───────────────────────────────────────

    /**
     * Producer factory for {@link StreamHealthEventDTO} messages.
     *
     * <p>Uses the same bootstrap servers and serialization settings as the viewer-event
     * factory but omits high-throughput tuning (linger, batch size, compression) because
     * health events are emitted at only 1 msg/stream/2 s.
     */
    @Bean
    public ProducerFactory<String, StreamHealthEventDTO> healthEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // No type headers — consumers rely on streamflow-common schema only
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * {@link KafkaTemplate} injected into
     * {@link com.streamflow.producer.simulator.StreamHealthProducer}.
     *
     * <p>Named {@code healthEventKafkaTemplate} to avoid ambiguity with
     * {@link #viewerEventKafkaTemplate()}.
     */
    @Bean
    public KafkaTemplate<String, StreamHealthEventDTO> healthEventKafkaTemplate() {
        return new KafkaTemplate<>(healthEventProducerFactory());
    }
}
