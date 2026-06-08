package com.streamflow.processor.config;

import com.streamflow.common.dto.ViewerEventDTO;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the StreamFlow Processor.
 *
 * <p>SPEC-04 requirements implemented here:
 * <ul>
 *   <li>R1  – concurrency = 3 on the {@code viewer-events} listener container.</li>
 *   <li>R6  – manual-ack mode ({@link ContainerProperties.AckMode#MANUAL_IMMEDIATE});
 *             error handler retries with exponential back-off (3 attempts, 1s base),
 *             then dead-letters to {@code viewer-events.DLT} via
 *             {@link DeadLetterPublishingRecoverer}.</li>
 * </ul>
 *
 * <p>The deserialiser trusts {@code com.streamflow.common.dto} to avoid
 * {@code IllegalStateException} on type mismatch (per application.properties).
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:stream-processor-group}")
    private String groupId;

    /**
     * Consumer factory that produces {@link ViewerEventDTO} values from JSON.
     */
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

        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(ViewerEventDTO.class, false));
    }

    /**
     * Container factory for {@code viewer-events} listeners.
     *
     * <p>Key settings:
     * <ul>
     *   <li>Concurrency = 3 (SPEC-04 R1)</li>
     *   <li>Ack mode = MANUAL_IMMEDIATE (SPEC-04 R6)</li>
     *   <li>Error handler: 3 retries at 1 s intervals, then DLT (SPEC-04 R6)</li>
     * </ul>
     *
     * @param kafkaTemplate used by {@link DeadLetterPublishingRecoverer} to publish
     *                      failed records to {@code viewer-events.DLT}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ViewerEventDTO>
    viewerEventListenerContainerFactory(
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, ViewerEventDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(viewerEventConsumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // SPEC-04 R6: 3 retries × 1 s back-off, then dead-letter
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
