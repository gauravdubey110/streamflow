package com.streamflow.producer;

import com.streamflow.producer.config.SimulationConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the StreamFlow Producer service.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>SPEC-03: Publish viewer events to {@code viewer-events} Kafka topic at configurable TPS
 *   <li>SPEC-09: Publish stream-health events to {@code stream-health} topic
 *   <li>SPEC-13: Expose REST endpoint for chaos injection
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SimulationConfig.class)
public class StreamProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamProducerApplication.class, args);
    }
}
