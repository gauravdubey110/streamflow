package com.streamflow.producer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Simulation configuration bound from {@code application.yml} under the
 * {@code streamflow.simulation} prefix.
 *
 * <p>SPEC-03 R2: reads tps, enabled, and per-stream definitions.
 * Registered via {@code @EnableConfigurationProperties(SimulationConfig.class)} on the
 * main application class — do NOT also annotate with {@code @Configuration} as that would
 * create a duplicate bean registration.
 */
@ConfigurationProperties(prefix = "streamflow.simulation")
@Data
public class SimulationConfig {

    /**
     * Target aggregate events-per-second across all streams.
     * Default: 1000.
     */
    private int tps = 1000;

    /**
     * When {@code false}, the application starts but emits zero messages.
     * Default: true.
     */
    private boolean enabled = true;

    /**
     * Per-stream simulation definitions. Each entry maps to a logical stream.
     */
    private List<StreamDefinition> streams = List.of();

    /**
     * Immutable definition of one simulated stream.
     *
     * <p>Lombok {@code @Data} on the outer class does not handle nested classes
     * automatically — we use a separate mutable inner class so Spring Boot can
     * bind list elements via setter injection.
     */
    @Data
    public static class StreamDefinition {

        /** Kafka message key and stream identifier (e.g. {@code stream-001}). */
        private String id;

        /** Human-readable stream name (e.g. {@code "Tech Talk Live"}). */
        private String name;

        /**
         * Approximate number of simulated active viewers.
         * Used by later specs for realistic viewer-count seeding.
         */
        private long baseViewers = 50_000;
    }
}
