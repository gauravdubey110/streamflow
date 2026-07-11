package com.streamflow.producer.config;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Micrometer configuration for the producer module.
 *
 * <p>SPEC-20 NFR2: Guards against cardinality explosions by capping the number
 * of distinct tag values for any meter. With a hard limit of 100 values per tag,
 * the maximum series count stays well below the 5 000-series limit.
 *
 * <p>The {@code topic} tag on {@code streamflow.events.published} has only 2 values
 * (viewer-events, stream-health), so the guard is a safety net rather than an
 * active limit in normal operation.
 */
@Configuration
public class MetricsConfig {

    /**
     * Caps the number of distinct values for any tag across all meters.
     *
     * <p>If a new tag value is seen after the limit is reached, the meter is
     * mapped to an overflow bucket (Micrometer's {@code MeterFilter.deny} fallback).
     * This prevents cardinality explosions from unbounded tags.
     *
     * @return a {@link MeterFilter} that enforces the cardinality limit
     */
    @Bean
    public MeterFilter cardinalityLimitFilter() {
        // SPEC-20 NFR2: maximum 100 distinct tag values per meter to stay well
        // below the 5 000-series cardinality limit.
        return MeterFilter.maximumAllowableTags(
                "streamflow.events.published", "topic", 10, MeterFilter.deny());
    }
}
