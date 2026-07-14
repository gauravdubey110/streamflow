package com.streamflow.api.config;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Micrometer configuration for the API gateway module.
 *
 * <p>SPEC-20 NFR2: Guards against cardinality explosions. The API's {@code
 * streamflow.events.consumed} counter has only 2 topic values (metrics-aggregated, alerts), so the
 * guard is a safety net.
 */
@Configuration
public class MetricsConfig {

  /**
   * Caps the number of distinct values for the {@code topic} tag on the {@code
   * streamflow.events.consumed} counter in the API module.
   *
   * @return a {@link MeterFilter} enforcing the tag cardinality limit
   */
  @Bean
  public MeterFilter consumedTopicCardinalityFilter() {
    // SPEC-20 NFR2: cap topic values at 10 (2 in practice)
    return MeterFilter.maximumAllowableTags(
        "streamflow.events.consumed", "topic", 10, MeterFilter.deny());
  }
}
