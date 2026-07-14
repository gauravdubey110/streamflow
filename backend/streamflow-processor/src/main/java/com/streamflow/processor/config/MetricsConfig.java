package com.streamflow.processor.config;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Micrometer configuration for the processor module.
 *
 * <p>SPEC-20 NFR2: Guards against cardinality explosions. In the processor, the {@code
 * streamflow.alerts.fired} counter has tags {@code severity} (3 values) and {@code alertType} (4
 * values) = max 12 series. The guard is a safety net.
 */
@Configuration
public class MetricsConfig {

  /**
   * Caps the number of distinct values for the {@code severity} tag on the {@code
   * streamflow.alerts.fired} counter.
   *
   * <p>With 3 severity values × 4 alert-type values = 12 series maximum, this filter will not
   * trigger under normal operation.
   *
   * @return a {@link MeterFilter} enforcing the tag cardinality limit
   */
  @Bean
  public MeterFilter alertsFiredCardinalityFilter() {
    // SPEC-20 NFR2: cap severity values at 10 (3 in practice)
    return MeterFilter.maximumAllowableTags(
        "streamflow.alerts.fired", "severity", 10, MeterFilter.deny());
  }
}
