package com.streamflow.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration for the chaos HTTP client.
 *
 * <p>SPEC-13 R5: provides a named {@link RestTemplate} bean used exclusively by {@link
 * com.streamflow.api.chaos.ProducerChaosClient} to forward chaos commands to the producer's
 * internal REST API.
 *
 * <p>The producer base URL is configured via {@code streamflow.producer.base-url} (default: {@code
 * http://localhost:8081}).
 */
@Configuration
public class ChaosClientConfig {

  /**
   * A plain {@link RestTemplate} instance for producer-to-internal communication.
   *
   * <p>Named {@code chaosRestTemplate} to avoid ambiguity if other RestTemplate beans are added in
   * later specs.
   */
  @Bean("chaosRestTemplate")
  public RestTemplate chaosRestTemplate() {
    return new RestTemplate();
  }
}
