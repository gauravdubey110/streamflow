package com.streamflow.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Jackson configuration for the API gateway.
 *
 * <p>Spring Boot's {@code JacksonAutoConfiguration} is active in the
 * {@code spring-boot-starter-web} context, so the default {@link ObjectMapper}
 * bean is already provided. This class is intentionally left as a marker
 * for future customisations (e.g. JavaTimeModule in SPEC-17) so that the bean
 * definition location is discoverable.
 *
 * <p>Note: We do NOT redeclare the bean here because Spring Boot autoconfigures
 * it. Any customisation should be done via {@code Jackson2ObjectMapperBuilderCustomizer}.
 * This file is kept as documentation and extension point only.
 */
@Configuration
public class JacksonConfig {
    // ObjectMapper is provided by JacksonAutoConfiguration via spring-boot-starter-web.
    // Customise via Jackson2ObjectMapperBuilderCustomizer when needed.
}
