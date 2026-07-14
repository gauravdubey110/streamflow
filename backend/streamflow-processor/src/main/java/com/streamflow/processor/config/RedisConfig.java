package com.streamflow.processor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis configuration for the StreamFlow Processor.
 *
 * <p>SPEC-04 §4: Uses {@code RedisTemplate<String, String>} with {@link StringRedisSerializer} for
 * both key and value so that sorted-set members (viewerIds) and scores remain human-readable in
 * {@code redis-cli}.
 */
@Configuration
public class RedisConfig {

  /**
   * Primary template used by {@link com.streamflow.processor.aggregator.ViewerCountAggregator}.
   *
   * <p>Both key and value are serialised as plain UTF-8 strings — no Java serialisation, no JSON
   * overhead — matching the design note in SPEC-04 §4.
   *
   * <p>{@code @Primary} is required because Spring Boot's Redis auto-configuration also registers a
   * {@code StringRedisTemplate} bean of the same generic type. Without {@code @Primary}, injection
   * of {@code RedisTemplate<String, String>} into {@link
   * com.streamflow.processor.aggregator.ViewerCountAggregator} would be ambiguous.
   */
  @Bean
  @Primary
  public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
    RedisTemplate<String, String> template = new RedisTemplate<>();
    template.setConnectionFactory(connectionFactory);

    StringRedisSerializer serializer = new StringRedisSerializer();
    template.setKeySerializer(serializer);
    template.setValueSerializer(serializer);
    template.setHashKeySerializer(serializer);
    template.setHashValueSerializer(serializer);
    template.setDefaultSerializer(serializer);

    template.afterPropertiesSet();
    return template;
  }
}
