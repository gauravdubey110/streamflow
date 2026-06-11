package com.streamflow.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis configuration for the API gateway.
 *
 * <p>SPEC-06 R3/R4: Provides a {@link RedisTemplate}{@code <String, String>}
 * that reads snapshot JSON from {@code stream_snapshot:{streamId}} keys and
 * stream members from {@code active_streams} — both written by the processor.
 *
 * <p>Both key and value serialisers are {@link StringRedisSerializer} to match
 * the processor's write format (plain JSON string values, string keys).
 */
@Configuration
public class RedisConfig {

    /**
     * {@link RedisTemplate} with string key/value serialisers.
     *
     * <p>This matches the serialisation used in the processor module so that
     * JSON values written by {@code SnapshotPublisher} can be read here without
     * type-header issues.
     *
     * <p>{@code @Primary} is required because Spring Boot's
     * {@code RedisAutoConfiguration} also registers a {@code stringRedisTemplate}
     * bean with the same generic type. Without {@code @Primary}, autowiring
     * fails with a "no unique bean" error when there are 2 matching beans.
     */
    @Bean
    @Primary
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(stringSerializer);
        template.afterPropertiesSet();
        return template;
    }
}
