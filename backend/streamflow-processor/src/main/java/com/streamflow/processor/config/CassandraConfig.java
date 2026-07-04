package com.streamflow.processor.config;

import com.datastax.oss.driver.api.core.CqlSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraTemplate;

/**
 * Cassandra configuration for the processor module.
 *
 * <p>SPEC-17 R3: Spring Data Cassandra is configured here with:
 * <ul>
 *   <li>Contact points, datacenter, and keyspace read from environment / properties.</li>
 *   <li>{@link CassandraTemplate} bean exposed for use by write repositories.</li>
 *   <li>Schema action = NONE — schema is pre-created by {@code infra/cassandra/init.cql}.</li>
 * </ul>
 *
 * <p>The {@link CqlSession} itself is auto-configured by Spring Boot's
 * {@code CassandraAutoConfiguration} using the {@code spring.cassandra.*} properties.
 * This class only adds the {@link CassandraTemplate} bean on top of the auto-configured session
 * so that repositories can perform typed CQL operations without raw session management.
 */
@Slf4j
@Configuration
public class CassandraConfig {

    /**
     * Exposes a {@link CassandraTemplate} backed by the auto-configured {@link CqlSession}.
     *
     * <p>The template is the primary persistence abstraction used by all three write
     * repositories ({@code CassandraViewerEventRepository}, {@code CassandraMetricSnapshotRepository},
     * {@code CassandraAlertRepository}) per SPEC-17 R4.
     *
     * <p>{@code @ConditionalOnBean(CqlSession.class)}: only created when Cassandra
     * auto-configuration is active. When auto-config is excluded in tests, this bean
     * is skipped and all {@code @ConditionalOnBean(CassandraOperations.class)} repositories
     * are also skipped, preventing startup failures in tests that don't need Cassandra.
     *
     * @param cqlSession the auto-configured Cassandra session
     * @return configured {@link CassandraTemplate}
     */
    @Bean
    @ConditionalOnBean(CqlSession.class)
    public CassandraTemplate cassandraTemplate(CqlSession cqlSession) {
        log.info("SPEC-17: CassandraTemplate initialised (session keyspace=streamflow)");
        return new CassandraTemplate(cqlSession);
    }
}
