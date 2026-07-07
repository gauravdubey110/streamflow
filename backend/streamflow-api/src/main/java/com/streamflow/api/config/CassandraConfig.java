package com.streamflow.api.config;

import com.datastax.oss.driver.api.core.CqlSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraTemplate;

/**
 * Cassandra configuration for the API gateway module.
 *
 * <p>SPEC-18 R6: exposes a {@link CassandraTemplate} bean backed by the auto-configured
 * {@link CqlSession} for use by read-only repositories ({@code MetricSnapshotReadRepository}
 * and {@code AlertReadRepository}).
 *
 * <p>{@code schema-action=none} — schema is pre-created by {@code infra/cassandra/init.cql}.
 *
 * <p>The {@code @ConditionalOnBean(CqlSession.class)} guard ensures this bean (and all
 * repositories that depend on it) are silently skipped when Cassandra auto-configuration
 * is excluded in tests that do not need Cassandra.
 */
@Slf4j
@Configuration
public class CassandraConfig {

    /**
     * Exposes a {@link CassandraTemplate} backed by the auto-configured {@link CqlSession}.
     *
     * <p>Only created when {@code CassandraAutoConfiguration} is active and has produced a
     * {@link CqlSession} bean.
     *
     * @param cqlSession the auto-configured Cassandra session
     * @return configured {@link CassandraTemplate}
     */
    @Bean
    @ConditionalOnBean(CqlSession.class)
    public CassandraTemplate cassandraTemplate(CqlSession cqlSession) {
        log.info("SPEC-18: CassandraTemplate initialised for api-gateway (keyspace=streamflow)");
        return new CassandraTemplate(cqlSession);
    }
}
