package com.streamflow.processor.persistence;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Cassandra entity for the {@code streamflow.alerts} table.
 *
 * <p>SPEC-17 R4: matches the DDL in {@code infra/cassandra/init.cql} exactly.
 *
 * <p>Partition key: {@code (stream_id, date_bucket)} — daily buckets keep
 * individual partitions bounded (Plan §8).
 * Clustering columns: {@code timestamp DESC, alert_id ASC} for latest-first reads.
 *
 * <p>TTL is set at the {@code CREATE TABLE} level ({@code default_time_to_live = 7776000}).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("alerts")
public class AlertEntity {

    /** Partition column 1: the stream this alert belongs to. */
    @PrimaryKeyColumn(name = "stream_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    private String streamId;

    /**
     * Partition column 2: daily bucket derived from {@code timestamp}.
     * Format: {@code yyyy-MM-dd} (e.g. {@code "2024-06-03"}).
     */
    @PrimaryKeyColumn(name = "date_bucket", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
    private String dateBucket;

    /** Clustering column 1: alert time. Descending order for latest-first reads. */
    @PrimaryKeyColumn(name = "timestamp", type = PrimaryKeyType.CLUSTERED, ordinal = 0,
            ordering = Ordering.DESCENDING)
    private Instant timestamp;

    /** Clustering column 2: UUID for uniqueness within the same millisecond. */
    @PrimaryKeyColumn(name = "alert_id", type = PrimaryKeyType.CLUSTERED, ordinal = 1)
    private UUID alertId;

    @Column("severity")
    private String severity;

    @Column("alert_type")
    private String alertType;

    @Column("message")
    private String message;

    @Column("actual_value")
    private Double actualValue;

    /** Nullable — populated when the alert is resolved (future spec). */
    @Column("resolved_at")
    private Instant resolvedAt;
}
