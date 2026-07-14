package com.streamflow.processor.persistence;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * Cassandra entity for the {@code streamflow.viewer_events} table.
 *
 * <p>SPEC-17 R4: matches the DDL in {@code infra/cassandra/init.cql} exactly.
 *
 * <p>Partition key: {@code (stream_id, date_bucket)} — hourly buckets keep individual partitions
 * bounded (Plan §8). Clustering columns: {@code timestamp DESC, event_id ASC} for latest-first
 * reads.
 *
 * <p>TTL is set at the {@code CREATE TABLE} level ({@code default_time_to_live = 604800}) so no
 * per-row TTL statement is needed.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("viewer_events")
public class ViewerEventEntity {

  /** Partition column 1: the stream this event belongs to. */
  @PrimaryKeyColumn(name = "stream_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  private String streamId;

  /**
   * Partition column 2: hourly bucket derived from {@code timestamp}. Format: {@code yyyy-MM-dd-HH}
   * (e.g. {@code "2024-06-03-14"}).
   */
  @PrimaryKeyColumn(name = "date_bucket", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  private String dateBucket;

  /** Clustering column 1: event time. Descending order for latest-first reads. */
  @PrimaryKeyColumn(
      name = "timestamp",
      type = PrimaryKeyType.CLUSTERED,
      ordinal = 0,
      ordering = Ordering.DESCENDING)
  private Instant timestamp;

  /** Clustering column 2: UUID for uniqueness within the same millisecond. */
  @PrimaryKeyColumn(name = "event_id", type = PrimaryKeyType.CLUSTERED, ordinal = 1)
  private UUID eventId;

  @Column("viewer_id")
  private String viewerId;

  @Column("event_type")
  private String eventType;

  @Column("quality")
  private String quality;

  @Column("buffer_ms")
  private Integer bufferMs;

  @Column("region")
  private String region;
}
