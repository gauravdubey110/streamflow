package com.streamflow.processor.persistence;

import java.time.Instant;
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
 * Cassandra entity for the {@code streamflow.metric_snapshots} table.
 *
 * <p>SPEC-17 R4: matches the DDL in {@code infra/cassandra/init.cql} exactly.
 *
 * <p>Partition key: {@code stream_id} (one partition per stream). Clustering column: {@code
 * minute_bucket DESC} for latest-first reads.
 *
 * <p>The {@code minute_bucket} is the {@code timestamp} value truncated to the start of the current
 * minute. This ensures exactly one row per stream per minute.
 *
 * <p>TTL is set at the {@code CREATE TABLE} level ({@code default_time_to_live = 2592000}).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("metric_snapshots")
public class MetricSnapshotEntity {

  /** Partition column: the stream this snapshot belongs to. */
  @PrimaryKeyColumn(name = "stream_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  private String streamId;

  /**
   * Clustering column: minute-truncated timestamp for this snapshot row. Descending order for
   * latest-first reads.
   */
  @PrimaryKeyColumn(
      name = "minute_bucket",
      type = PrimaryKeyType.CLUSTERED,
      ordinal = 0,
      ordering = Ordering.DESCENDING)
  private Instant minuteBucket;

  @Column("live_viewer_count")
  private Long liveViewerCount;

  @Column("buffer_rate_pct")
  private Double bufferRatePct;

  @Column("p95_latency_ms")
  private Integer p95LatencyMs;

  @Column("health_score")
  private Double healthScore;

  @Column("quality_1080p_pct")
  private Double quality1080pPct;

  @Column("quality_720p_pct")
  private Double quality720pPct;

  @Column("quality_480p_pct")
  private Double quality480pPct;

  @Column("quality_360p_pct")
  private Double quality360pPct;
}
