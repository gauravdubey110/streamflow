package com.streamflow.api.repository;

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
 * Read-side Cassandra entity for the {@code streamflow.metric_snapshots} table.
 *
 * <p>SPEC-18 R6: mirrors the write-side entity in {@code streamflow-processor} but lives in the api
 * module so the api gateway does not depend on the processor module's internal classes.
 *
 * <p>Primary key: {@code (stream_id)} partitioned; {@code minute_bucket DESC} clustered.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("metric_snapshots")
public class MetricSnapshotEntity {

  @PrimaryKeyColumn(name = "stream_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  private String streamId;

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
