package com.streamflow.api.repository;

import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Read-only repository for the {@code streamflow.metric_snapshots} Cassandra table.
 *
 * <p>SPEC-18 R4/R6: fetches metric snapshots for a given stream within a time range. The {@code
 * metric_snapshots} table partitions by {@code stream_id} only, so a single partition scan with a
 * clustering-key range filter covers the full request efficiently.
 *
 * <p>Uses {@link CqlOperations#query(String, RowMapper, Object...)} for typed result mapping. The
 * {@link ConditionalOnBean} guard ensures this bean is skipped when Cassandra auto-configuration is
 * excluded in tests that do not need Cassandra.
 */
@Slf4j
@Repository
@ConditionalOnBean(CassandraOperations.class)
public class MetricSnapshotReadRepository {

  private final CqlOperations cqlOps;

  public MetricSnapshotReadRepository(CassandraOperations cassandraOperations) {
    this.cqlOps = ((CassandraTemplate) cassandraOperations).getCqlOperations();
  }

  /**
   * Fetches metric snapshot rows for a stream within the given time window.
   *
   * <p>The query uses a clustering-key range predicate ({@code minute_bucket >= ? AND minute_bucket
   * <= ?}) against the single partition for the stream. Results are returned in Cassandra's native
   * {@code DESC} order (latest first). The caller (HistoryService) reverses them to ascending.
   *
   * @param streamId the stream to query
   * @param from start of range (inclusive), epoch-milliseconds
   * @param to end of range (inclusive), epoch-milliseconds
   * @return list of snapshot rows, ordered DESC by minute_bucket (latest first)
   */
  public List<MetricSnapshotEntity> findByStreamAndRange(String streamId, long from, long to) {
    Instant fromInstant = Instant.ofEpochMilli(from);
    Instant toInstant = Instant.ofEpochMilli(to);

    String cql =
        "SELECT stream_id, minute_bucket, live_viewer_count, buffer_rate_pct, "
            + "p95_latency_ms, health_score, quality_1080p_pct, quality_720p_pct, "
            + "quality_480p_pct, quality_360p_pct "
            + "FROM streamflow.metric_snapshots "
            + "WHERE stream_id = ? "
            + "AND minute_bucket >= ? AND minute_bucket <= ?";

    RowMapper<MetricSnapshotEntity> mapper =
        (row, rowNum) ->
            MetricSnapshotEntity.builder()
                .streamId(row.getString("stream_id"))
                .minuteBucket(row.getInstant("minute_bucket"))
                .liveViewerCount(
                    row.isNull("live_viewer_count") ? null : row.getLong("live_viewer_count"))
                .bufferRatePct(
                    row.isNull("buffer_rate_pct") ? null : row.getDouble("buffer_rate_pct"))
                .p95LatencyMs(row.isNull("p95_latency_ms") ? null : row.getInt("p95_latency_ms"))
                .healthScore(row.isNull("health_score") ? null : row.getDouble("health_score"))
                .quality1080pPct(
                    row.isNull("quality_1080p_pct") ? null : row.getDouble("quality_1080p_pct"))
                .quality720pPct(
                    row.isNull("quality_720p_pct") ? null : row.getDouble("quality_720p_pct"))
                .quality480pPct(
                    row.isNull("quality_480p_pct") ? null : row.getDouble("quality_480p_pct"))
                .quality360pPct(
                    row.isNull("quality_360p_pct") ? null : row.getDouble("quality_360p_pct"))
                .build();

    try {
      List<MetricSnapshotEntity> results =
          cqlOps.query(cql, mapper, streamId, fromInstant, toInstant);
      log.debug(
          "SPEC-18: metric snapshots fetched: stream={} from={} to={} count={}",
          streamId,
          fromInstant,
          toInstant,
          results.size());
      return results;
    } catch (Exception e) {
      log.error(
          "SPEC-18: failed to query metric_snapshots for stream={}: {}",
          streamId,
          e.getMessage(),
          e);
      return List.of();
    }
  }
}
