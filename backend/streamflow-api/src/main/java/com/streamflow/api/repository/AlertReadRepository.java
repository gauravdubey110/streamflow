package com.streamflow.api.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Read-only repository for the {@code streamflow.alerts} Cassandra table.
 *
 * <p>SPEC-18 R2/R6: fetches alerts for a given stream and time range, spanning one or more
 * {@code (stream_id, date_bucket)} partitions as needed.
 *
 * <p>Because the alerts table partitions by {@code (stream_id, date_bucket)}, the repository
 * must issue one query per daily bucket that intersects the requested range.
 * Results are merged in-memory in the service layer.
 *
 * <p>Severity filtering is applied post-fetch in the service (SPEC-18 Design Notes §4).
 */
@Slf4j
@Repository
@ConditionalOnBean(CassandraOperations.class)
public class AlertReadRepository {

    private final CqlOperations cqlOps;

    public AlertReadRepository(CassandraOperations cassandraOperations) {
        this.cqlOps = ((CassandraTemplate) cassandraOperations).getCqlOperations();
    }

    /**
     * Fetches alert rows for a stream within the given time range.
     *
     * <p>Issues one CQL query per daily bucket in {@code dayBuckets} (at most 2 for a 24h range).
     * The clustering range predicate ({@code timestamp >= ? AND timestamp <= ?}) narrows each
     * bucket to only the rows within the requested window.
     *
     * @param streamId   the stream to query
     * @param from       start of range (inclusive), epoch-milliseconds
     * @param to         end of range (inclusive), epoch-milliseconds
     * @param dayBuckets list of {@code yyyy-MM-dd} bucket strings covering the range
     * @return merged list of alert rows across all queried partitions
     */
    public List<AlertEntity> findByStreamAndRange(String streamId, long from, long to,
                                                   List<String> dayBuckets) {
        Instant fromInstant = Instant.ofEpochMilli(from);
        Instant toInstant   = Instant.ofEpochMilli(to);

        String cql = "SELECT stream_id, date_bucket, timestamp, alert_id, severity, " +
                     "alert_type, message, actual_value, resolved_at " +
                     "FROM streamflow.alerts " +
                     "WHERE stream_id = ? AND date_bucket = ? " +
                     "AND timestamp >= ? AND timestamp <= ?";

        RowMapper<AlertEntity> mapper = (row, rowNum) ->
                AlertEntity.builder()
                        .streamId(row.getString("stream_id"))
                        .dateBucket(row.getString("date_bucket"))
                        .timestamp(row.getInstant("timestamp"))
                        .alertId(row.getUuid("alert_id"))
                        .severity(row.getString("severity"))
                        .alertType(row.getString("alert_type"))
                        .message(row.getString("message"))
                        .actualValue(row.isNull("actual_value") ? null : row.getDouble("actual_value"))
                        .resolvedAt(row.getInstant("resolved_at"))
                        .build();

        List<AlertEntity> results = new ArrayList<>();
        for (String bucket : dayBuckets) {
            try {
                List<AlertEntity> rows = cqlOps.query(cql, mapper,
                        streamId, bucket, fromInstant, toInstant);
                results.addAll(rows);
                log.debug("SPEC-18: alerts fetched: stream={} bucket={} count={}",
                        streamId, bucket, rows.size());
            } catch (Exception e) {
                log.error("SPEC-18: failed to query alerts for stream={} bucket={}: {}",
                        streamId, bucket, e.getMessage(), e);
            }
        }
        return results;
    }
}
