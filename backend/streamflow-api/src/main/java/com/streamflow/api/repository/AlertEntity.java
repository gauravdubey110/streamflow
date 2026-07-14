package com.streamflow.api.repository;

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
 * Read-side Cassandra entity for the {@code streamflow.alerts} table.
 *
 * <p>SPEC-18 R6: mirrors the write-side entity in {@code streamflow-processor} but lives in the api
 * module so the api gateway does not depend on the processor module's internal classes.
 *
 * <p>Primary key: {@code (stream_id, date_bucket)} partitioned; {@code timestamp DESC, alert_id
 * ASC} clustered.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("alerts")
public class AlertEntity {

  @PrimaryKeyColumn(name = "stream_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  private String streamId;

  @PrimaryKeyColumn(name = "date_bucket", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  private String dateBucket;

  @PrimaryKeyColumn(
      name = "timestamp",
      type = PrimaryKeyType.CLUSTERED,
      ordinal = 0,
      ordering = Ordering.DESCENDING)
  private Instant timestamp;

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

  @Column("resolved_at")
  private Instant resolvedAt;
}
