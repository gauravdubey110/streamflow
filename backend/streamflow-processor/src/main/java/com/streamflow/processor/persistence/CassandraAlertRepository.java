package com.streamflow.processor.persistence;

import com.streamflow.common.dto.AlertEventDTO;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

/**
 * Write repository for the {@code streamflow.alerts} Cassandra table.
 *
 * <p>SPEC-17 R4: persists {@link AlertEventDTO} records asynchronously with bounded backpressure.
 * The repository:
 *
 * <ul>
 *   <li>Derives the {@code date_bucket} (daily, format {@code yyyy-MM-dd}) from the alert's {@code
 *       timestamp} field.
 *   <li>Fires a non-blocking insert via a dedicated executor thread pool.
 *   <li>Uses a {@link Semaphore} to cap concurrent in-flight writes (SPEC-17 R6).
 * </ul>
 */
@Slf4j
@Repository
@ConditionalOnBean(CassandraOperations.class)
public class CassandraAlertRepository {

  /** Daily bucket date format: {@code yyyy-MM-dd}. */
  private static final DateTimeFormatter DAY_BUCKET_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

  /** Maximum concurrent in-flight Cassandra writes (SPEC-17 R6). */
  static final int MAX_IN_FLIGHT = 32;

  private final CassandraOperations cassandraOperations;
  private final Semaphore semaphore;
  private final Executor writeExecutor;

  public CassandraAlertRepository(CassandraOperations cassandraOperations) {
    this.cassandraOperations = cassandraOperations;
    this.semaphore = new Semaphore(MAX_IN_FLIGHT, true);
    this.writeExecutor =
        Executors.newFixedThreadPool(
            2,
            r -> {
              Thread t = new Thread(r, "cassandra-alert-writer");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Persists an alert event to Cassandra asynchronously.
   *
   * <p>SPEC-17 R4: derives {@code date_bucket} as a daily bucket from the alert's {@code timestamp}
   * (epoch-millis).
   *
   * @param alert the alert event to persist
   */
  public void persist(AlertEventDTO alert) {
    boolean acquired;
    try {
      acquired = semaphore.tryAcquire(50L, java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(
          "SPEC-17: interrupted waiting for alert Cassandra write permit stream={}",
          alert.streamId());
      return;
    }

    if (!acquired) {
      log.warn(
          "SPEC-17: alert Cassandra write backpressure exceeded — dropping alert alertId={}",
          alert.alertId());
      return;
    }

    CompletableFuture.runAsync(
        () -> {
          try {
            Instant ts = Instant.ofEpochMilli(alert.timestamp());
            String dateBucket = DAY_BUCKET_FMT.format(ts);

            AlertEntity entity =
                AlertEntity.builder()
                    .streamId(alert.streamId())
                    .dateBucket(dateBucket)
                    .timestamp(ts)
                    .alertId(UUID.fromString(alert.alertId()))
                    .severity(alert.severity() != null ? alert.severity().name() : null)
                    .alertType(alert.alertType() != null ? alert.alertType().name() : null)
                    .message(alert.message())
                    .actualValue(alert.actualValue())
                    .resolvedAt(null)
                    .build();

            cassandraOperations.insert(entity);
            log.debug(
                "SPEC-17: alert persisted: alertId={} stream={} type={}",
                alert.alertId(),
                alert.streamId(),
                alert.alertType());
          } catch (Exception e) {
            log.error(
                "SPEC-17: failed to persist alert alertId={} stream={}: {}",
                alert.alertId(),
                alert.streamId(),
                e.getMessage(),
                e);
          } finally {
            semaphore.release();
          }
        },
        writeExecutor);
  }

  /**
   * Returns the formatted daily bucket for the given epoch-millis timestamp. Exposed
   * package-private for unit testing.
   */
  static String dayBucket(long epochMillis) {
    return DAY_BUCKET_FMT.format(Instant.ofEpochMilli(epochMillis));
  }
}
