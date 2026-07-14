package com.streamflow.processor.persistence;

import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

/**
 * Write repository for the {@code streamflow.metric_snapshots} Cassandra table.
 *
 * <p>SPEC-17 R4: persists one {@link StreamMetricSnapshotDTO} row per stream per minute. The
 * repository:
 *
 * <ul>
 *   <li>Truncates the snapshot's {@code snapshotTs} (epoch-millis) to the start of the current
 *       minute ({@code ChronoUnit.MINUTES}) — the {@code minute_bucket} column.
 *   <li>Tracks the last persisted minute per stream in a {@link ConcurrentHashMap} so that the
 *       caller (SnapshotPublisher, running every second) only triggers a write once per minute per
 *       stream (SPEC-17 R5).
 *   <li>Uses a {@link Semaphore} to cap concurrent in-flight writes (SPEC-17 R6).
 * </ul>
 */
@Slf4j
@Repository
@ConditionalOnBean(CassandraOperations.class)
public class CassandraMetricSnapshotRepository {

  /** Maximum concurrent in-flight Cassandra writes (SPEC-17 R6). */
  static final int MAX_IN_FLIGHT = 32;

  private final CassandraOperations cassandraOperations;
  private final Semaphore semaphore;
  private final Executor writeExecutor;

  /** Tracks the last-written minute bucket per stream to gate once-per-minute writes. */
  private final Map<String, Instant> lastWrittenMinute = new ConcurrentHashMap<>();

  public CassandraMetricSnapshotRepository(CassandraOperations cassandraOperations) {
    this.cassandraOperations = cassandraOperations;
    this.semaphore = new Semaphore(MAX_IN_FLIGHT, true);
    this.writeExecutor =
        Executors.newFixedThreadPool(
            2,
            r -> {
              Thread t = new Thread(r, "cassandra-snapshot-writer");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Persists a metric snapshot to Cassandra if the current minute has not already been written for
   * this stream.
   *
   * <p>SPEC-17 R5: only one row per (stream, minute) is written. If the caller invokes this method
   * multiple times within the same minute (e.g. SnapshotPublisher runs every second), all but the
   * first call within a minute are no-ops.
   *
   * @param snapshot the snapshot to persist
   */
  public void persist(StreamMetricSnapshotDTO snapshot) {
    Instant ts = Instant.ofEpochMilli(snapshot.snapshotTs());
    Instant minuteBucket = ts.truncatedTo(ChronoUnit.MINUTES);
    String streamId = snapshot.streamId();

    // Gate: skip if this minute has already been written for this stream
    Instant previous = lastWrittenMinute.get(streamId);
    if (minuteBucket.equals(previous)) {
      log.trace(
          "SPEC-17: snapshot for stream={} minute={} already written — skipping",
          streamId,
          minuteBucket);
      return;
    }

    // Update tracking map before async write to prevent concurrent duplicates
    lastWrittenMinute.put(streamId, minuteBucket);

    boolean acquired;
    try {
      acquired = semaphore.tryAcquire(50L, java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(
          "SPEC-17: interrupted waiting for snapshot Cassandra write permit stream={}", streamId);
      return;
    }

    if (!acquired) {
      log.warn(
          "SPEC-17: snapshot Cassandra write backpressure exceeded — dropping snapshot stream={}",
          streamId);
      return;
    }

    Map<String, Double> qd = snapshot.qualityDistribution();

    CompletableFuture.runAsync(
        () -> {
          try {
            MetricSnapshotEntity entity =
                MetricSnapshotEntity.builder()
                    .streamId(streamId)
                    .minuteBucket(minuteBucket)
                    .liveViewerCount(snapshot.liveViewerCount())
                    .bufferRatePct(snapshot.bufferRatePct())
                    .p95LatencyMs(snapshot.p95LatencyMs())
                    .healthScore(snapshot.healthScore())
                    .quality1080pPct(qd != null ? qd.getOrDefault("1080p", 0.0) : 0.0)
                    .quality720pPct(qd != null ? qd.getOrDefault("720p", 0.0) : 0.0)
                    .quality480pPct(qd != null ? qd.getOrDefault("480p", 0.0) : 0.0)
                    .quality360pPct(qd != null ? qd.getOrDefault("360p", 0.0) : 0.0)
                    .build();

            cassandraOperations.insert(entity);
            log.debug(
                "SPEC-17: metric snapshot persisted: stream={} minute={}", streamId, minuteBucket);
          } catch (Exception e) {
            log.error(
                "SPEC-17: failed to persist metric snapshot for stream={} minute={}: {}",
                streamId,
                minuteBucket,
                e.getMessage(),
                e);
          } finally {
            semaphore.release();
          }
        },
        writeExecutor);
  }

  /**
   * Returns the last-written minute bucket for a stream, or {@code null} if never written. Exposed
   * package-private for testing.
   */
  Instant getLastWrittenMinute(String streamId) {
    return lastWrittenMinute.get(streamId);
  }
}
