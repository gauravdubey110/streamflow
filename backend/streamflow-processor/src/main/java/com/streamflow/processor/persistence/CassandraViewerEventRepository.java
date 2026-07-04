package com.streamflow.processor.persistence;

import com.streamflow.common.dto.ViewerEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Write repository for the {@code streamflow.viewer_events} Cassandra table.
 *
 * <p>SPEC-17 R4: persists {@link ViewerEventDTO} records asynchronously with bounded
 * backpressure. The repository:
 * <ul>
 *   <li>Derives the {@code date_bucket} (hourly, format {@code yyyy-MM-dd-HH}) from
 *       the event's {@code timestamp} field.</li>
 *   <li>Fires a non-blocking insert via a dedicated executor thread pool.</li>
 *   <li>Uses a {@link Semaphore} (capacity {@value #MAX_IN_FLIGHT}) to cap the number
 *       of concurrent in-flight writes — prevents OOM under burst (SPEC-17 R6).</li>
 * </ul>
 *
 * <p>SPEC-17 NFR1: sustained write throughput target is ≥ 1000 events/sec.
 * At 1000 TPS with 3 streams the consumer delivers ~333 events/stream/sec.
 * With 64 in-flight permits and a typical Cassandra single-node round-trip
 * of &lt; 5 ms, the steady-state throughput per-thread exceeds 200 ops/sec,
 * so 4 threads can absorb the full 1000 TPS comfortably.
 *
 * <p>SPEC-17 NFR2: the Kafka consumer thread never blocks; the {@link Semaphore#acquire()}
 * call is the only potential blocking point. With the permit count set generously relative
 * to the expected throughput, p95 waiting time is effectively 0.
 *
 * <p>If the caller thread cannot acquire a permit within
 * {@value #ACQUIRE_TIMEOUT_MS} ms (i.e. Cassandra is severely backed up), the write
 * is dropped and a WARN is logged — this is preferable to blocking the Kafka consumer
 * thread indefinitely, which could halt partition consumption.
 */
@Slf4j
@Repository
@ConditionalOnBean(CassandraOperations.class)
public class CassandraViewerEventRepository {

    /** Hourly bucket date format: {@code yyyy-MM-dd-HH}. */
    private static final DateTimeFormatter HOUR_BUCKET_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneOffset.UTC);

    /** Maximum number of concurrent in-flight Cassandra writes (SPEC-17 R6). */
    static final int MAX_IN_FLIGHT = 256;

    /** Timeout for acquiring a semaphore permit (milliseconds). */
    private static final long ACQUIRE_TIMEOUT_MS = 50L;

    private final CassandraOperations cassandraOperations;
    private final Semaphore semaphore;
    private final Executor writeExecutor;

    public CassandraViewerEventRepository(CassandraOperations cassandraOperations) {
        this.cassandraOperations = cassandraOperations;
        this.semaphore = new Semaphore(MAX_IN_FLIGHT, true);
        // Virtual threads available in Java 21+; using a bounded cached pool for Java 17 compat.
        this.writeExecutor = Executors.newFixedThreadPool(4,
                r -> {
                    Thread t = new Thread(r, "cassandra-viewer-writer");
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Persists a viewer event to Cassandra asynchronously.
     *
     * <p>SPEC-17 R4: derives {@code date_bucket} as an hourly bucket from the event's
     * {@code timestamp} (epoch-millis). Converts the millisecond timestamp to an
     * {@link Instant} for the Cassandra {@code TIMESTAMP} column.
     *
     * <p>SPEC-17 R6: bounded by {@link #semaphore}. If the backpressure limit is hit,
     * the write is dropped with a WARN log instead of blocking the caller.
     *
     * @param event the viewer event to persist
     */
    public void persist(ViewerEventDTO event) {
        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(ACQUIRE_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("SPEC-17: interrupted waiting for Cassandra write permit for stream={}", event.streamId());
            return;
        }

        if (!acquired) {
            log.warn("SPEC-17: Cassandra write backpressure exceeded (>{} in-flight) " +
                    "— dropping viewer event for stream={}", MAX_IN_FLIGHT, event.streamId());
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                Instant ts = Instant.ofEpochMilli(event.timestamp());
                String dateBucket = HOUR_BUCKET_FMT.format(ts);

                ViewerEventEntity entity = ViewerEventEntity.builder()
                        .streamId(event.streamId())
                        .dateBucket(dateBucket)
                        .timestamp(ts)
                        .eventId(UUID.fromString(event.eventId()))
                        .viewerId(event.viewerId())
                        .eventType(event.eventType() != null ? event.eventType().name() : null)
                        .quality(event.quality() != null ? event.quality().name() : null)
                        .bufferMs(event.bufferDurationMs() != null
                                ? event.bufferDurationMs().intValue() : null)
                        .region(event.region())
                        .build();

                cassandraOperations.insert(entity);
                log.trace("SPEC-17: viewer event persisted: stream={} eventId={}",
                        event.streamId(), event.eventId());
            } catch (Exception e) {
                log.error("SPEC-17: failed to persist viewer event for stream={} eventId={}: {}",
                        event.streamId(), event.eventId(), e.getMessage(), e);
            } finally {
                semaphore.release();
            }
        }, writeExecutor);
    }

    /**
     * Returns the formatted hourly bucket for the given epoch-millis timestamp.
     * Exposed package-private for unit testing.
     */
    static String hourBucket(long epochMillis) {
        return HOUR_BUCKET_FMT.format(Instant.ofEpochMilli(epochMillis));
    }
}
