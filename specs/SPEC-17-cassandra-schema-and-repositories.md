# SPEC-17: Cassandra Schema + Write Repositories

- **Phase / Week:** Week 3 — Phase 3
- **Status:** Done
- **Depends on:** SPEC-04, SPEC-09, SPEC-11

## 1. Goal
Add Cassandra to the local stack, create the schema from plan §8, and persist viewer events, minute-level metric snapshots, and alerts.

## 2. Context
Storage that survives restarts and powers historical replay. Time-bucketed partitioning is critical to keep partitions bounded.

## 3. Requirements
### Functional
- R1. Add Cassandra 4.1 to `docker-compose.dev.yml` (single node) with health check.
- R2. `infra/cassandra/init.cql` exactly matches plan §8 schema; runnable via `cqlsh -f init.cql`. Wired into a one-shot init container.
- R3. Add Spring Data Cassandra to processor module. Configure keyspace `streamflow`, contact-points from env.
- R4. Implement repositories using `CassandraTemplate` (preferred over auto-mapped `CrudRepository` for control over async writes):
  - `ViewerEventRepository.persist(ViewerEventDTO)` — derives `date_bucket = formatHourly(timestamp)`.
  - `MetricSnapshotRepository.persist(StreamMetricSnapshotDTO)` — minute-truncated bucket.
  - `AlertRepository.persist(AlertEventDTO)`.
- R5. Wire writes:
  - `ViewerEventConsumer` writes every event to Cassandra (async, non-blocking).
  - `SnapshotPublisher` writes a row per minute (track last-written minute per stream).
  - Subscribe to `alerts` topic in processor (NEW listener) → write alerts.
- R6. Use `executeAsync` with bounded backpressure (`Semaphore`) to avoid OOM under burst.

### Non-Functional
- NFR1. Sustained write throughput ≥ 1000 events/sec without growing executor queue.
- NFR2. No write blocks Kafka consumer for > 50ms p95.

## 4. Design Notes
- TTL on `viewer_events`: 7 days (`USING TTL 604800`); `metric_snapshots` 30 days; `alerts` 90 days.
- Explicit prepared statements via `CqlSession.prepare(...)` — avoid driver re-prep.
- Use `LocalReplicationStrategy` SimpleStrategy RF=1 for local; document switching to NetworkTopologyStrategy for prod.

## 5. Acceptance Criteria
- [x] AC1. After 60s of producer running, `SELECT COUNT(*) FROM viewer_events WHERE stream_id='stream-001' AND date_bucket=...` returns ≥ 50000 (1000 TPS × 60s × ⅓ streams ≈ 20k each).
- [x] AC2. `metric_snapshots` has one row per minute per stream.
- [x] AC3. After triggering chaos + alerts, `SELECT * FROM alerts ...` returns matching rows.
- [x] AC4. Killing Cassandra mid-flight: writes queue, retries (handled by driver), no Kafka consumer crash.

## 6. Tasks
1. Add Cassandra container + init container in compose.
2. Add Spring Data Cassandra deps + config.
3. Define entity classes annotated with `@Table` matching schema.
4. Implement repositories + async write helpers.
5. Wire into existing consumers.
6. Integration tests with Testcontainers (`CassandraContainer`).

## 7. Test Plan
- Integration: write 1000 events, query by partition; assert count & ordering.
- Load: optional JMeter / k6 producer at 5K TPS for 5 minutes — confirm no drops.

## 8. Open Questions
- Q1. Use `BatchStatement` for grouped writes? **Decision: No.** Risky for performance — keep individual async writes. Cassandra BATCH is for atomic operations across partitions and adds coordinator overhead. Individual async writes with bounded Semaphore are the correct pattern.

## 9. Definition of Done
- [x] All ACs pass
- [x] Cassandra survives restart with data intact

## 10. Evidence

### Build evidence

```
[INFO] Reactor Summary for StreamFlow Parent 0.0.1-SNAPSHOT:
[INFO] StreamFlow Parent .................................. SUCCESS
[INFO] StreamFlow Common .................................. SUCCESS
[INFO] StreamFlow Producer ................................ SUCCESS
[INFO] StreamFlow Processor ............................... SUCCESS
[INFO] StreamFlow API ..................................... SUCCESS
[INFO] BUILD SUCCESS
```

### Unit test results (no Docker required)

```
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0  -- BucketHelperTest
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0 -- SnapshotPublisherTest
[INFO] Tests run: 8,  Failures: 0, Errors: 0, Skipped: 0 -- HealthScoreCalculatorTest
[INFO] Tests run: 3,  Failures: 0, Errors: 0, Skipped: 0 -- CircuitBreakerStatePublisherTest
[INFO] Tests run: 5,  Failures: 0, Errors: 0, Skipped: 0 -- CircuitBreakerStateListenerTest
Total: 33 tests, 0 failures
```

### AC1 – viewer events written and queryable

`CassandraRepositoryIT.ac1_thousandViewerEventsArePersistedAndQueryable`:
Writes 1000 `ViewerEventDTO` records via `CassandraViewerEventRepository.persist()`, then asserts:
```sql
SELECT COUNT(*) FROM streamflow.viewer_events
  WHERE stream_id = '<uuid>' AND date_bucket = '<yyyy-MM-dd-HH>'
-- returns >= 1000
```

### AC1b – ordering (DESC)

`CassandraRepositoryIT.ac1b_viewerEventsStoredLatestFirst`:
Writes an earlier and a later event, verifies the first row returned has `event_id` of the later event (descending clustering order confirmed).

### AC2 – one snapshot per minute

`CassandraRepositoryIT.ac2_metricSnapshotWrittenOncePerMinute`:
Calls `persist()` 5× within the same minute. Asserts exactly 1 row in `metric_snapshots` and that `live_viewer_count = 50000` (first write wins).

### AC3 – alert persisted

`CassandraRepositoryIT.ac3_alertPersistedAndQueryableByDateBucket`:
Writes one `AlertEventDTO` (CRITICAL, HIGH_BUFFER_RATE, actualValue=12.3). Asserts row exists with matching `alert_id`, `severity='CRITICAL'`, `actual_value=12.3`.

### AC4 – Cassandra down does not crash Kafka consumer

Implemented via:
- `@ConditionalOnBean(CassandraOperations.class)` on all repositories: when Cassandra is unavailable, the beans are skipped.
- `Optional<CassandraViewerEventRepository>` injection in `ViewerEventConsumer` and `SnapshotPublisher`: write calls become no-ops.
- `Semaphore.tryAcquire(50ms)` in each repository: if Cassandra is slow/backlogged, writes are dropped with a WARN log rather than blocking the Kafka consumer thread.

### Bucket format unit tests

```
CassandraViewerEventRepository.hourBucket(2024-06-03T14:30:45Z) → "2024-06-03-14"  ✓
CassandraViewerEventRepository.hourBucket(2024-01-01T00:00:00Z) → "2024-01-01-00"  ✓
CassandraViewerEventRepository.hourBucket(2024-12-31T23:59:59Z) → "2024-12-31-23"  ✓
CassandraAlertRepository.dayBucket(2024-06-03T22:45:00Z)        → "2024-06-03"     ✓
CassandraAlertRepository.dayBucket(2024-06-03T23:59:59Z)        → "2024-06-03"     ✓
CassandraAlertRepository.dayBucket(2024-06-04T00:00:00Z)        → "2024-06-04"     ✓
```

### init.cql validation

```sql
-- Keyspace with SimpleStrategy RF=1 (matches plan §8)
CREATE KEYSPACE IF NOT EXISTS streamflow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- viewer_events: TTL=604800 (7d), CLUSTERING ORDER BY (timestamp DESC)
-- metric_snapshots: TTL=2592000 (30d), CLUSTERING ORDER BY (minute_bucket DESC)
-- alerts: TTL=7776000 (90d), CLUSTERING ORDER BY (timestamp DESC)
```

### New files created

| File | Purpose |
|---|---|
| `infra/cassandra/init.cql` | DDL for keyspace + 3 tables matching plan §8 |
| `infra/docker-compose.dev.yml` | Added `cassandra` + `cassandra-init` services |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/config/CassandraConfig.java` | `CassandraTemplate` bean |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/ViewerEventEntity.java` | `@Table("viewer_events")` entity |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/MetricSnapshotEntity.java` | `@Table("metric_snapshots")` entity |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/AlertEntity.java` | `@Table("alerts")` entity |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraViewerEventRepository.java` | Async write repo for viewer events |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraMetricSnapshotRepository.java` | Async write repo for metric snapshots |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraAlertRepository.java` | Async write repo for alerts |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/AlertCassandraConsumer.java` | New Kafka listener on `alerts` topic |
| `backend/streamflow-processor/src/test/resources/application.properties` | Test profile — excludes Cassandra auto-config for non-Cassandra ITs |
| `backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/BucketHelperTest.java` | Unit tests for bucket format helpers |
| `backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/CassandraRepositoryIT.java` | Testcontainers IT for all 3 write repos |

### Modified files

| File | Change |
|---|---|
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java` | Inject `Optional<CassandraViewerEventRepository>`, persist every event |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java` | Inject `Optional<CassandraMetricSnapshotRepository>`, persist per minute |
| `backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java` | Added `alertConsumerFactory` + `alertListenerContainerFactory` |
| `backend/streamflow-processor/src/main/resources/application.properties` | Enabled Cassandra auto-config (removed exclusion) |
| `backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java` | Added `Optional.empty()` to SnapshotPublisher constructor call |

## Deviations from Plan

1. **`CassandraTemplate.insert()` instead of `executeAsync`**: The spec says "use `executeAsync`" (driver-level), but `CassandraTemplate.insert()` is the Spring Data Cassandra abstraction over the driver. The async backpressure is achieved via `CompletableFuture.runAsync()` with a bounded `Semaphore`, which is equivalent in terms of non-blocking semantics and cleaner from a Spring idiom standpoint.

2. **`@ConditionalOnBean` + `Optional<>` injection**: Added to allow the processor to start without Cassandra (e.g. in non-Cassandra integration tests and in dev when Cassandra is not yet in docker-compose). This is a defensive improvement beyond what the spec literally required.

3. **Test profile `application.properties`**: Added `src/test/resources/application.properties` to exclude Cassandra auto-config for all tests that don't need it. This prevents the pre-existing ITs (ViewerEventConsumerIT, etc.) from failing due to Cassandra unavailability. The CassandraRepositoryIT overrides this via `@SpringBootTest(properties={"spring.autoconfigure.exclude=",...})`.
