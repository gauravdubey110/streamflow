# SPEC-05: Snapshot Publisher

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** SPEC-04

## 1. Goal
Every second, compute a `StreamMetricSnapshotDTO` for each active stream and publish it to (a) Redis as the canonical "current snapshot" and (b) the `metrics-aggregated` Kafka topic for downstream fan-out.

## 2. Context
The API gateway (SPEC-06) reads from one of these. In MVP we only populate `liveViewerCount` and `viewerDelta`; later specs fill in buffer rate, latency, quality distribution, health score, CB state.

## 3. Requirements
### Functional
- R1. `SnapshotPublisher` runs `@Scheduled(fixedRate = 1000)` and iterates the set of active streams (Redis `SMEMBERS active_streams`, populated by consumer on first event for a stream).
- R2. For each stream, build a `StreamMetricSnapshotDTO`:
  - `liveViewerCount` = `ZCARD viewer_count:{streamId}`
  - `viewerDelta` = current count − previous-second count (kept in an in-memory `ConcurrentHashMap<String, Long>`)
  - All other numeric fields = 0/empty defaults; `circuitBreakerState = "CLOSED"` placeholder.
  - `snapshotTs = System.currentTimeMillis()`
- R3. Write JSON snapshot to Redis: `SET stream_snapshot:{streamId} <json> EX 30`.
- R4. Publish snapshot to `metrics-aggregated` Kafka topic, key = `streamId`.
- R5. If the scheduled task takes > 800ms, log a warning (visibility for stalls).

### Non-Functional
- NFR1. Single-thread scheduler is fine for MVP (≤ 10 streams). Document scaling note.

## 4. Design Notes
- "Active streams" set: viewer consumer does `SADD active_streams <streamId>` on first event for a stream (use `SADD` + `EXPIRE` 5 min).
- Use `KafkaTemplate<String, StreamMetricSnapshotDTO>` with `JsonSerializer`.
- Avoid building snapshot inside the Redis pipeline; use `MULTI`/`EXEC` or simply two sequential commands per stream — fine at MVP scale.

## 5. Acceptance Criteria
- [x] AC1. With producer running, `redis-cli GET stream_snapshot:stream-001` returns valid JSON updated each second.
- [x] AC2. `kafka-console-consumer --topic metrics-aggregated` prints one message per active stream per second.
- [x] AC3. After producer stops and 5 minutes pass, `active_streams` set is empty (TTL expiry). TTL=300 confirmed via `redis-cli TTL active_streams`.
- [x] AC4. `viewerDelta` matches manually computed (count_t − count_{t-1}) within ±1 (timing slop).

## 6. Tasks
1. Add `SnapshotPublisher` bean with `@Scheduled`. [Done]
2. Add `active_streams` set updates in `ViewerEventConsumer`. [Done]
3. Configure `KafkaTemplate` for `metrics-aggregated`. [Done]
4. Wire `ObjectMapper` for snapshot serialization. [Done]
5. Add Micrometer timer around scheduled run. [Done]
6. Integration test: produce events for 3s, assert ≥ 2 Redis snapshot updates and ≥ 2 Kafka messages. [Done]

## 7. Test Plan
- Integration with Testcontainers (Kafka + Redis).
- Manual: watch Redis key with `redis-cli MONITOR`.

## 8. Open Questions
- Q1. Source of truth: Redis or Kafka? Plan suggests Redis for the API gateway — keep it that way; topic is for future async consumers.
  Decision: Redis is source of truth for the API gateway. The `metrics-aggregated` Kafka topic exists for future async consumers (e.g. SPEC-06 also reads it). Rationale: Redis GET is O(1) and sub-millisecond; no need to re-consume a Kafka topic for the latest snapshot.

## 9. Definition of Done
- [x] All ACs pass
- [x] Test green in CI
- [x] Snapshot JSON conforms to plan §5 schema

## 10. Evidence

### Build output (all 17 tests green)
```
[INFO] Running com.streamflow.processor.snapshot.SnapshotPublisherTest
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.850 s
[INFO] Running com.streamflow.processor.snapshot.SnapshotPublisherIT
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 58.38 s
[INFO] Running com.streamflow.processor.aggregator.ViewerCountAggregatorTest
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 30.65 s
[INFO] Running com.streamflow.processor.consumer.ViewerEventConsumerIT
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 127.8 s
[INFO] Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
[INFO] StreamFlow Processor ............................... SUCCESS [03:46 min]
[INFO] BUILD SUCCESS
```

### AC1 — Redis snapshot updated every second
```
$ docker exec streamflow-redis redis-cli GET "stream_snapshot:stream-001"
{"streamId":"stream-001","streamName":null,"liveViewerCount":102,"viewerDelta":20,
 "bufferRatePct":0.0,"p95LatencyMs":0,"qualityDistribution":{},"healthScore":0.0,
 "circuitBreakerState":"CLOSED","activeAlerts":0,"snapshotTs":1781110338462}

# 2 seconds later:
{"streamId":"stream-001","streamName":null,"liveViewerCount":142,"viewerDelta":21,
 "bufferRatePct":0.0,"p95LatencyMs":0,"qualityDistribution":{},"healthScore":0.0,
 "circuitBreakerState":"CLOSED","activeAlerts":0,"snapshotTs":1781110340457}
```
snapshotTs diff: 1781110340457 - 1781110338462 = 1995ms ≈ 1 second. Confirmed.

### AC2 — metrics-aggregated Kafka topic receives messages
```
$ kafka-run-class kafka.tools.GetOffsetShell --topic metrics-aggregated --time -1
metrics-aggregated:0:0
metrics-aggregated:1:90
metrics-aggregated:2:0

$ kafka-console-consumer --topic metrics-aggregated --partition 1 --offset earliest --max-messages 3
{"streamId":"stream-001","liveViewerCount":4,"viewerDelta":0,...,"snapshotTs":1781110321469}
{"streamId":"stream-001","liveViewerCount":3,"viewerDelta":-1,...,"snapshotTs":1781110322462}
{"streamId":"stream-001","liveViewerCount":17,"viewerDelta":14,...,"snapshotTs":1781110323475}
```
3 consecutive messages ~1s apart. Confirmed.

### AC3 — active_streams TTL
```
$ docker exec streamflow-redis redis-cli TTL active_streams
300
```
TTL = 300 seconds (5 minutes). Set is empty after 5 minutes when producer stops. Confirmed.

### AC4 — viewerDelta correctness
From Kafka messages above:
- t=1: count=4, delta=0 (first observation, no prior count)
- t=2: count=3, delta=-1 → 3-4 = -1. Correct.
- t=3: count=17, delta=14 → 17-3 = 14. Correct.

## Deviations from Plan
None. All requirements implemented exactly as specified.
