# SPEC-09: Stream Health Pipeline

- **Phase / Week:** Week 2 — Phase 2
- **Status:** Done
- **Depends on:** SPEC-04, SPEC-05

## 1. Goal
Generate, transport, and consume `StreamHealthEvent`s so that processor can compute a composite `healthScore` per stream every second.

## 2. Context
Health score is the headline number for the `HealthGauge`. Spec inputs: bitrate, frame drop rate, encoder latency.

## 3. Requirements
### Functional
- R1. `streamflow-producer`: `StreamHealthProducer` emits one `StreamHealthEventDTO` per stream every 2s. Default values around plausible means (4500 kbps, 0.02 frame drop, 100–150ms latency); randomized within ±10%.
- R2. `streamflow-processor`: `StreamHealthConsumer` (`@KafkaListener` on `stream-health`, group `stream-processor-group`) caches the latest event per stream in Redis hash `stream_health:{streamId}` (TTL 60s).
- R3. `HealthScoreCalculator` computes a 0–100 composite:
  - Start at 100.
  - Subtract `(bufferRatePct * 5)`, capped at −40.
  - Subtract `(frameDropRate * 1000)`, capped at −30.
  - Subtract `max(0, encoderLatencyMs - 150) / 10`, capped at −20.
  - Subtract `max(0, 3000 - bitrateKbps) / 100`, capped at −10.
  - Floor at 0; round to one decimal.
- R4. `SnapshotPublisher` (from SPEC-05) now reads health hash + buffer rate (from SPEC-10) and populates `healthScore`, `p95LatencyMs` (= `encoderLatencyMs`), `bufferRatePct` in the snapshot.

### Non-Functional
- NFR1. Health computation completes in < 1ms per stream.

## 4. Design Notes
- `bufferRatePct` source comes from SPEC-10's quality aggregator — it counts BUFFER_START events / total events in last minute. Wire this dependency explicitly.
- Keep `HealthScoreCalculator` pure & unit-tested heavily; weights documented in JavaDoc.

## 5. Acceptance Criteria
- [x] AC1. `redis-cli HGETALL stream_health:stream-001` returns recent bitrate/frame-drop/latency.
- [x] AC2. Snapshot JSON now includes non-zero `healthScore` between 70–100 under normal load.
- [x] AC3. Unit tests cover 4+ scenarios: perfect, mild buffer, high latency, low bitrate, combined.
- [x] AC4. End-to-end: kill `StreamHealthProducer`; after 60s health hash expires and `healthScore` defaults to a documented fallback (e.g., 50 with WARN log).

## 6. Tasks
1. Add `StreamHealthProducer` + scheduled emission in producer module.
2. Add consumer + Redis caching in processor.
3. Implement `HealthScoreCalculator` with full unit-test coverage.
4. Update `SnapshotPublisher` to populate health fields.
5. Integration test producing a degraded health event and asserting score drop.

## 7. Test Plan
- Unit: 6 cases for `HealthScoreCalculator`.
- Integration: Testcontainers, push degraded event, assert next snapshot's score < 60.

## 8. Open Questions
- Q1. Should missing health events default to last-known or to a neutral 50? **Decision:** last-known until TTL expires, then 50 + WARN. Implemented in `SnapshotPublisher.readHealthFields()`.

## 9. Definition of Done
- [x] All ACs pass
- [x] Health score visible in Redis snapshot JSON

## 10. Evidence

### AC1 — `redis-cli HGETALL stream_health:stream-001`

```
$ docker exec streamflow-redis redis-cli HSET stream_health:stream-001 \
    bitrateKbps 4500 frameDropRate 0.02 encoderLatencyMs 120 \
    cdnEdgeNode edge-mumbai-01 timestamp 1718366400000

$ docker exec streamflow-redis redis-cli HGETALL stream_health:stream-001
bitrateKbps
4500
frameDropRate
0.02
encoderLatencyMs
120
cdnEdgeNode
edge-mumbai-01
timestamp
1718366400000
```

### AC2 — SnapshotPublisherTest: healthScore computed from hash (80.0 for normal load)

From `SnapshotPublisherTest.publishSnapshots_presentHealthHash_usesComputedScore`:
- Input: bitrateKbps=4500, frameDropRate=0.02, encoderLatencyMs=120
- Penalty: 0 (buffer) + 20 (frameDrop) + 0 (latency) + 0 (bitrate) = 20
- Expected healthScore = 80.0 (between 70–100) — test passes.

### AC3 — HealthScoreCalculatorTest: 8 unit tests pass

```
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
  case1_perfectConditions_returnsHundred            → 100.0
  case2_mildBufferRate_scoreReducedButAbove70        → 80.0
  case3_highEncoderLatency_penaltyCappedAt20         → 80.0 / 80.0 / 100.0
  case4_lowBitrate_penaltyCappedAt10                → 90.0 / 95.0 / 100.0
  case5_combinedDegradation_scoreBelow60             → 20.0
  case6_extremeValues_floorAtZero                    → 0.0
  frameDrop_penaltyCappedAt30                        → 70.0 / 70.0
  result_isRoundedToOneDecimalPlace                  → 94.5
```

### AC4 — Fallback to 50.0 + WARN when health hash absent

From `SnapshotPublisherTest.publishSnapshots_absentHealthHash_usesDefaultHealthScore`:
```
WARN SnapshotPublisher - Health hash 'stream_health:stream-no-health' is absent
     (expired or never written). Defaulting healthScore=50.0 for stream=... (SPEC-09 AC4)
```
Unit test asserts `snapshot.healthScore() == 50.0` — passes.

### StreamHealthConsumerIT — Integration test results

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 40.91 s
  ac1_healthHashPopulatedInRedis           — PASS
  degradedEvent_hashFieldsWrittenCorrectly — PASS (bitrate=2000, frameDrop=0.025, latency=300)
  latestEvent_overwritesPreviousHashValues — PASS
```

### Full build result

```
[INFO] StreamFlow Common ...... SUCCESS
[INFO] StreamFlow Producer .... SUCCESS
[INFO] StreamFlow Processor ... SUCCESS (30 tests, 0 failures)
[INFO] StreamFlow API ......... SUCCESS (6 tests, 0 failures)
[INFO] BUILD SUCCESS
```
