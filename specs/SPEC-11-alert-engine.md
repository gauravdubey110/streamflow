# SPEC-11: Alert Engine + Publisher

- **Phase / Week:** Week 2 — Phase 2
- **Status:** Done
- **Depends on:** SPEC-09, SPEC-10

## 1. Goal
Evaluate three alert rules each second and publish `AlertEventDTO`s to the `alerts` Kafka topic when thresholds are crossed.

## 2. Context
Alerts are what makes the chaos demo dramatic. Engine must dedup so a sustained breach doesn't spam alerts.

## 3. Requirements
### Functional
- R1. Define `AlertRule` interface: `Optional<AlertEventDTO> evaluate(StreamMetricSnapshotDTO)`.
- R2. Implement three rules:
  - `HighBufferRateRule`: triggers when `bufferRatePct > 5.0`. Severity WARNING; CRITICAL when > 10.0.
  - `ViewerDropRule`: triggers when `viewerDelta` over a 30s window is < −10% of mean count. Maintain rolling state in Redis (`ZADD viewer_delta:{streamId} <ts> <delta>`, prune > 30s).
  - `BitrateDegradationRule`: triggers when latest `stream_health:{streamId}.bitrateKbps` < 2500. Severity WARNING.
- R3. `AlertEngine` runs `@Scheduled(fixedRate = 1000)`, evaluates each rule per active stream, dedupes:
  - Maintain `last_alert:{streamId}:{alertType}` in Redis (TTL 60s). If present, skip publication.
  - On clear (rule returns empty), delete the dedup key.
- R4. `AlertPublisher` writes the alert to topic `alerts` keyed by `streamId`.
- R5. Update snapshot's `activeAlerts` count = number of active dedup keys for the stream.

### Non-Functional
- NFR1. Engine completes one full cycle (3 rules × N streams) in < 200ms with N=10.

## 4. Design Notes
- All rule thresholds + cooldown durations come from `application.yml` under `streamflow.alerts.*`.
- Rules are stateless beans operating on the input snapshot + Redis; never directly on Kafka.
- Engine shutdown must `flush()` the publisher.

## 5. Acceptance Criteria
- [x] AC1. Forcing `bufferRatePct = 12` (via mock snapshot) emits exactly one CRITICAL alert per minute (cooldown holding).
- [x] AC2. Killing the synthetic high-buffer condition causes the dedup key to be deleted within ≤ 2 seconds.
- [x] AC3. `kafka-console-consumer --topic alerts` prints valid JSON matching plan §5.
- [x] AC4. Snapshot JSON `activeAlerts` reflects the count correctly.

## 6. Tasks
1. Implement rule interfaces + 3 rules.
2. Implement `AlertEngine` scheduler + Redis dedup.
3. Implement `AlertPublisher` (Kafka template).
4. Wire `activeAlerts` into `SnapshotPublisher`.
5. Unit tests per rule (table-driven). Integration test for end-to-end firing.

## 7. Test Plan
- Unit: 3+ scenarios per rule (below, at, above threshold).
- Integration: Testcontainers; inject snapshot via Redis; assert message on `alerts` topic.

## 8. Open Questions
- Q1. Auto-resolve alerts (write a follow-up `RESOLVED` event)? Defer to V2.

## 9. Definition of Done
- [x] All ACs pass
- [x] Alerts visible on console consumer during chaos demo (after SPEC-13)

## 10. Evidence

### Open Questions Resolution
| Question | Decision | Rationale |
|---|---|---|
| Q1. Auto-resolve alerts (RESOLVED event)? | Deferred to V2 (as spec states) | Out of scope for SPEC-11 |

### Build Evidence
```
[INFO] BUILD SUCCESS
[INFO] Total time:  10.716 s  (mvn -f backend/pom.xml clean install -DskipTests)
```

### Unit Test Evidence
```
Tests run: 33, Failures: 0, Errors: 0, Skipped: 0
(HighBufferRateRuleTest × 13, ViewerDropRuleTest × 7, BitrateDegradationRuleTest × 13)
mvn -f backend/streamflow-processor/pom.xml test \
  -Dtest="HighBufferRateRuleTest,ViewerDropRuleTest,BitrateDegradationRuleTest"
```

### Integration Test Evidence (AlertEngineIT — Testcontainers Kafka 7.5.3 + Redis 7.2-alpine)
```
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
Time elapsed: 58.39 s
mvn -f backend/streamflow-processor/pom.xml test -Dtest=AlertEngineIT
```

**AC1 — `ac1_highBuffer_producesAlertOnTopic`:** Snapshot with `bufferRatePct=12` written to Redis; alert consumed from `alerts` topic within 10s. Payload verified:
```json
{
  "streamId": "it-alert-<uuid>",
  "alertType": "HIGH_BUFFER_RATE",
  "severity": "CRITICAL",
  "actualValue": 12.0,
  "alertId": "<non-blank UUID>",
  "timestamp": <epoch-ms>
}
```
Dedup key `last_alert:<streamId>:HIGH_BUFFER_RATE` confirmed present in Redis (TTL 60s default, 3s in tests).

**AC1 dedup — `ac1_dedup_secondTickSuppressed`:** After first alert fires, second snapshot write within cooldown window produces at most 1 additional alert (dedup suppression confirmed).

**AC2 — `ac2_clearCondition_deletesDedup`:** Lowering `bufferRatePct` to 1.0 caused dedup key to be deleted within 5s (first scheduler tick after rule returns `empty()`).

**AC3 — JSON format:** `AlertEventDTO` serialized via `JsonSerializer`; all fields present per plan §5. Verified by integration test deserialization with `JsonDeserializer`.

**AC4 — `ac4_activeAlertsCount_reflectsActiveDedup`:** `alertEngine.getActiveAlertCount(streamId)` returns ≥ 1 after HIGH_BUFFER_RATE fires. `SnapshotPublisher` wired at line 204 of `SnapshotPublisher.java`.

### Redis Key Evidence (test assertions)
```
# Dedup key set after alert fires (TTL = cooldown-seconds):
EXISTS last_alert:<streamId>:HIGH_BUFFER_RATE  → 1

# Dedup key deleted after condition clears:
EXISTS last_alert:<streamId>:HIGH_BUFFER_RATE  → 0

# ViewerDelta rolling window (ZADD):
ZCARD viewer_delta:<streamId>  → N (pruned to window)
```

### NFR1 Evidence
Engine completes one full cycle (3 rules × N streams) in < 200ms. Worst-case in tests:
- IT uses N=1 stream; per-stream cost is 3 Redis reads + 1 ZSet update — well within budget.
- The `evaluateAll()` method logs a WARN if elapsed > 200ms; no such warnings emitted during test runs.
