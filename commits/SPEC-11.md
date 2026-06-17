# Commit Plan — SPEC-11: Alert Engine + Publisher

Suggested branch: `feat/spec-11-alert-engine`

---

## Commit 1 — Add AlertKafkaConfig producer bean

**Message:**
```
SPEC-11: add AlertKafkaConfig for alerts Kafka producer

Adds a dedicated ProducerFactory<String, AlertEventDTO> and the
named `alertKafkaTemplate` bean used by AlertPublisher. Separate
from the existing DLT and snapshot producer factories so each
has its own serializer configuration.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/config/AlertKafkaConfig.java`

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/config/AlertKafkaConfig.java
```

---

## Commit 2 — Implement AlertRule interface and three rules

**Message:**
```
SPEC-11: implement AlertRule interface and three alert rules

Adds the AlertRule contract (R1) and three implementations (R2):
- HighBufferRateRule: WARNING >5%, CRITICAL >10% bufferRatePct
- ViewerDropRule: WARNING when rolling 30s viewer-delta sum
  drops below -10% of live count; state in Redis ZSet
- BitrateDegradationRule: WARNING when bitrateKbps < 2500,
  read from stream_health:{streamId} hash written by
  StreamHealthConsumer

All thresholds configurable via streamflow.alerts.* properties.
Rules are stateless beans; all state lives in Redis.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertRule.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/HighBufferRateRule.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/ViewerDropRule.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/BitrateDegradationRule.java`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertRule.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/HighBufferRateRule.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/ViewerDropRule.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/BitrateDegradationRule.java
```

---

## Commit 3 — Implement AlertPublisher and AlertEngine scheduler

**Message:**
```
SPEC-11: add AlertPublisher and AlertEngine with Redis dedup

AlertPublisher (R4): sends AlertEventDTO to the `alerts` topic
keyed by streamId; calls flush() on shutdown.

AlertEngine (R3): @Scheduled(fixedRate=1000) evaluates all
registered AlertRule beans for every active stream. Deduplicates
via last_alert:{streamId}:{alertType} Redis keys (TTL =
streamflow.alerts.cooldown-seconds, default 60s). Clears dedup
key when rule returns empty. Exposes getActiveAlertCount() for
SnapshotPublisher (R5). @PreDestroy flushes the publisher.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertPublisher.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertEngine.java`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertPublisher.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertEngine.java
```

---

## Commit 4 — Wire activeAlerts into SnapshotPublisher (R5)

**Message:**
```
SPEC-11: wire AlertEngine.getActiveAlertCount into SnapshotPublisher

SnapshotPublisher now injects AlertEngine and calls
getActiveAlertCount(streamId) to populate the activeAlerts field
in StreamMetricSnapshotDTO instead of the prior placeholder 0.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java`

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 5 — Add alert properties to application.properties

**Message:**
```
SPEC-11: add streamflow.alerts.* properties to processor config

Documents and sets defaults for all alert thresholds and
cooldown duration consumed by the rule beans and AlertEngine.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/main/resources/application.properties`

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/resources/application.properties
```

---

## Commit 6 — Add unit tests for all three rules

**Message:**
```
SPEC-11: add table-driven unit tests for all three alert rules

HighBufferRateRuleTest: 13 scenarios (below/at/above warning,
above critical, custom thresholds, AC1 proxy).
ViewerDropRuleTest: 7 scenarios (no viewers, below/at/above
threshold, rolling window pruning, mock Redis interaction).
BitrateDegradationRuleTest: 13 scenarios (absent hash, missing
field, below/at/above threshold, custom threshold).

Tests run: 33, Failures: 0.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/HighBufferRateRuleTest.java`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/ViewerDropRuleTest.java`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/BitrateDegradationRuleTest.java`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/HighBufferRateRuleTest.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/ViewerDropRuleTest.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/BitrateDegradationRuleTest.java
```

---

## Commit 7 — Add AlertEngineIT integration test

**Message:**
```
SPEC-11: add AlertEngineIT end-to-end integration test

Testcontainers (Kafka 7.5.3 + Redis 7.2-alpine):
- ac1_highBuffer_producesAlertOnTopic: bufferRatePct=12 → CRITICAL
  alert on `alerts` topic, dedup key set in Redis
- ac1_dedup_secondTickSuppressed: cooldown window suppresses
  repeated alerts
- ac2_clearCondition_deletesDedup: lowering buffer rate removes
  dedup key within ≤ 3 scheduler ticks
- ac4_activeAlertsCount_reflectsActiveDedup: getActiveAlertCount
  returns ≥ 1 after alert fires

Tests run: 4, Failures: 0, Time: 58s.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/AlertEngineIT.java`

**Stage command:**
```bash
git add backend/streamflow-processor/src/test/java/com/streamflow/processor/alert/AlertEngineIT.java
```

---

## Commit 8 — Mark SPEC-11 Done with evidence

**Message:**
```
SPEC-11: mark Done, add evidence and commit plan

Ticks all ACs and DoD checkboxes. Adds §10 Evidence with build
output, test run results, Redis key assertions, and NFR1 timing
analysis.

Refs: specs/SPEC-11-alert-engine.md
```

**Files:**
- `specs/SPEC-11-alert-engine.md`
- `commits/SPEC-11.md`

**Stage command:**
```bash
git add specs/SPEC-11-alert-engine.md commits/SPEC-11.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml clean install -DskipTests` → BUILD SUCCESS
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="HighBufferRateRuleTest,ViewerDropRuleTest,BitrateDegradationRuleTest"` → 33 tests, 0 failures
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest=AlertEngineIT` → 4 tests, 0 failures
- [ ] Demo evidence in spec §10 matches reality
