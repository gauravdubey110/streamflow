# Commit Plan — SPEC-10: Quality Distribution Aggregator

Suggested branch: feat/spec-10-quality-distribution-aggregator

---

## Commit 1 — Core SPEC-10 implementation (producer + aggregator + wiring)

**Message:**
```
SPEC-10: add QualityDistAggregator and wire into producer/processor

Extend NormalLoadStrategy to emit quality field on all events plus
QUALITY_SWITCH (5%) and BUFFER_START (5%) event types (raised from 1%
to make buffer rate observable — see Deviations section in spec).

Add QualityDistAggregator (Redis minute-bucket HINCRBY write path and
two-bucket merge read path). Wire into ViewerEventConsumer.recordEvent
and SnapshotPublisher to populate qualityDistribution + bufferRatePct
fields in every snapshot payload.

Refs: specs/SPEC-10-quality-distribution-aggregator.md
```

**Files:**
- backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/NormalLoadStrategy.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/QualityDistAggregator.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java

**Stage command:**
```bash
git add \
  backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/NormalLoadStrategy.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/QualityDistAggregator.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 2 — Tests for SPEC-10 (unit + integration)

**Message:**
```
SPEC-10: add QualityDistAggregator tests; update existing test suites

Add QualityDistAggregatorTest (15 unit tests via Testcontainers Redis)
and QualityDistAggregatorIT (2 integration tests via Testcontainers
Kafka+Redis) covering all acceptance criteria.

Update ViewerEventConsumerIT (timeout bump for SPEC-10 wiring),
SnapshotPublisherTest (assert qualityDistribution + bufferRatePct in
snapshot), and NormalLoadStrategyTest (updated distribution assertions
for 65/25/5/5 mix; add BUFFER_START bufferDurationMs range test).

Refs: specs/SPEC-10-quality-distribution-aggregator.md
```

**Files:**
- backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/QualityDistAggregatorTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/QualityDistAggregatorIT.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/ViewerEventConsumerIT.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java
- backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/NormalLoadStrategyTest.java

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/QualityDistAggregatorTest.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/QualityDistAggregatorIT.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/ViewerEventConsumerIT.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java \
  backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/NormalLoadStrategyTest.java
```

---

## Commit 3 — Mark SPEC-10 Done with evidence and commit plan

**Message:**
```
SPEC-10: mark Done, add evidence and commit plan

All acceptance criteria verified:
- AC1: qualityDistribution sums to 100 ±0.5 (QualityDistAggregatorIT)
- AC2: bufferRatePct measurable and accurate (unit + IT)
- AC3: high BUFFER_START injection raises bufferRatePct > 5% (IT)

Documents deviation: BUFFER_START baseline raised from 1% to 5% to
make the rate observable. All 74 backend tests pass (0 failures).

Refs: specs/SPEC-10-quality-distribution-aggregator.md
```

**Files:**
- specs/SPEC-10-quality-distribution-aggregator.md
- commits/SPEC-10.md

**Stage command:**
```bash
git add specs/SPEC-10-quality-distribution-aggregator.md commits/SPEC-10.md
```

---

## Verification before pushing
- [x] `mvn -f backend/pom.xml verify` — 74 tests, 0 failures, BUILD SUCCESS
- [x] Demo evidence in spec matches reality
- [ ] Frontend: N/A — SPEC-10 is backend-only (no frontend changes in this spec)
