# Commit Plan — SPEC-09: Stream Health Pipeline

Suggested branch: feat/spec-09-stream-health-pipeline

---

## Commit 1 — Add StreamHealthProducer and healthEventKafkaTemplate

**Message:**
```
SPEC-09: add StreamHealthProducer and health KafkaTemplate to producer

Adds StreamHealthProducer (@Scheduled every 2 s) that emits one
StreamHealthEventDTO per configured stream with ±10% jitter around
4500 kbps / 0.02 frame-drop / 125 ms latency baselines.

Adds healthEventProducerFactory + healthEventKafkaTemplate beans to
KafkaProducerConfig so the producer can publish to stream-health topic.

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/config/KafkaProducerConfig.java

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/config/KafkaProducerConfig.java
```

---

## Commit 2 — Add StreamHealthConsumer + streamHealthListenerContainerFactory

**Message:**
```
SPEC-09: add StreamHealthConsumer and Kafka consumer factory in processor

StreamHealthConsumer (@KafkaListener on stream-health, group
stream-processor-group) caches the latest health event per stream in
Redis Hash stream_health:{streamId} with a 60 s TTL.

streamHealthListenerContainerFactory added to KafkaConsumerConfig with
concurrency=1 and BATCH ack mode (health events are idempotent).

application.properties updated with streamflow.kafka.topics.stream-health.

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/StreamHealthConsumer.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java
- backend/streamflow-processor/src/main/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/StreamHealthConsumer.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java \
        backend/streamflow-processor/src/main/resources/application.properties
```

---

## Commit 3 — Add HealthScoreCalculator with full JavaDoc

**Message:**
```
SPEC-09: add HealthScoreCalculator with documented penalty weights

Pure component that computes a 0-100 composite health score from:
  - bufferRatePct x5, capped at -40
  - frameDropRate x1000, capped at -30
  - (encoderLatencyMs - 150) / 10, capped at -20 (below 150 ms: free)
  - (3000 - bitrateKbps) / 100, capped at -10 (above 3000 kbps: free)
  - result floored at 0, rounded to one decimal place

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/HealthScoreCalculator.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/HealthScoreCalculator.java
```

---

## Commit 4 — Wire health score into SnapshotPublisher

**Message:**
```
SPEC-09: wire HealthScoreCalculator into SnapshotPublisher

SnapshotPublisher now reads stream_health:{streamId} Redis Hash on
each publish cycle and computes healthScore + p95LatencyMs via
HealthScoreCalculator. Falls back to 50.0 + WARN log when the hash
is absent (TTL expired or producer not running) — SPEC-09 AC4.

Constructor receives HealthScoreCalculator; existing
SnapshotPublisherTest and SnapshotPublisherIT updated accordingly.

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 5 — Add unit and integration tests for SPEC-09

**Message:**
```
SPEC-09: add HealthScoreCalculatorTest and StreamHealthConsumerIT

HealthScoreCalculatorTest: 8 unit cases (perfect, mild buffer, high
latency, low bitrate, combined degradation, floor-at-0, frameDrop cap,
rounding). No infrastructure needed.

StreamHealthConsumerIT (Testcontainers Kafka + Redis): 3 cases verifying
Redis Hash population, degraded-event field values, and latest-write-wins
overwrite semantics.

SnapshotPublisherTest updated: constructor now takes HealthScoreCalculator;
two new tests verify fallback (50.0) and computed (80.0) health scores.

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/HealthScoreCalculatorTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/StreamHealthConsumerIT.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/HealthScoreCalculatorTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/StreamHealthConsumerIT.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java
```

---

## Commit 6 — Mark SPEC-09 Done with evidence

**Message:**
```
SPEC-09: mark Done, add evidence and commit plan

Updates spec status to Done, ticks all acceptance criteria checkboxes,
and records demo evidence (redis-cli output, test results, score
calculation verification) under section 10.

Refs: specs/SPEC-09-stream-health-pipeline.md
```

**Files:**
- specs/SPEC-09-stream-health-pipeline.md
- commits/SPEC-09.md

**Stage command:**
```bash
git add specs/SPEC-09-stream-health-pipeline.md \
        commits/SPEC-09.md
```

---

## Verification before pushing
- [ ] `mvn -f backend/pom.xml verify` — all 55+ tests pass, BUILD SUCCESS
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` — 21 tests, no lint errors, build succeeds
- [ ] Demo evidence in spec section 10 matches reality
- [ ] `redis-cli HGETALL stream_health:stream-001` returns all 5 fields after running the processor against the dev stack
