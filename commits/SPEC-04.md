# Commit Plan — SPEC-04: Viewer Event Consumer + Redis Sliding Window

Suggested branch: `feat/spec-04-viewer-event-consumer-redis-window`

---

## Commit 1 — Add processor module config: Kafka consumer, Redis, DLT producer

**Message:**
```
SPEC-04: add KafkaConsumerConfig, RedisConfig, DltKafkaConfig

Configure the streamflow-processor module for SPEC-04:
- KafkaConsumerConfig: manual-ack container factory (concurrency=3),
  JSON deserialiser trusted to com.streamflow.common.dto, DefaultErrorHandler
  with FixedBackOff(1s, 3 attempts) + DeadLetterPublishingRecoverer.
- RedisConfig: @Primary RedisTemplate<String,String> with
  StringRedisSerializer on all key/value/hash slots.
- DltKafkaConfig: minimal KafkaTemplate<String,Object> used exclusively
  by the DeadLetterPublishingRecoverer to publish to viewer-events.DLT.
- application.properties: Redis, Kafka, Cassandra (excluded until SPEC-17)
  and Actuator settings.

Refs: specs/SPEC-04-viewer-event-consumer-and-redis-window.md
```

**Files:**
- backend/streamflow-processor/pom.xml
- backend/streamflow-processor/src/main/resources/application.properties
- backend/streamflow-processor/src/main/java/com/streamflow/processor/StreamProcessorApplication.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/RedisConfig.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/DltKafkaConfig.java

**Stage command:**
```bash
git add backend/streamflow-processor/pom.xml \
        backend/streamflow-processor/src/main/resources/application.properties \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/StreamProcessorApplication.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/config/RedisConfig.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/config/DltKafkaConfig.java
```

---

## Commit 2 — Add ViewerCountAggregator, ViewerEventConsumer, StaleViewerEvictionTask

**Message:**
```
SPEC-04: add ViewerCountAggregator, consumer, and eviction scheduler

Core business logic for the Redis sliding-window viewer count:
- ViewerCountAggregator: ZADD on JOIN (score=eventTimestamp), ZREM on
  DROP, ZCARD for getLiveCount. TTL refreshed on every write (10 min).
  Tracks knownStreamIds for the eviction scheduler.
- ViewerEventConsumer: @KafkaListener on viewer-events; routes JOIN/DROP
  to the aggregator; manual-acks after Redis write; exceptions propagate
  to the error handler for retry+DLT.
- StaleViewerEvictionTask: @Scheduled(fixedDelay=10_000) removes members
  with score < (now - 5 min) via ZREMRANGEBYSCORE on all known streams.

Refs: specs/SPEC-04-viewer-event-consumer-and-redis-window.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/ViewerCountAggregator.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/scheduler/StaleViewerEvictionTask.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/aggregator/ViewerCountAggregator.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/scheduler/StaleViewerEvictionTask.java
```

---

## Commit 3 — Add unit and integration tests; extend IT await timeouts

**Message:**
```
SPEC-04: add ViewerCountAggregatorTest and ViewerEventConsumerIT

Tests cover all automated Acceptance Criteria:
- ViewerCountAggregatorTest (6 tests, Testcontainers Redis 7.2-alpine):
  AC1 1000 JOINs → ZCARD=1000; AC2 1000J+200D → 800; AC3 idempotency;
  eviction removes stale members; getLiveCount returns 0 for unknown
  stream; knownStreamIds tracked on join+drop.
- ViewerEventConsumerIT (3 tests, Testcontainers Kafka 7.5.3 + Redis):
  end-to-end AC1/AC2/AC3 via produce→consume→assert ZCARD.
  Await timeouts set to 60 s to accommodate Testcontainers overhead on
  developer laptops with Colima (single-partition topic, serial consumer).

Refs: specs/SPEC-04-viewer-event-consumer-and-redis-window.md
```

**Files:**
- backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/ViewerCountAggregatorTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/ViewerEventConsumerIT.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/test/java/com/streamflow/processor/aggregator/ViewerCountAggregatorTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/consumer/ViewerEventConsumerIT.java
```

---

## Commit 4 — Mark SPEC-04 Done; add evidence section

**Message:**
```
SPEC-04: mark Done, add evidence and commit plan

- specs/SPEC-04-*: Status → Done; ACs AC1/AC2/AC3 ticked; DoD ticked;
  §10 Evidence section added (test output, per-AC explanation, Q1 decision).
- commits/SPEC-04.md: this commit plan.

Refs: specs/SPEC-04-viewer-event-consumer-and-redis-window.md
```

**Files:**
- specs/SPEC-04-viewer-event-consumer-and-redis-window.md
- commits/SPEC-04.md

**Stage command:**
```bash
git add specs/SPEC-04-viewer-event-consumer-and-redis-window.md \
        commits/SPEC-04.md
```

---

## Verification before pushing

- [x] `DOCKER_HOST=unix:///~/.colima/default/docker.sock TESTCONTAINERS_RYUK_DISABLED=true mvn -f backend/streamflow-processor/pom.xml test` — 9 tests, 0 failures
- [ ] `mvn -f backend/pom.xml verify` (full multi-module build — run after SPEC-05/06 add the api module tests; processor alone is green)
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` (frontend not yet present — SPEC-07)
- [x] Demo evidence in spec §10 matches test output
