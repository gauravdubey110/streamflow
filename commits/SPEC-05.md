# Commit Plan — SPEC-05: Snapshot Publisher

Suggested branch: `feat/spec-05-snapshot-publisher`

---

## Commit 1 — Add SnapshotKafkaConfig and ObjectMapper bean

**Message:**
```
SPEC-05: add SnapshotKafkaConfig with KafkaTemplate and ObjectMapper

Configure the snapshot producer pipeline for SPEC-05:
- SnapshotKafkaConfig: dedicated ProducerFactory<String,StreamMetricSnapshotDTO>
  with JsonSerializer, ADD_TYPE_INFO_HEADERS=false (wire-format clean),
  ENABLE_IDEMPOTENCE=true / ACKS=all for at-least-once delivery.
- KafkaTemplate<String,StreamMetricSnapshotDTO> bean named snapshotKafkaTemplate
  to avoid ambiguity with DltKafkaConfig's kafkaTemplate<String,Object>.
- ObjectMapper bean declared here because the processor uses spring-boot-starter
  (not -web) so JacksonAutoConfiguration is not active.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/SnapshotKafkaConfig.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/config/SnapshotKafkaConfig.java
```

---

## Commit 2 — Add SnapshotPublisher scheduled bean

**Message:**
```
SPEC-05: add SnapshotPublisher with @Scheduled, Redis write, Kafka publish

Core SPEC-05 business logic:
- @Scheduled(fixedRate=1000): discovers active streams via SMEMBERS
  active_streams; iterates and calls publishSnapshotForStream per stream.
- publishSnapshotForStream: ZCARD → liveViewerCount; delta = current -
  previous (ConcurrentHashMap); SET stream_snapshot:{id} <json> EX 30;
  KafkaTemplate.send to metrics-aggregated keyed by streamId.
- R5: WARN log if entire cycle exceeds 800ms.
- Micrometer Timer wraps the scheduler body (streamflow.snapshot.publish).
- Scaling note in Javadoc: SSCAN + parallel scheduler needed beyond 10 streams.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 3 — Add active_streams tracking to ViewerEventConsumer

**Message:**
```
SPEC-05: track active_streams in ViewerEventConsumer (SADD + EXPIRE)

On every JOIN or DROP event, call trackActiveStream(streamId) which:
- SADD active_streams <streamId>  (idempotent)
- EXPIRE active_streams 300       (refreshed on every event so active
  streams never expire while traffic keeps flowing; expiry = 5 min per spec)
This is the registration mechanism that SnapshotPublisher reads via SMEMBERS.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java
```

---

## Commit 4 — Add unit and integration tests for SnapshotPublisher

**Message:**
```
SPEC-05: add SnapshotPublisherTest and SnapshotPublisherIT

SnapshotPublisherTest (6 unit tests, all deps mocked):
- AC1-proxy: Redis SET called per stream with correct key + 30 s TTL.
- AC2-proxy: Kafka send called per stream with correct topic + key.
- AC4: viewerDelta computed correctly across two successive calls
  (1000 → 1200 yields delta=200).
- Snapshot JSON contains streamId, liveViewerCount, circuitBreakerState.
- Empty active_streams → no Redis/Kafka writes.
- Null SMEMBERS result → no Redis/Kafka writes.

SnapshotPublisherIT (2 integration tests, Testcontainers Kafka + Redis):
- ac1_and_ac2: produce 50 JOINs for 3 streams, await Redis snapshot keys
  and metrics-aggregated Kafka messages (15 s timeout with 500 ms poll).
- ac4: produce 100 JOINs, await Redis snapshot, parse JSON and assert
  streamId, liveViewerCount > 0, snapshotTs > 0, circuitBreakerState=CLOSED.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherIT.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherIT.java
```

---

## Commit 5 — Pass DOCKER_HOST through Surefire for Colima compatibility

**Message:**
```
SPEC-05: pass DOCKER_HOST and ryuk-disabled flag through Surefire

Without DOCKER_HOST in the forked test JVM, Testcontainers cannot find the
Colima Docker socket (unix:///~/.colima/default/docker.sock) and fails to
start the ryuk reaper, aborting all Testcontainers-based tests.

Add <environmentVariables> to maven-surefire-plugin in streamflow-processor/
pom.xml:
  DOCKER_HOST=${env.DOCKER_HOST}          -- inherits from shell (Colima/Desktop)
  TESTCONTAINERS_RYUK_DISABLED=true       -- matches ~/.testcontainers.properties

This makes `mvn verify` work without exporting DOCKER_HOST manually.
Standard Docker Desktop installs are unaffected because DOCKER_HOST is empty
and Testcontainers falls back to /var/run/docker.sock.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- backend/streamflow-processor/pom.xml

**Stage command:**
```bash
git add backend/streamflow-processor/pom.xml
```

---

## Commit 6 — Mark SPEC-05 Done; add evidence and commit plan

**Message:**
```
SPEC-05: mark Done, add evidence and commit plan

- specs/SPEC-05-*: Status → Done; all ACs ticked; DoD ticked;
  §10 Evidence section added (build output, per-AC curl/redis-cli/kafka output,
  Q1 decision rationale).
- commits/SPEC-05.md: this commit plan.

Refs: specs/SPEC-05-snapshot-publisher.md
```

**Files:**
- specs/SPEC-05-snapshot-publisher.md
- commits/SPEC-05.md

**Stage command:**
```bash
git add specs/SPEC-05-snapshot-publisher.md \
        commits/SPEC-05.md
```

---

## Verification before pushing

- [x] `DOCKER_HOST="unix:///Users/zop2285/.colima/default/docker.sock" mvn -f backend/pom.xml verify -pl streamflow-processor -am` — 17 tests, 0 failures, BUILD SUCCESS
- [ ] `mvn -f backend/pom.xml verify` (full multi-module build — api module tests not yet present; added in SPEC-06)
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` (frontend not yet present — SPEC-07)
- [x] Demo evidence in spec §10 matches reality (Redis redis-cli output, Kafka offset check, Kafka consumer output)
