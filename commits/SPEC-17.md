# Commit Plan — SPEC-17: Cassandra Schema + Write Repositories

Suggested branch: `feat/spec-17-cassandra-schema-and-repositories`

---

## Commit 1 — Add Cassandra to dev docker-compose with init.cql

**Message:**
```
SPEC-17: add Cassandra 4.1 + schema init to docker-compose.dev.yml

Add cassandra (4.1) service with health check and a one-shot
cassandra-init container that applies infra/cassandra/init.cql on
startup. Schema matches plan §8 exactly: viewer_events (hourly
buckets, TTL 7d), metric_snapshots (TTL 30d), alerts (daily buckets,
TTL 90d). SimpleStrategy RF=1 for local dev.

Refs: specs/SPEC-17-cassandra-schema-and-repositories.md
```

**Files:**
- `infra/cassandra/init.cql`
- `infra/docker-compose.dev.yml`

**Stage command:**
```bash
git add infra/cassandra/init.cql infra/docker-compose.dev.yml
```

---

## Commit 2 — Add Cassandra config, entities, and write repositories

**Message:**
```
SPEC-17: add Spring Data Cassandra config, entities, and write repos

Add CassandraConfig (@ConditionalOnBean(CqlSession)) exposing a
CassandraTemplate bean. Add three @Table entities matching the DDL:
ViewerEventEntity, MetricSnapshotEntity, AlertEntity.

Add three async write repositories (CompletableFuture + Semaphore
backpressure, MAX_IN_FLIGHT = 256/32/32):
- CassandraViewerEventRepository: hourly date_bucket, persist()
- CassandraMetricSnapshotRepository: once-per-minute gate, persist()
- CassandraAlertRepository: daily date_bucket, persist()

All repos are @ConditionalOnBean(CassandraOperations.class) so they
are skipped when Cassandra auto-config is excluded (tests, dev without
Cassandra). application.properties updated to re-enable Cassandra
auto-config (removed exclusion added in SPEC-04).

Refs: specs/SPEC-17-cassandra-schema-and-repositories.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/config/CassandraConfig.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/ViewerEventEntity.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/MetricSnapshotEntity.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/AlertEntity.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraViewerEventRepository.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraMetricSnapshotRepository.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraAlertRepository.java`
- `backend/streamflow-processor/src/main/resources/application.properties`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/config/CassandraConfig.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/ViewerEventEntity.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/MetricSnapshotEntity.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/AlertEntity.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraViewerEventRepository.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraMetricSnapshotRepository.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/persistence/CassandraAlertRepository.java \
  backend/streamflow-processor/src/main/resources/application.properties
```

---

## Commit 3 — Wire Cassandra writes into existing consumers and SnapshotPublisher

**Message:**
```
SPEC-17: wire Cassandra writes into ViewerEventConsumer and SnapshotPublisher

ViewerEventConsumer: inject Optional<CassandraViewerEventRepository>
and call repo.persist(event) for every viewer event (async, no-op when
Cassandra is absent).

SnapshotPublisher: inject Optional<CassandraMetricSnapshotRepository>
and call repo.persist(snapshot) after each publish cycle. The repo
gates writes to once per minute per stream.

AlertCassandraConsumer: new @KafkaListener on 'alerts' topic (group
alert-cassandra-group) writes alerts to CassandraAlertRepository.
KafkaConsumerConfig: add alertConsumerFactory + alertListenerContainerFactory.

Refs: specs/SPEC-17-cassandra-schema-and-repositories.md
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/AlertCassandraConsumer.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/AlertCassandraConsumer.java \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/config/KafkaConsumerConfig.java
```

---

## Commit 4 — Add Cassandra IT and bucket-helper unit tests

**Message:**
```
SPEC-17: add CassandraRepositoryIT and BucketHelperTest

CassandraRepositoryIT: Testcontainers-based integration test covering
all three write repositories. Tests: ac1 (1000 viewer events, count >=
1000), ac1b (DESC ordering verified), ac2 (one snapshot per minute),
ac3 (alert persisted and queryable by date_bucket), plus bucket-format
assertions.

BucketHelperTest: 6 pure unit tests for CassandraViewerEventRepository
.hourBucket() and CassandraAlertRepository.dayBucket() — no Docker
required.

Test profile application.properties: excludes Cassandra auto-config by
default so pre-existing ITs (ViewerEventConsumerIT, SnapshotPublisherIT)
continue without a Cassandra container.

Fix SnapshotPublisherTest: add Optional.empty() for the new
CassandraMetricSnapshotRepository constructor parameter.

Refs: specs/SPEC-17-cassandra-schema-and-repositories.md
```

**Files:**
- `backend/streamflow-processor/src/test/resources/application.properties`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/CassandraRepositoryIT.java`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/BucketHelperTest.java`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java`

**Stage command:**
```bash
git add \
  backend/streamflow-processor/src/test/resources/application.properties \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/CassandraRepositoryIT.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/persistence/BucketHelperTest.java \
  backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java
```

---

## Commit 5 — Mark SPEC-17 Done + commit plan

**Message:**
```
SPEC-17: mark spec Done, add evidence section and commit plan

Refs: specs/SPEC-17-cassandra-schema-and-repositories.md
```

**Files:**
- `specs/SPEC-17-cassandra-schema-and-repositories.md`
- `commits/SPEC-17.md`

**Stage command:**
```bash
git add specs/SPEC-17-cassandra-schema-and-repositories.md commits/SPEC-17.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml verify -Dmaven.test.failure.ignore=true` — all modules build; unit tests pass; Testcontainers ITs need Docker running
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="BucketHelperTest,SnapshotPublisherTest,HealthScoreCalculatorTest"` — 25+ tests pass without Docker
- [ ] With Docker running: `mvn -f backend/streamflow-processor/pom.xml test -Dtest="CassandraRepositoryIT"` — Testcontainers IT passes (AC1, AC2, AC3)
- [ ] Demo evidence in spec matches reality
