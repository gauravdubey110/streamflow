# Commit Plan — SPEC-18: History API + Service

Suggested branch: feat/spec-18-history-api-and-service

## Commit 1 — Enable Cassandra in api module and add CassandraConfig

**Message:**
```
SPEC-18: enable Cassandra in api gateway and add CassandraConfig

Remove the spring.autoconfigure.exclude block for Cassandra from the
main application.properties (it was a placeholder until SPEC-17/SPEC-18).
Add a CassandraConfig that exposes CassandraTemplate with the same
@ConditionalOnBean(CqlSession) guard used in the processor module.
Add test/resources/application.properties to re-exclude Cassandra for
tests that don't need it (mirrors the processor module's pattern).

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- backend/streamflow-api/src/main/resources/application.properties
- backend/streamflow-api/src/main/java/com/streamflow/api/config/CassandraConfig.java
- backend/streamflow-api/src/test/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-api/src/main/resources/application.properties \
        backend/streamflow-api/src/main/java/com/streamflow/api/config/CassandraConfig.java \
        backend/streamflow-api/src/test/resources/application.properties
```

---

## Commit 2 — Add BucketHelper, entities, and read repositories

**Message:**
```
SPEC-18: add BucketHelper, read-side entities, and Cassandra repos

BucketHelper: enumerates hourly/daily bucket strings spanning a time
range — used to drive partition-key queries without ALLOW FILTERING.

MetricSnapshotEntity and AlertEntity: read-side mirrors of the
processor entities living in the api module's own package so the
api module doesn't depend on processor internals.

MetricSnapshotReadRepository: single-partition range query against
metric_snapshots (partitioned by stream_id only).

AlertReadRepository: per-bucket queries against alerts table
(partitioned by stream_id, date_bucket), merged in-memory.

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/repository/BucketHelper.java
- backend/streamflow-api/src/main/java/com/streamflow/api/repository/MetricSnapshotEntity.java
- backend/streamflow-api/src/main/java/com/streamflow/api/repository/AlertEntity.java
- backend/streamflow-api/src/main/java/com/streamflow/api/repository/MetricSnapshotReadRepository.java
- backend/streamflow-api/src/main/java/com/streamflow/api/repository/AlertReadRepository.java

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/repository/BucketHelper.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/repository/MetricSnapshotEntity.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/repository/AlertEntity.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/repository/MetricSnapshotReadRepository.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/repository/AlertReadRepository.java
```

---

## Commit 3 — Add HistoryService, HistoryRangeException, and GlobalExceptionHandler update

**Message:**
```
SPEC-18: add HistoryService with range validation and downsampling

HistoryService orchestrates bucket-spanning reads, sorts results
ascending (metric snapshots) or descending (alerts), applies HOUR
downsampling (every 60th row), caps at 1500 points, and filters
alerts by severity in-memory.

HistoryRangeException: thrown when to-from exceeds max-range-hours.
GlobalExceptionHandler: new handler maps HistoryRangeException to
HTTP 400 problem-detail JSON with maxRangeHours and actual span.

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/service/HistoryService.java
- backend/streamflow-api/src/main/java/com/streamflow/api/exception/HistoryRangeException.java
- backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/service/HistoryService.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/exception/HistoryRangeException.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java
```

---

## Commit 4 — Add HistoryController with ETag and Cache-Control

**Message:**
```
SPEC-18: add HistoryController with ETag and Cache-Control support

GET /api/v1/streams/{streamId}/history: returns List<StreamMetricSnapshotDTO>
GET /api/v1/streams/{streamId}/alerts: returns List<AlertEventDTO>

Both endpoints:
- Set Cache-Control: max-age=30
- Compute weak ETag from SHA-256(streamId:from:to:variant:resultCount)[0..16]
- Return 304 when If-None-Match header matches current ETag

@ConditionalOnBean(CassandraOperations) ensures controller is absent
when Cassandra is excluded (existing tests unaffected).

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/controller/HistoryController.java

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/controller/HistoryController.java
```

---

## Commit 5 — Add tests and update api pom.xml

**Message:**
```
SPEC-18: add unit tests, integration test, and Cassandra test dep

BucketHelperTest: 9 tests covering hourly/daily bucket enumeration.
HistoryServiceTest: 13 tests covering range validation, ordering,
  HOUR downsampling, MAX_POINTS cap, severity filter, from==to edge.
HistoryControllerTest: 5 tests covering ETag properties.
HistoryControllerIT: Testcontainer-based IT seeding 30 snapshots and
  8 alerts, verifying AC1–AC4 (requires Docker).

pom.xml: add testcontainers:cassandra test dependency.

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- backend/streamflow-api/pom.xml
- backend/streamflow-api/src/test/java/com/streamflow/api/repository/BucketHelperTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/service/HistoryServiceTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/controller/HistoryControllerTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/controller/HistoryControllerIT.java

**Stage command:**
```bash
git add backend/streamflow-api/pom.xml \
        backend/streamflow-api/src/test/java/com/streamflow/api/repository/BucketHelperTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/service/HistoryServiceTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/controller/HistoryControllerTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/controller/HistoryControllerIT.java
```

---

## Commit 6 — Update SPEC-18 to Done with evidence

**Message:**
```
SPEC-18: mark spec Done, add evidence section and commit plan

All ACs verified by unit tests (38 tests, 0 failures).
HistoryControllerIT added for full Cassandra integration validation.

Refs: specs/SPEC-18-history-api-and-service.md
```

**Files:**
- specs/SPEC-18-history-api-and-service.md
- commits/SPEC-18.md

**Stage command:**
```bash
git add specs/SPEC-18-history-api-and-service.md \
        commits/SPEC-18.md
```

---

## Verification before pushing
- [ ] `mvn -f backend/pom.xml clean install -DskipTests` — BUILD SUCCESS
- [ ] `mvn -f backend/streamflow-api/pom.xml test -Dtest="BucketHelperTest,HistoryServiceTest,HistoryControllerTest,ChaosControllerTest,AlertPushConsumerTest,CircuitBreakerPushConsumerTest"` — 38 tests, 0 failures
- [ ] `mvn -f backend/streamflow-api/pom.xml test -Dtest="HistoryControllerIT"` — requires Docker + internet access to pull cassandra:4.1
- [ ] Demo evidence in spec matches reality
