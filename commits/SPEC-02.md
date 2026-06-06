# Commit Plan — SPEC-02: streamflow-common Module

Suggested branch: `feat/spec-02-streamflow-common`

---

## Commit 1 — Add jackson-annotations dependency to streamflow-common pom

**Message:**
```
SPEC-02: add jackson-annotations compile dep to streamflow-common

jackson-annotations is needed for @JsonValue, @JsonCreator, and
@JsonInclude used by the VideoQuality enum and ViewerEventDTO.
jackson-databind was already present; annotations is split into its
own artifact and must be declared explicitly.

Refs: specs/SPEC-02-streamflow-common.md
```

**Files:**
- `backend/streamflow-common/pom.xml`

**Stage command:**
```bash
git add backend/streamflow-common/pom.xml
```

---

## Commit 2 — Add enums: EventType, AlertType, AlertSeverity, VideoQuality

**Message:**
```
SPEC-02: add EventType, AlertType, AlertSeverity, VideoQuality enums

EventType, AlertType, AlertSeverity use default Jackson name()
serialization (no custom annotation needed). VideoQuality stores a
wire string ("1080p" etc.) and uses @JsonValue/@JsonCreator so the
JSON payloads are human-readable without knowing ordinals.

Refs: specs/SPEC-02-streamflow-common.md
```

**Files:**
- `backend/streamflow-common/src/main/java/com/streamflow/common/enums/EventType.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/enums/AlertType.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/enums/AlertSeverity.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/enums/VideoQuality.java`

**Stage command:**
```bash
git add backend/streamflow-common/src/main/java/com/streamflow/common/enums/EventType.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/enums/AlertType.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/enums/AlertSeverity.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/enums/VideoQuality.java
```

---

## Commit 3 — Add KafkaTopics constants class

**Message:**
```
SPEC-02: add KafkaTopics constants (viewer-events, stream-health,
alerts, metrics-aggregated)

Constant strings match plan §6 exactly. Private constructor prevents
instantiation. All downstream modules will reference these constants
rather than inlining string literals.

Refs: specs/SPEC-02-streamflow-common.md
```

**Files:**
- `backend/streamflow-common/src/main/java/com/streamflow/common/constants/KafkaTopics.java`

**Stage command:**
```bash
git add backend/streamflow-common/src/main/java/com/streamflow/common/constants/KafkaTopics.java
```

---

## Commit 4 — Add DTOs as Java 17 records

**Message:**
```
SPEC-02: add ViewerEventDTO, StreamHealthEventDTO, AlertEventDTO,
StreamMetricSnapshotDTO as Java 17 records

Records give free equals/hashCode/toString and enforce immutability.
Camel-case field names are the JSON wire names (Jackson default).
bufferDurationMs and region on ViewerEventDTO are @JsonInclude(NON_NULL)
so they are absent from JSON when null, matching NFR R4.

Refs: specs/SPEC-02-streamflow-common.md
```

**Files:**
- `backend/streamflow-common/src/main/java/com/streamflow/common/dto/ViewerEventDTO.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/dto/StreamHealthEventDTO.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/dto/AlertEventDTO.java`
- `backend/streamflow-common/src/main/java/com/streamflow/common/dto/StreamMetricSnapshotDTO.java`

**Stage command:**
```bash
git add backend/streamflow-common/src/main/java/com/streamflow/common/dto/ViewerEventDTO.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/dto/StreamHealthEventDTO.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/dto/AlertEventDTO.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/dto/StreamMetricSnapshotDTO.java
```

---

## Commit 5 — Add JUnit 5 round-trip tests; update spec to Done

**Message:**
```
SPEC-02: add round-trip tests for all DTOs and enums; mark spec Done

DtoRoundTripTest (11 tests): JSON round-trip for each DTO; verifies
@JsonInclude(NON_NULL) omits null optional fields; verifies VideoQuality
wire-format serialization and deserialization (AC3).

KafkaTopicsTest (4 tests): asserts all topic name constants match
plan §6 exactly (AC2).

All 15 tests pass. Module installed to local Maven repo so downstream
specs (SPEC-03+) can declare it as a compile dependency.

Refs: specs/SPEC-02-streamflow-common.md
```

**Files:**
- `backend/streamflow-common/src/test/java/com/streamflow/common/DtoRoundTripTest.java`
- `backend/streamflow-common/src/test/java/com/streamflow/common/KafkaTopicsTest.java`
- `specs/SPEC-02-streamflow-common.md`
- `commits/SPEC-02.md`

**Stage command:**
```bash
git add backend/streamflow-common/src/test/java/com/streamflow/common/DtoRoundTripTest.java \
        backend/streamflow-common/src/test/java/com/streamflow/common/KafkaTopicsTest.java \
        specs/SPEC-02-streamflow-common.md \
        commits/SPEC-02.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml -pl streamflow-common -am test` — 15 tests, 0 failures
- [ ] `mvn -f backend/pom.xml -DskipTests package` — full backend builds without errors
- [ ] `mvn -f backend/pom.xml -pl streamflow-common -am install` — jar installed to local repo
- [ ] Demo evidence in `specs/SPEC-02-streamflow-common.md` §10 matches the commands above
