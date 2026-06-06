# SPEC-02: `streamflow-common` Module

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** SPEC-01

## 1. Goal
Provide a single source of truth for Kafka message contracts, enums, and constants shared across producer, processor, and API modules.

## 2. Context
All three runtime modules serialize/deserialize the same JSON payloads (`ViewerEvent`, `StreamHealthEvent`, `AlertEvent`, `StreamMetricSnapshot`). Putting them in `streamflow-common` avoids drift.

## 3. Requirements
### Functional
- R1. Package `com.streamflow.common.dto` contains immutable Java records (or Lombok `@Value`) for: `ViewerEventDTO`, `StreamHealthEventDTO`, `AlertEventDTO`, `StreamMetricSnapshotDTO` matching the JSON schemas in plan §5.
- R2. Package `com.streamflow.common.enums` contains: `EventType`, `AlertType`, `AlertSeverity`, `VideoQuality`. Each enum's `name()` matches the JSON values from §5.
- R3. Package `com.streamflow.common.constants` contains `KafkaTopics` with `public static final String VIEWER_EVENTS`, `STREAM_HEALTH`, `ALERTS`, `METRICS_AGGREGATED`.
- R4. DTOs are Jackson-serializable with snake- or camel-case (camel chosen, document it). `@JsonInclude(NON_NULL)` on optional fields (`bufferDurationMs`, `region`).
- R5. Module published as a normal jar (no Spring Boot repackage).

### Non-Functional
- NFR1. No Spring dependencies; only `jackson-annotations` (compile) and `jackson-databind` (test). Keeps the module reusable.

## 4. Design Notes
- Use Java 17 records to enforce immutability and free `equals/hashCode`.
- `VideoQuality` enum values: `Q_1080P("1080p")`, `Q_720P("720p")`, `Q_480P("480p")`, `Q_360P("360p")`, `Q_144P("144p")`. Store the wire string in a field and use `@JsonValue`/`@JsonCreator` for round-tripping.
- `AlertEventDTO.actualValue` is a `double`; `threshold` likewise.
- Timestamps are epoch millis (`long`).
- Camel-case field names are the JSON wire names (Jackson default, no `@JsonProperty` needed).
- `@JsonInclude(NON_NULL)` is placed at individual record component level; Jackson 2.16.x (from Spring Boot 3.2.5 BOM) honors component-level annotations on records.
- Open Question Q1 resolved: JSON only for MVP. Protobuf deferred.

## 5. Acceptance Criteria
- [x] AC1. `mvn -pl streamflow-common -am test` passes with at least one round-trip test per DTO (`objectMapper.readValue(objectMapper.writeValueAsBytes(dto), DTO.class).equals(dto)`).
- [x] AC2. `KafkaTopics` constants exactly match the strings in plan §6.
- [x] AC3. `VideoQuality.fromWire("720p") == Q_720P` and `objectMapper.writeValueAsString(Q_720P)` equals `"720p"`.

## 6. Tasks
1. Add `jackson-annotations` to module `pom.xml`.
2. Implement enums with `@JsonValue` / `@JsonCreator`.
3. Implement DTOs as records.
4. Add `KafkaTopics` constants class with `private` constructor.
5. Add JUnit 5 round-trip tests using shared `ObjectMapper`.

## 7. Test Plan
- Unit: round-trip JSON for each DTO (golden samples cribbed from plan §5).
- Unit: enum wire-format mapping.

## 8. Open Questions
- Q1. Add Protobuf later? **Resolved: JSON only for MVP.** Protobuf can be added as a follow-up spec if needed for performance.

## 9. Definition of Done
- [x] All ACs pass
- [x] Tests added under `src/test/java`
- [x] Module installed locally (`mvn install`) so downstream specs can depend on it

## 10. Evidence

### AC1 — `mvn -pl streamflow-common -am test` passes (15 tests, 0 failures)

```
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.streamflow.common.KafkaTopicsTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.037 s -- in com.streamflow.common.KafkaTopicsTest
[INFO] Running com.streamflow.common.DtoRoundTripTest
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.284 s -- in com.streamflow.common.DtoRoundTripTest
[INFO]
[INFO] Results:
[INFO]
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
[INFO]
[INFO] BUILD SUCCESS
[INFO] Total time:  3.883 s
```

Round-trip tests cover:
- `ViewerEventDTO` (all fields present)
- `ViewerEventDTO` (null optional fields omitted from JSON)
- `StreamHealthEventDTO`
- `AlertEventDTO`
- `StreamMetricSnapshotDTO`
- `VideoQuality` `@JsonValue` serialization (all 5 tiers)
- `VideoQuality` `@JsonCreator` deserialization
- `EventType`, `AlertSeverity`, `AlertType` enum name round-trips

### AC2 — `KafkaTopics` constants match plan §6

```java
// KafkaTopicsTest — all 4 assertions pass:
assertEquals("viewer-events",      KafkaTopics.VIEWER_EVENTS);
assertEquals("stream-health",      KafkaTopics.STREAM_HEALTH);
assertEquals("alerts",             KafkaTopics.ALERTS);
assertEquals("metrics-aggregated", KafkaTopics.METRICS_AGGREGATED);
```

### AC3 — `VideoQuality` wire-format

```java
// DtoRoundTripTest.videoQuality_fromWire_returns_correct_constant — passes:
assertEquals(VideoQuality.Q_720P,  VideoQuality.fromWire("720p"));
assertEquals(VideoQuality.Q_1080P, VideoQuality.fromWire("1080p"));
// ... all 5 tiers pass

// DtoRoundTripTest.videoQuality_jackson_serializes_to_wire_string — passes:
assertEquals("\"720p\"",  mapper.writeValueAsString(VideoQuality.Q_720P));
assertEquals("\"1080p\"", mapper.writeValueAsString(VideoQuality.Q_1080P));
// ... all 5 tiers pass
```

### Module installed to local Maven repo

```
$ mvn -f backend/pom.xml -pl streamflow-common -am install -q
(exit code 0 — no output = success)
```

Jar available at:
`~/.m2/repository/com/streamflow/streamflow-common/0.0.1-SNAPSHOT/streamflow-common-0.0.1-SNAPSHOT.jar`
