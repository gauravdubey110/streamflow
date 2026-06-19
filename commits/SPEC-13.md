# Commit Plan — SPEC-13: Chaos Injector + Chaos REST API

Suggested branch: feat/spec-13-chaos-injector

---

## Commit 1 — Add ChaosInjector, scenarios, and ChaosAwareStrategy to producer

**Message:**
```
SPEC-13: add ChaosInjector, ChaosScenario enum, ChaosState record

Implements SPEC-13 R1/R2: ChaosInjector tracks active chaos sessions
per stream in a ConcurrentHashMap, schedules auto-revert via
ScheduledFuture, and exposes activeScenario(streamId) for strategy
lookup. ChaosAwareStrategy (decorator over NormalLoadStrategy) applies
scenario modifiers: VIEWER_DROP doubles DROP ratio, HIGH_BUFFER raises
BUFFER_START to 12%, BITRATE_SPIKE delegates to health producer,
STREAM_DOWN returns null to suppress emission.

Refs: specs/SPEC-13-chaos-injector.md
```

**Files:**
- backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosScenario.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosState.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosInjector.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/ChaosAwareStrategy.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosScenario.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosState.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/chaos/ChaosInjector.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/ChaosAwareStrategy.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java
```

---

## Commit 2 — Add InternalChaosController to producer (R3)

**Message:**
```
SPEC-13: add InternalChaosController to producer (POST/DELETE /internal/chaos)

Implements SPEC-13 R3/R6: exposes POST /internal/chaos (returns 202)
and DELETE /internal/chaos/{chaosId} (204/404) on port 8081. Bean
Validation enforces scenario enum, streamId not-blank, and
durationSeconds 1-300. Adds spring-boot-starter-validation dependency.

Refs: specs/SPEC-13-chaos-injector.md
```

**Files:**
- backend/streamflow-producer/src/main/java/com/streamflow/producer/rest/InternalChaosController.java
- backend/streamflow-producer/pom.xml

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/rest/InternalChaosController.java \
        backend/streamflow-producer/pom.xml
```

---

## Commit 3 — Add ChaosController + ProducerChaosClient to API gateway (R4/R5)

**Message:**
```
SPEC-13: add ChaosController and ProducerChaosClient to API gateway

Implements SPEC-13 R4/R5: ChaosController forwards POST/DELETE to the
producer via ProducerChaosClient (RestTemplate). Producer base URL is
configurable via streamflow.producer.base-url (default localhost:8081).
GlobalExceptionHandler extended with MethodArgumentNotValidException
and HttpMessageNotReadableException handlers for R6 validation coverage.

Refs: specs/SPEC-13-chaos-injector.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosScenarioDTO.java
- backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosRequest.java
- backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosResponse.java
- backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ProducerChaosClient.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/ChaosClientConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/controller/ChaosController.java
- backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java
- backend/streamflow-api/src/main/resources/application.properties
- backend/streamflow-api/pom.xml

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosScenarioDTO.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosRequest.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ChaosResponse.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/chaos/ProducerChaosClient.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/config/ChaosClientConfig.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/controller/ChaosController.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java \
        backend/streamflow-api/src/main/resources/application.properties \
        backend/streamflow-api/pom.xml
```

---

## Commit 4 — Add unit and slice tests for chaos components

**Message:**
```
SPEC-13: add unit tests for ChaosInjector, ChaosAwareStrategy, and controllers

26 producer tests + 6 API ChaosControllerTest tests pass. Covers:
start/cancel/auto-revert, scenario modifiers (DROP rate > 40%,
BUFFER rate > 8%, STREAM_DOWN → null, BITRATE_SPIKE → delegate),
HTTP 202/204/404/400 semantics on both internal and public endpoints.

Refs: specs/SPEC-13-chaos-injector.md
```

**Files:**
- backend/streamflow-producer/src/test/java/com/streamflow/producer/chaos/ChaosInjectorTest.java
- backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/ChaosAwareStrategyTest.java
- backend/streamflow-producer/src/test/java/com/streamflow/producer/rest/InternalChaosControllerTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/controller/ChaosControllerTest.java

**Stage command:**
```bash
git add backend/streamflow-producer/src/test/java/com/streamflow/producer/chaos/ChaosInjectorTest.java \
        backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/ChaosAwareStrategyTest.java \
        backend/streamflow-producer/src/test/java/com/streamflow/producer/rest/InternalChaosControllerTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/controller/ChaosControllerTest.java
```

---

## Commit 5 — Update spec to Done and add evidence

**Message:**
```
SPEC-13: mark Done, add evidence and commit plan

All ACs verified: 202 on POST, 204 on DELETE, 404 on unknown id,
400 on invalid input. bufferRatePct rising trend observed live.
Auto-revert verified by unit test (1s duration).

Refs: specs/SPEC-13-chaos-injector.md
```

**Files:**
- specs/SPEC-13-chaos-injector.md
- commits/SPEC-13.md

**Stage command:**
```bash
git add specs/SPEC-13-chaos-injector.md commits/SPEC-13.md
```

---

## Verification before pushing
- [ ] `mvn -f backend/streamflow-producer/pom.xml test` — 26 tests pass
- [ ] `mvn -f backend/streamflow-api/pom.xml test` — 12 tests pass (StreamApiIT + ChaosControllerTest)
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="HealthScoreCalculatorTest,QualityDistAggregatorTest,BitrateDegradationRuleTest,HighBufferRateRuleTest,ViewerDropRuleTest,SnapshotPublisherTest,CircuitBreakerStateListenerTest"` — 72 tests pass
- [ ] Demo evidence in spec matches reality
- [ ] Start services locally: `mvn -f backend/streamflow-producer/pom.xml spring-boot:run` and verify `curl -X POST http://localhost:8081/internal/chaos ...` returns 202
