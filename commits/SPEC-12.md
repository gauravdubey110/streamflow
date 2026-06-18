# Commit Plan — SPEC-12: Resilience4j Circuit Breaker on Alert Engine

Suggested branch: feat/spec-12-resilience4j-circuit-breaker

---

## Commit 1 — Add AOP + Resilience4j micrometer deps and CB config

**Message:**
```
SPEC-12: add spring-boot-starter-aop, resilience4j-micrometer, and CB config

Add spring-boot-starter-aop (required for @CircuitBreaker annotation proxying)
and resilience4j-micrometer (Micrometer bridge for Actuator/Prometheus metrics)
to streamflow-processor. Configure the alertProcessor circuit breaker in
application.properties: COUNT_BASED window=20, min-calls=10, failure-rate=50%,
wait=30s, half-open=3 probes. Enable the health indicator for AC4.

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- backend/streamflow-processor/pom.xml
- backend/streamflow-processor/src/main/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-processor/pom.xml \
        backend/streamflow-processor/src/main/resources/application.properties
```

---

## Commit 2 — Add CircuitBreakerStateEvent and AlertProcessorCircuitBreaker

**Message:**
```
SPEC-12: add CB event type and Redis/Spring-event listener

CircuitBreakerStateEvent: Spring ApplicationEvent carrying previousState,
currentState, reason, and occurredAt (named to avoid clash with
ApplicationEvent's final getTimestamp() method).

AlertProcessorCircuitBreaker: @PostConstruct registers an onStateTransition
listener that (a) writes SET cb_state:alert_processor <STATE> EX 300 to Redis
(R5) and (b) publishes CircuitBreakerStateEvent on ApplicationEventPublisher
(R6). Also writes initial CLOSED state on startup. Exposes getCurrentState()
for SnapshotPublisher (R7).

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateEvent.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/AlertProcessorCircuitBreaker.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateEvent.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/AlertProcessorCircuitBreaker.java
```

---

## Commit 3 — Annotate AlertPublisher with @CircuitBreaker + fallback

**Message:**
```
SPEC-12: wrap AlertPublisher.publish() with @CircuitBreaker and fallback

Add @CircuitBreaker(name="alertProcessor", fallbackMethod="publishFallback")
to AlertPublisher.publish(). The fallback logs WARN and stores the dropped
alert ID in Redis list dropped_alerts:{streamId}, capped at 100 via LTRIM (R4).
Inject RedisTemplate into AlertPublisher for fallback use. Make DROPPED_KEY_PREFIX
and DROPPED_ALERTS_MAX_SIZE public constants for test access.

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertPublisher.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertPublisher.java
```

---

## Commit 4 — Wire CB state into SnapshotPublisher (R7)

**Message:**
```
SPEC-12: replace hard-coded "CLOSED" in SnapshotPublisher with real CB state

Inject AlertProcessorCircuitBreaker into SnapshotPublisher. Replace the
placeholder "CLOSED" string with alertProcessorCircuitBreaker.getCurrentState()
which reads from the local Resilience4j registry (zero-latency, no Redis
round-trip per snapshot cycle). Update Javadoc.

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 5 — Add SPEC-12 tests

**Message:**
```
SPEC-12: add unit and integration tests for circuit breaker

CircuitBreakerStateListenerTest (unit): programmatically trip the CB using
a fast config (window=4, min=2, threshold=50%); assert Redis SET is called
with OPEN state; assert CircuitBreakerStateEvent is published; assert
getCurrentState() returns correct string.

AlertPublisherCircuitBreakerIT (integration): stub alertKafkaTemplate to throw
on every send; call publish() 12 times; assert cb_state:alert_processor = OPEN
in Redis; assert dropped_alerts list is non-empty. Second test: call fallback()
110 times and assert list is capped at 100.

SnapshotPublisherTest: add two new tests verifying circuitBreakerState field
reflects OPEN and HALF_OPEN from the mocked CB. Update constructor to include
AlertProcessorCircuitBreaker mock.

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateListenerTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/AlertPublisherCircuitBreakerIT.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateListenerTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/AlertPublisherCircuitBreakerIT.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/snapshot/SnapshotPublisherTest.java
```

---

## Commit 6 — Update SPEC-12 to Done + add commit plan

**Message:**
```
SPEC-12: mark Done, add evidence and commit plan

Update spec status to Done, tick all ACs and DoD checkboxes, add evidence
section with log excerpts and test results. Add commits/SPEC-12.md. Document
deviations: single Redis key (not per-stream), getOccurredAt() naming,
spring-boot-starter-aop dependency addition.

Refs: specs/SPEC-12-resilience4j-circuit-breaker.md
```

**Files:**
- specs/SPEC-12-resilience4j-circuit-breaker.md
- commits/SPEC-12.md

**Stage command:**
```bash
git add specs/SPEC-12-resilience4j-circuit-breaker.md \
        commits/SPEC-12.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="CircuitBreakerStateListenerTest,AlertPublisherCircuitBreakerIT,SnapshotPublisherTest"` → 18 tests, 0 failures
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="*Test,AlertPublisherCircuitBreakerIT,AlertEngineIT,CircuitBreakerStateListenerTest"` → 84 tests, 0 failures
- [ ] `mvn -f backend/streamflow-processor/pom.xml test -Dtest="SnapshotPublisherIT"` → 2 tests, 0 failures (run in isolation)
- [ ] Demo evidence in spec matches reality
