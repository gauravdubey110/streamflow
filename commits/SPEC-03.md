# Commit Plan — SPEC-03: Viewer Event Producer @ 1k TPS

Suggested branch: `feat/spec-03-viewer-event-producer`

---

## Commit 1 — Configure pom.xml and application.yml for producer

**Message:**
```
SPEC-03: configure streamflow-producer pom and application.yml

Add spring-kafka, spring-boot-starter-actuator, testcontainers-kafka
dependencies. Add application.yml with server port 8081, Actuator
health exposure, and three default stream definitions producing 1000
TPS total. Deletes the old empty application.properties.

Refs: specs/SPEC-03-viewer-event-producer.md
```

**Files:**
- `backend/streamflow-producer/pom.xml`
- `backend/streamflow-producer/src/main/resources/application.yml`
- ~~`backend/streamflow-producer/src/main/resources/application.properties`~~ (deleted)

**Stage command:**
```bash
git add backend/streamflow-producer/pom.xml \
        backend/streamflow-producer/src/main/resources/application.yml
git rm  backend/streamflow-producer/src/main/resources/application.properties
```

---

## Commit 2 — Add SimulationConfig and KafkaProducerConfig

**Message:**
```
SPEC-03: add SimulationConfig and KafkaProducerConfig beans

SimulationConfig (@ConfigurationProperties) binds tps, enabled, and
per-stream definitions from application.yml. KafkaProducerConfig
creates a DefaultKafkaProducerFactory with acks=1, linger.ms=10,
batch.size=32768, compression.type=lz4 as required by R5.

Refs: specs/SPEC-03-viewer-event-producer.md
```

**Files:**
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/config/SimulationConfig.java`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/config/KafkaProducerConfig.java`

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/config/SimulationConfig.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/config/KafkaProducerConfig.java
```

---

## Commit 3 — Add EventGenerationStrategy and NormalLoadStrategy

**Message:**
```
SPEC-03: add EventGenerationStrategy interface and NormalLoadStrategy

NormalLoadStrategy produces 70% JOIN / 25% DROP / 5% QUALITY_SWITCH
using a pre-built weighted list for O(1) selection (R4). Quality and
region are drawn from uniform/weighted distributions. Interface is the
extensibility hook for SPEC-13 SpikeLoadStrategy.

Refs: specs/SPEC-03-viewer-event-producer.md
```

**Files:**
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/EventGenerationStrategy.java`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/NormalLoadStrategy.java`

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/EventGenerationStrategy.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/strategy/NormalLoadStrategy.java
```

---

## Commit 4 — Add ViewerEventProducer, StreamSimulator, and health indicator

**Message:**
```
SPEC-03: add ViewerEventProducer, StreamSimulator, health indicator

ViewerEventProducer: one ScheduledExecutorService per stream emitting
at perStreamTps using microsecond-precision scheduleAtFixedRate. Tracks
last-publish timestamp for health reporting (R3, R6, R7).

StreamSimulator: @PostConstruct starts producers; @PreDestroy stops
them within 5s each (R6). Skips all producers when enabled=false (AC4).

KafkaProducerHealthIndicator: reports UP only when ≥1 event published
in last 10s across any stream (R7, AC3).

StreamProducerApplication updated to @EnableConfigurationProperties.

Refs: specs/SPEC-03-viewer-event-producer.md
```

**Files:**
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamSimulator.java`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/health/KafkaProducerHealthIndicator.java`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/StreamProducerApplication.java`

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamSimulator.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/health/KafkaProducerHealthIndicator.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/StreamProducerApplication.java
```

---

## Commit 5 — Add unit and integration tests; mark spec Done

**Message:**
```
SPEC-03: add NormalLoadStrategyTest and ViewerEventProducerIT; mark Done

NormalLoadStrategyTest: 4 unit tests — field population, distribution
chi-square check (10k samples, ±5% tolerance), UUID uniqueness, and
streamId pass-through.

ViewerEventProducerIT: 4 Testcontainers integration tests at 200 TPS
— throughput (≥100 msgs in 2s), key correctness, JSON round-trip, and
health indicator state.

Spec updated to Status: Done with evidence in §10.

Refs: specs/SPEC-03-viewer-event-producer.md
```

**Files:**
- `backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/NormalLoadStrategyTest.java`
- `backend/streamflow-producer/src/test/java/com/streamflow/producer/simulator/ViewerEventProducerIT.java`
- `specs/SPEC-03-viewer-event-producer.md`
- `commits/SPEC-03.md`

**Stage command:**
```bash
git add backend/streamflow-producer/src/test/java/com/streamflow/producer/strategy/NormalLoadStrategyTest.java \
        backend/streamflow-producer/src/test/java/com/streamflow/producer/simulator/ViewerEventProducerIT.java \
        specs/SPEC-03-viewer-event-producer.md \
        commits/SPEC-03.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml verify -pl streamflow-common,streamflow-producer -am` (unit tests pass; IT pass with `DOCKER_HOST` + `TESTCONTAINERS_RYUK_DISABLED=true`)
- [ ] `mvn -f backend/streamflow-producer/pom.xml spring-boot:run` starts on port 8081 and `curl http://localhost:8081/actuator/health` returns `{"status":"UP","components":{"kafkaProducer":{"status":"UP",...}}}`
- [ ] `kafka-console-consumer --topic viewer-events --max-messages 1000 --property print.key=true` returns within ~1s with all three streamIds as keys
- [ ] Demo evidence in `specs/SPEC-03-viewer-event-producer.md §10` matches reality

### Note on CI environment
The integration test (`ViewerEventProducerIT`) uses Testcontainers with a Colima Docker runtime on this machine. In CI (GitHub Actions, Linux), the standard Docker socket `/var/run/docker.sock` will be available and no special env vars are needed. The `TESTCONTAINERS_RYUK_DISABLED=true` workaround is only required locally with Colima.
