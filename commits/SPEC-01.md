# Commit Plan — SPEC-01: Infrastructure & Maven Bootstrap

Suggested branch: `feat/spec-01-infra-maven-bootstrap`

---

## Commit 1 — Add root skeleton: README, .gitignore, CI workflow

**Message:**
```
SPEC-01: add root skeleton (README, .gitignore, CI workflow)

Create the top-level repo skeleton:
- README.md with quick-start commands
- .gitignore covering Java/Maven, Node/npm, IDE, and OS artefacts
- .github/workflows/ci.yml — builds backend on push/PR with Java 17

Refs: specs/SPEC-01-infra-and-maven-bootstrap.md
```

**Files:**
- `README.md`
- `.gitignore`
- `.github/workflows/ci.yml`

**Stage command:**
```bash
git add README.md .gitignore .github/workflows/ci.yml
```

---

## Commit 2 — Add Maven multi-module backend skeleton

**Message:**
```
SPEC-01: add Maven multi-module backend skeleton

Parent POM (backend/pom.xml) imports spring-boot-dependencies 3.2.5
and resilience4j-bom 2.1.0 via <dependencyManagement> (BOM import
pattern; see Design Notes for Q1 rationale).

Four child modules:
- streamflow-common  (library jar; Jackson + Lombok)
- streamflow-producer (Spring Boot 8081; Spring Kafka + Web)
- streamflow-processor (Spring Boot 8082; Kafka + Redis + Cassandra +
                        Resilience4j)
- streamflow-api      (Spring Boot 8080; Web + WebSocket + Kafka +
                        Redis + Cassandra + Prometheus)

Each runnable module has a no-op @SpringBootApplication main class
and a minimal application.properties with env-var overrides for
Kafka, Redis, and Cassandra connection strings.

mvn -f backend/pom.xml -q -DskipTests package → 4 JARs produced.

Refs: specs/SPEC-01-infra-and-maven-bootstrap.md
```

**Files:**
- `backend/pom.xml`
- `backend/streamflow-common/pom.xml`
- `backend/streamflow-common/src/main/java/com/streamflow/common/package-info.java`
- `backend/streamflow-producer/pom.xml`
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/StreamProducerApplication.java`
- `backend/streamflow-producer/src/main/resources/application.properties`
- `backend/streamflow-processor/pom.xml`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/StreamProcessorApplication.java`
- `backend/streamflow-processor/src/main/resources/application.properties`
- `backend/streamflow-api/pom.xml`
- `backend/streamflow-api/src/main/java/com/streamflow/api/StreamApiApplication.java`
- `backend/streamflow-api/src/main/resources/application.properties`

**Stage command:**
```bash
git add \
  backend/pom.xml \
  backend/streamflow-common/pom.xml \
  backend/streamflow-common/src/main/java/com/streamflow/common/package-info.java \
  backend/streamflow-producer/pom.xml \
  backend/streamflow-producer/src/main/java/com/streamflow/producer/StreamProducerApplication.java \
  backend/streamflow-producer/src/main/resources/application.properties \
  backend/streamflow-processor/pom.xml \
  backend/streamflow-processor/src/main/java/com/streamflow/processor/StreamProcessorApplication.java \
  backend/streamflow-processor/src/main/resources/application.properties \
  backend/streamflow-api/pom.xml \
  backend/streamflow-api/src/main/java/com/streamflow/api/StreamApiApplication.java \
  backend/streamflow-api/src/main/resources/application.properties
```

---

## Commit 3 — Add infra: docker-compose.dev.yml + init-topics.sh

**Message:**
```
SPEC-01: add dev infra (Kafka, Redis) + topic init script

infra/docker-compose.dev.yml brings up:
- confluentinc/cp-zookeeper:7.5.3 with bash-TCP health check
- confluentinc/cp-kafka:7.5.3 with dual listeners:
    PLAINTEXT://kafka:29092  (inter-container)
    PLAINTEXT_HOST://localhost:9092  (host dev access)
  KAFKA_AUTO_CREATE_TOPICS_ENABLE=false (explicit topic creation)
- redis:7.2-alpine with redis-cli health check

infra/kafka/init-topics.sh creates the 4 canonical topics:
- viewer-events      (6 partitions, RF=1)
- stream-health      (3 partitions, RF=1)
- alerts             (3 partitions, RF=1)
- metrics-aggregated (3 partitions, RF=1)

All containers verified healthy; all topics created successfully.

Refs: specs/SPEC-01-infra-and-maven-bootstrap.md
```

**Files:**
- `infra/docker-compose.dev.yml`
- `infra/kafka/init-topics.sh`

**Stage command:**
```bash
git add infra/docker-compose.dev.yml infra/kafka/init-topics.sh
```

---

## Commit 4 — Mark SPEC-01 Done + add evidence + commit plan

**Message:**
```
SPEC-01: mark Done, add evidence and commit plan

- Tick all ACs and DoD checkboxes in spec
- Document Q1 resolution (BOM import) in Design Notes
- Add § 10 Evidence with actual command output
- Add commits/SPEC-01.md commit plan

Refs: specs/SPEC-01-infra-and-maven-bootstrap.md
```

**Files:**
- `specs/SPEC-01-infra-and-maven-bootstrap.md`
- `commits/SPEC-01.md`

**Stage command:**
```bash
git add specs/SPEC-01-infra-and-maven-bootstrap.md commits/SPEC-01.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml -q -DskipTests package` exits 0 and produces 4 JARs
- [ ] `docker compose -f infra/docker-compose.dev.yml up -d` → all containers `healthy`
- [ ] `./infra/kafka/init-topics.sh` → exits 0, 4 topics listed
- [ ] `docker exec streamflow-redis redis-cli ping` → `PONG`
- [ ] Demo evidence in `specs/SPEC-01-infra-and-maven-bootstrap.md § 10` matches reality
