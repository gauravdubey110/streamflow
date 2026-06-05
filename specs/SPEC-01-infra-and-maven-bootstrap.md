# SPEC-01: Infrastructure & Maven Bootstrap

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** none

## 1. Goal
Stand up the project skeleton so every later spec has a working Maven build and runnable Kafka + Redis infrastructure to develop against.

## 2. Context
The plan calls for a Maven multi-module backend (`streamflow-common`, `-producer`, `-processor`, `-api`) plus a `frontend/` React app, with Kafka + Redis (and later Cassandra) running locally via Docker Compose. This spec creates the empty skeleton — no business logic.

## 3. Requirements
### Functional
- R1. Repo root contains `backend/`, `frontend/`, `infra/`, `specs/`, `README.md`, root `.gitignore`.
- R2. `backend/pom.xml` is a Maven parent POM (packaging `pom`) declaring 4 child modules and managing Spring Boot 3.2.x, Spring Kafka, Spring Data Redis, Spring Data Cassandra, Resilience4j, Jackson, Lombok, JUnit 5, Testcontainers versions in `<dependencyManagement>`.
- R3. Each backend child module has a placeholder `pom.xml` that inherits from the parent and a `src/main/java` package (`com.streamflow.<module>`) with a no-op `*Application.java` (Spring Boot main class for `-producer`, `-processor`, `-api`; library jar for `-common`).
- R4. `infra/docker-compose.dev.yml` brings up: Zookeeper, Kafka 3.6 (single broker, advertised listener `localhost:9092`), Redis 7. Health checks defined.
- R5. `infra/kafka/init-topics.sh` creates topics `viewer-events` (6 partitions), `stream-health` (3), `alerts` (3), `metrics-aggregated` (3), all RF=1.
- R6. Root `.github/workflows/ci.yml` placeholder runs `mvn -B -pl backend -am verify` on push (skip if no tests yet — `-DskipTests` allowed).

### Non-Functional
- NFR1. `mvn -f backend/pom.xml clean install -DskipTests` succeeds on a fresh clone with Java 17.
- NFR2. `docker compose -f infra/docker-compose.dev.yml up -d && ./infra/kafka/init-topics.sh` exits 0 on Mac/Linux; topics visible via `kafka-topics --list`.

## 4. Design Notes
- Use `spring-boot-starter-parent` 3.2.5 in `<parent>` of each runnable module rather than the parent POM, OR import `spring-boot-dependencies` BOM in the parent — pick one and document.
- **Decision (Q1):** BOM import in `backend/pom.xml` via `<dependencyManagement>`. Each runnable module configures `spring-boot-maven-plugin` with `repackage` goal explicitly. Rationale: single parent hierarchy; standard enterprise pattern; avoids plugin configuration duplication issues.
- Pin Kafka image to `confluentinc/cp-kafka:7.5.3`, Zookeeper to `confluentinc/cp-zookeeper:7.5.3`, Redis to `redis:7.2-alpine`.
- Expose Kafka on host `9092`, Redis on `6379`. Use a named docker network `streamflow-net`.
- Add `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` so topic creation is explicit.
- Kafka listeners: `PLAINTEXT://kafka:29092` (inter-container) and `PLAINTEXT_HOST://localhost:9092` (host access). Spring Boot dev services connect via `localhost:9092`.

## 5. Acceptance Criteria
- [x] AC1. `tree -L 2 backend frontend infra specs` shows the documented layout.
- [x] AC2. `mvn -f backend/pom.xml -q -DskipTests package` produces 4 jars.
- [x] AC3. `docker compose -f infra/docker-compose.dev.yml ps` shows all containers `healthy`.
- [x] AC4. `docker exec <kafka> kafka-topics --bootstrap-server localhost:9092 --list` outputs the 4 topics with correct partition counts.
- [x] AC5. `redis-cli -h localhost ping` returns `PONG`.

## 6. Tasks / Implementation Steps
1. Create directory structure and root `README.md` (link to plan + specs).
2. Write parent `backend/pom.xml` with `<dependencyManagement>` and module list.
3. Generate 4 child modules with placeholder app classes.
4. Write `infra/docker-compose.dev.yml` and `init-topics.sh` (chmod +x).
5. Write `.github/workflows/ci.yml` placeholder.
6. Verify ACs locally and commit.

## 7. Test Plan
- Manual: run all AC commands and screenshot outputs.
- No automated tests required in this spec (no business logic yet).

## 8. Open Questions
- ~~Q1. Use Spring Boot parent inheritance per module, or BOM import in shared parent?~~
  **Resolved:** BOM import in parent POM. See Design Notes.

## 9. Definition of Done
- [x] All acceptance criteria pass
- [x] CI workflow runs green on first push
- [x] Spec status updated to `Done`

## 10. Evidence

### AC2 — `mvn -f backend/pom.xml -q -DskipTests package` produces 4 jars

```
$ find backend -name "*.jar" -not -name "*-sources.jar" | sort
backend/streamflow-api/target/streamflow-api-0.0.1-SNAPSHOT.jar
backend/streamflow-common/target/streamflow-common-0.0.1-SNAPSHOT.jar
backend/streamflow-processor/target/streamflow-processor-0.0.1-SNAPSHOT.jar
backend/streamflow-producer/target/streamflow-producer-0.0.1-SNAPSHOT.jar
```

### AC3 — All containers healthy

```
$ docker compose -f infra/docker-compose.dev.yml ps
NAME                   IMAGE                             COMMAND                  SERVICE     CREATED          STATUS                    PORTS
streamflow-kafka       confluentinc/cp-kafka:7.5.3       "/etc/confluent/dock…"   kafka       56 seconds ago   Up 42 seconds (healthy)   0.0.0.0:9092->9092/tcp
streamflow-redis       redis:7.2-alpine                  "docker-entrypoint.s…"   redis       56 seconds ago   Up 54 seconds (healthy)   0.0.0.0:6379->6379/tcp
streamflow-zookeeper   confluentinc/cp-zookeeper:7.5.3   "/etc/confluent/dock…"   zookeeper   57 seconds ago   Up 54 seconds (healthy)   2181/tcp
```

### AC4 — 4 topics created with correct partition counts

```
$ docker exec streamflow-kafka kafka-topics --bootstrap-server localhost:9092 --describe
Topic: alerts            PartitionCount: 3  ReplicationFactor: 1
Topic: metrics-aggregated PartitionCount: 3 ReplicationFactor: 1
Topic: stream-health     PartitionCount: 3  ReplicationFactor: 1
Topic: viewer-events     PartitionCount: 6  ReplicationFactor: 1
```

### AC5 — Redis PING

```
$ docker exec streamflow-redis redis-cli ping
PONG
```

*(Note: `redis-cli` is run inside the container since it is not installed on the host. The Redis port 6379 is bound to `localhost:6379`, satisfying AC5.)*

### init-topics.sh output

```
Waiting for Kafka at localhost:9092...
Kafka is up.

Creating StreamFlow Kafka topics...
  Creating topic: viewer-events (6 partitions, RF=1)
Created topic viewer-events.
  Creating topic: stream-health (3 partitions, RF=1)
Created topic stream-health.
  Creating topic: alerts (3 partitions, RF=1)
Created topic alerts.
  Creating topic: metrics-aggregated (3 partitions, RF=1)
Created topic metrics-aggregated.

Topic list:
alerts
metrics-aggregated
stream-health
viewer-events

Done. All StreamFlow topics created.
```
