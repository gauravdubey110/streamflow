# Commit Plan — SPEC-21: CI + Full-Stack Docker Compose

Suggested branch: `feat/spec-21-ci-and-full-stack-compose`

---

## Commit 1 — Add multi-stage Dockerfiles for all three backend services

**Message:**
```
SPEC-21: add multi-stage Dockerfiles for backend services

Each Dockerfile uses a maven:3.9-eclipse-temurin-17 build stage with
BuildKit cache mounts for ~/.m2, Spring Boot layertools JAR extraction,
and an eclipse-temurin:17-jre-alpine runtime stage. Services run as a
non-root `streamflow` user.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `backend/streamflow-producer/Dockerfile`
- `backend/streamflow-processor/Dockerfile`
- `backend/streamflow-api/Dockerfile`

**Stage command:**
```bash
git add backend/streamflow-producer/Dockerfile \
        backend/streamflow-processor/Dockerfile \
        backend/streamflow-api/Dockerfile
```

---

## Commit 2 — Add frontend Dockerfile and Nginx config

**Message:**
```
SPEC-21: add frontend Dockerfile (node:20-alpine → nginx:alpine)

Build stage runs `npm ci && npm run build`; runtime stage copies the
Vite dist into nginx:alpine with a custom config that proxies /api/ and
/ws to the api service inside the Docker network.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `frontend/Dockerfile`
- `frontend/nginx.conf`

**Stage command:**
```bash
git add frontend/Dockerfile frontend/nginx.conf
```

---

## Commit 3 — Add root docker-compose.yml (full stack)

**Message:**
```
SPEC-21: add root docker-compose.yml for single-command full-stack startup

Brings up 12 services: Zookeeper, Kafka, kafka-init (one-shot topic
creation), Redis, Cassandra, cassandra-init (one-shot schema init),
Prometheus (using prometheus-compose.yml for container-name targets),
Grafana, producer, processor, api, and frontend. All services use
healthchecks and depends_on conditions to enforce startup order.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `docker-compose.yml`
- `infra/prometheus/prometheus-compose.yml`

**Stage command:**
```bash
git add docker-compose.yml infra/prometheus/prometheus-compose.yml
```

---

## Commit 4 — Update GitHub Actions CI workflow

**Message:**
```
SPEC-21: expand CI workflow with frontend + docker jobs

Adds a frontend job (Node 20, npm ci + lint + test + build, npm cache)
and a docker job (builds + pushes 4 images to GHCR on main). Expands
the backend job to run `mvn spotless:check` before `mvn verify`.
Concurrency group cancels in-flight runs on the same branch.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `.github/workflows/ci.yml`

**Stage command:**
```bash
git add .github/workflows/ci.yml
```

---

## Commit 5 — Add Spotless to parent POM and apply formatting to all Java files

**Message:**
```
SPEC-21: add Spotless (Google Java Format) to parent POM; apply to all sources

spotless-maven-plugin 2.43.0 with googleJavaFormat 1.22.0 added to
pluginManagement. `mvn spotless:apply` reformatted 42 Java source files
to Google style (style-only; no logic changes). `mvn spotless:check`
now passes on all modules.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `backend/pom.xml`
- `backend/streamflow-common/src/main/java/com/streamflow/common/**/*.java` (all reformatted)
- `backend/streamflow-producer/src/main/java/com/streamflow/producer/**/*.java`
- `backend/streamflow-producer/src/test/java/com/streamflow/producer/**/*.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/**/*.java`
- `backend/streamflow-processor/src/test/java/com/streamflow/processor/**/*.java`
- `backend/streamflow-api/src/main/java/com/streamflow/api/**/*.java`
- `backend/streamflow-api/src/test/java/com/streamflow/api/**/*.java`

**Stage command:**
```bash
git add backend/pom.xml
git add backend/streamflow-common/src/
git add backend/streamflow-producer/src/
git add backend/streamflow-processor/src/
git add backend/streamflow-api/src/
```

---

## Commit 6 — Add frontend pre-commit hooks and lint-staged config

**Message:**
```
SPEC-21: add husky pre-commit hook and lint-staged config for frontend

.husky/pre-commit runs lint-staged on staged TS/TSX files (ESLint fix +
Prettier). .lintstagedrc.json also formats JSON, CSS, and MD files.
To activate: cd frontend && npm install --save-dev husky lint-staged &&
npx husky install

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `frontend/.lintstagedrc.json`
- `frontend/.husky/pre-commit`

**Stage command:**
```bash
git add frontend/.lintstagedrc.json frontend/.husky/pre-commit
```

---

## Commit 7 — Update README with CI badge and full setup instructions

**Message:**
```
SPEC-21: update README with CI badge, architecture table, and setup guide

Adds CI status badge (update YOUR_ORG/StreamFlow with actual repo path),
architecture overview, environment variable table, and pre-commit hook
installation instructions.

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `README.md`

**Stage command:**
```bash
git add README.md
```

---

## Commit 8 — Mark SPEC-21 Done with evidence

**Message:**
```
SPEC-21: mark spec Done, add evidence section and commit plan

Evidence: backend BUILD SUCCESS, 104 frontend tests pass, spotless check
passes (42 files), docker-compose.yml validated (12 services), frontend
dist 776 KB. Docker image sizes require Docker daemon (CI will verify).

Refs: specs/SPEC-21-ci-and-full-stack-compose.md
```

**Files:**
- `specs/SPEC-21-ci-and-full-stack-compose.md`
- `commits/SPEC-21.md`

**Stage command:**
```bash
git add specs/SPEC-21-ci-and-full-stack-compose.md commits/SPEC-21.md
```

---

## Verification before pushing

- [ ] `mvn -B -f backend/pom.xml spotless:check` — exits 0
- [ ] `mvn -B -f backend/pom.xml verify -DskipTests` — exits 0 (full verify with tests requires Docker)
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` — all pass
- [ ] `docker compose -f docker-compose.yml config --quiet` — exits 0 (no Docker daemon needed)
- [ ] Demo evidence in spec matches reality
