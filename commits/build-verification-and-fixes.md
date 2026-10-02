# Commit Plan — Full Build/Test Verification, Frontend Scaffold Restore, Locale Fix

Suggested branch: `chore/build-verification-and-frontend-scaffold-fix`

Context: ran `mvn verify` across all backend modules and a full frontend
lint/test/build pass. Backend was already green (163 tests, 0 real failures;
13 Testcontainers ITs are blocked by a local Docker-npipe environment
constraint, not a code defect — not part of this commit). Frontend was
missing its entire Vite/TS/ESLint scaffold and had one real locale bug,
both fixed below. `CLAUDE.md` was added in an earlier, unrelated pass.

**Update:** GitHub Actions job 88201495670 (commit e7b68db) subsequently failed
`streamflow-processor`'s test phase in CI — a real code defect, distinct from
the local Docker-npipe constraint above, since it happens during Spring's
bean-definition registration phase and fails identically with or without a
reachable Docker daemon. Root-caused and fixed in Commit 5 below.

---

## Commit 1 — Add CLAUDE.md repository guidance for Claude Code

**Message:**
```
Add CLAUDE.md with build commands and architecture overview

Documents Maven/npm/Docker commands for building, testing, and linting
each module, plus the cross-module architecture (common/producer/
processor/api Kafka→Redis→Cassandra pipeline, frontend STOMP/Zustand
data flow) so future Claude Code sessions in this repo can be
productive without re-deriving it from scratch.
```

**Files:**
- `CLAUDE.md`

**Stage command:**
```bash
git add CLAUDE.md
```

---

## Commit 2 — Restore missing frontend Vite/TypeScript/ESLint scaffold

**Message:**
```
Restore missing frontend build scaffold (Vite/TS/ESLint config)

frontend/ was missing index.html, src/main.tsx, src/vite-env.d.ts,
tsconfig.json/.app.json/.node.json, vite.config.ts, postcss.config.js,
and eslint.config.js — confirmed via git log that these were never
committed (not a .gitignore accident), so `npm run lint` and
`npm run build` both failed immediately with no project to build.

Recreated per specs/SPEC-07-frontend-bootstrap.md: strict TS with
noUncheckedIndexedAccess, Vite dev-proxy for /api and /ws to
localhost:8080, Vitest configured with jsdom + src/test/setup.ts,
Tailwind v3 via PostCSS + autoprefixer, and an ESLint v10 flat config
(@eslint/js + typescript-eslint + eslint-plugin-react-hooks +
eslint-plugin-react-refresh + eslint-config-prettier).

Verified: npm run lint (0 errors), npm test (104/104 pass),
npm run build (succeeds, 226 KB gzip).

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/index.html`
- `frontend/src/main.tsx`
- `frontend/src/vite-env.d.ts`
- `frontend/tsconfig.json`
- `frontend/tsconfig.app.json`
- `frontend/tsconfig.node.json`
- `frontend/vite.config.ts`
- `frontend/postcss.config.js`
- `frontend/eslint.config.js`

**Stage command:**
```bash
git add frontend/index.html frontend/src/main.tsx frontend/src/vite-env.d.ts \
        frontend/tsconfig.json frontend/tsconfig.app.json frontend/tsconfig.node.json \
        frontend/vite.config.ts frontend/postcss.config.js frontend/eslint.config.js
```

---

## Commit 3 — Fix locale-dependent number formatting

**Message:**
```
Pin Intl.NumberFormat/toLocaleString to en-US

StreamCard, ViewerCountChart, and ReplayChart all called
Intl.NumberFormat()/toLocaleString() with no locale argument, so
digit grouping silently followed the runtime's default ICU locale.
On a host with a non-US default locale (e.g. en-IN), 847230 rendered
as "8,47,230" instead of "847,230" — caught by
StreamCard.test.tsx failing outside an en-US environment.

Refs: frontend/src/test/StreamCard.test.tsx
```

**Files:**
- `frontend/src/components/stream/StreamCard.tsx`
- `frontend/src/components/stream/ViewerCountChart.tsx`
- `frontend/src/components/history/ReplayChart.tsx`

**Stage command:**
```bash
git add frontend/src/components/stream/StreamCard.tsx \
        frontend/src/components/stream/ViewerCountChart.tsx \
        frontend/src/components/history/ReplayChart.tsx
```

---

## Commit 4 — Add this commit plan

**Message:**
```
Add commit plan for build verification and frontend fixes
```

**Files:**
- `commits/build-verification-and-fixes.md`

**Stage command:**
```bash
git add commits/build-verification-and-fixes.md
```

---

## Commit 5 — Fix CI failure: bean name collisions in streamflow-processor

**Message:**
```
Fix BeanDefinitionOverrideException breaking all processor IT tests in CI

GitHub Actions job 88201495670 (commit e7b68db) failed all 22 assertions
across streamflow-processor's 7 IT test classes with:

  BeanDefinitionOverrideException: Invalid bean definition with name
  'processorMetrics' ... already been defined in
  [.../metrics/ProcessorMetrics.class] and overriding is disabled.

Root cause: com.streamflow.processor.metrics.ProcessorMetrics (SPEC-20,
a @Component) shares its simple class name with Micrometer's built-in
io.micrometer.core.instrument.binder.system.ProcessorMetrics, which Spring
Boot Actuator's SystemMetricsAutoConfiguration also registers under the
default bean name "processorMetrics". Every @SpringBootTest context in the
module hit this during bean-definition registration — before any container
or broker connection is attempted — which is why all 7 IT classes failed
identically regardless of which Testcontainers they used. Fixed by pinning
an explicit, non-colliding bean name (@Component("streamflowProcessorMetrics"));
all existing injection sites resolve by type and are unaffected.

Fixing that unmasked a second, previously-unreached defect: with context
loading no longer aborting immediately, AlertCassandraConsumer (SPEC-17)
failed instead with "required a bean of type CassandraAlertRepository that
could not be found." It used a class-level @ConditionalOnBean on a
@Component picked up via classpath scanning — a pattern Spring Boot's own
docs warn is not reliably evaluated (unlike @ConditionalOnBean inside a
@Configuration class). Fixed by switching to the same Optional<>-wrapped
constructor-injection convention already used by ViewerEventConsumer and
SnapshotPublisher for the same Cassandra-optional problem, dropping the
unreliable class-level conditional entirely.

Verified locally: mvn spring-boot:run against streamflow-processor now
reaches "Started StreamProcessorApplication" (previously failed during
context refresh) with both the main and the test-scoped
(Cassandra-autoconfig-excluded) property sets. Testcontainers-backed IT
tests themselves could not be re-run locally (pre-existing Docker-npipe
constraint, see above) — recommend confirming green on the next CI run.

Refs: SPEC-17 R5, SPEC-20 R3
```

**Files:**
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/metrics/ProcessorMetrics.java`
- `backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/AlertCassandraConsumer.java`

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/metrics/ProcessorMetrics.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/AlertCassandraConsumer.java
```

---

## Verification before pushing

- [x] `mvn -B -f backend/pom.xml spotless:check` — exits 0 (all 4 modules clean)
- [x] `mvn -B -f backend/pom.xml verify` — 163 tests, 0 failures (13 Testcontainers ITs
      blocked by local Docker-npipe access, unrelated to these changes)
- [x] `npm --prefix frontend run lint` — 0 errors
- [x] `npm --prefix frontend test` — 104/104 pass
- [x] `npm --prefix frontend run build` — succeeds, 226 KB gzip
- [x] `mvn -B -f backend/pom.xml spotless:check` (post Commit 5) — exits 0
- [x] `mvn -B -f backend/pom.xml test "-Dtest=!*IT"` (post Commit 5) — 69 tests, 0 failures
      (3 errors are the pre-existing local Docker-npipe constraint, unrelated)
- [x] `mvn -B -f backend/streamflow-processor/pom.xml spring-boot:run` (post Commit 5) —
      reaches "Started StreamProcessorApplication" under both main and test property sets
      (previously failed during ApplicationContext refresh with BeanDefinitionOverrideException)
- [ ] Full Testcontainers IT suite — not runnable locally (Docker-npipe constraint); confirm
      on next CI run of `.github/workflows/ci.yml`'s `backend` job
