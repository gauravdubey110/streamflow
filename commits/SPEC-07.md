# Commit Plan — SPEC-07: Frontend Bootstrap

Suggested branch: `feat/spec-07-frontend-bootstrap`

---

## Commit 1 — Add TypeScript types for stream and alert domain

**Message:**
```
SPEC-07: add stream and alert TypeScript domain types

Add src/types/stream.types.ts (StreamMetricSnapshot,
StreamSummaryDTO, WS message shapes) and
src/types/alert.types.ts (AlertEvent, AlertFiredMessage),
mirroring the backend DTOs from streamflow-common.

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/src/types/stream.types.ts`
- `frontend/src/types/alert.types.ts`

**Stage command:**
```bash
git add frontend/src/types/stream.types.ts frontend/src/types/alert.types.ts
```

---

## Commit 2 — Add Axios and STOMP/SockJS services

**Message:**
```
SPEC-07: add api.ts Axios instance and stompClient factory

services/api.ts: Axios instance with VITE_API_BASE base URL.
services/stompClient.ts: createStompClient() factory —
SockJS transport, 5s reconnect, 10s heartbeat.

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/src/services/api.ts`
- `frontend/src/services/stompClient.ts`
- `frontend/.env`

**Stage command:**
```bash
git add frontend/src/services/api.ts frontend/src/services/stompClient.ts frontend/.env
```

---

## Commit 3 — Add useWebSocket hook and streamStore Zustand store

**Message:**
```
SPEC-07: add useWebSocket hook and Zustand streamStore

useWebSocket.ts: connects STOMP on mount, exposes
{connected, subscribe, unsubscribe}, handles reconnect
and cleanup correctly.

streamStore.ts: Zustand store with streams Record and
setSnapshot upsert action.

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/src/hooks/useWebSocket.ts`
- `frontend/src/store/streamStore.ts`

**Stage command:**
```bash
git add frontend/src/hooks/useWebSocket.ts frontend/src/store/streamStore.ts
```

---

## Commit 4 — Add LiveDot component and replace App.tsx scaffold

**Message:**
```
SPEC-07: add LiveDot component and connection-indicator App

components/common/LiveDot.tsx: pulsing green dot when
connected, gray when disconnected.

App.tsx replaced: dark background, uses useWebSocket,
shows "StreamFlow — connected/disconnected" with LiveDot.

Removes unused App.css scaffold file.

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/src/components/common/LiveDot.tsx`
- `frontend/src/App.tsx`
- `frontend/src/App.css` (deleted)

**Stage command:**
```bash
git add frontend/src/components/common/LiveDot.tsx frontend/src/App.tsx
git rm frontend/src/App.css
```

---

## Commit 5 — Add Vitest test setup and streamStore unit tests

**Message:**
```
SPEC-07: add Vitest setup and streamStore unit tests

src/test/setup.ts: imports @testing-library/jest-dom.
src/test/streamStore.test.ts: 3 tests — insert,
upsert, and multi-stream isolation for setSnapshot.

All 3 tests pass (vitest run).

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `frontend/src/test/setup.ts`
- `frontend/src/test/streamStore.test.ts`

**Stage command:**
```bash
git add frontend/src/test/setup.ts frontend/src/test/streamStore.test.ts
```

---

## Commit 6 — Mark SPEC-07 Done and add commit plan

**Message:**
```
SPEC-07: mark Done, add evidence and commit plan

Update spec status to Done, tick all AC and DoD
checkboxes, add §10 Evidence with lint/build/test output.
Add commits/SPEC-07.md commit plan.
Note React 19 deviation.

Refs: specs/SPEC-07-frontend-bootstrap.md
```

**Files:**
- `specs/SPEC-07-frontend-bootstrap.md`
- `commits/SPEC-07.md`

**Stage command:**
```bash
git add specs/SPEC-07-frontend-bootstrap.md commits/SPEC-07.md
```

---

## Verification before pushing

- [ ] `npm --prefix frontend run lint` — 0 errors
- [ ] `npm --prefix frontend run build` — 0 TS errors, dist/assets gzip < 500 KB
- [ ] `npm --prefix frontend run test` — 3/3 tests pass
- [ ] Demo evidence in spec matches reality (lint, build, test outputs)
