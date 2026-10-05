# TaskRail

TaskRail is a Go background task service with a Gin REST API, a PostgreSQL queue and independent workers. It demonstrates concurrent claiming, bounded execution and lease recovery.

## Quick start

Requirements: Docker Desktop / Docker Engine with Compose v2. Go 1.26.6 is needed only for native development.

```powershell
Copy-Item .env.example .env
# Edit API_KEYS and POSTGRES_PASSWORD for your local setup.
docker compose up --build -d
```

The migration service runs before the API and workers. PostgreSQL data persists in a named volume. The database is not exposed to the host. The API is bound to localhost: http://localhost:8080

The Compose deployment is a development environment. A public deployment additionally needs TLS termination, managed secrets, backup/restore and resource sizing.

## Create and inspect a task

PowerShell:

```powershell
$headers = @{'X-API-Key'='replace-this-local-key'; 'Idempotency-Key'=[guid]::NewGuid().ToString()}
$body = '{"type":"report.aggregate","payload":{"numbers":[1,2,3]}}'
$task = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/tasks -Headers $headers -ContentType application/json -Body $body
Invoke-RestMethod -Uri "http://localhost:8080/v1/tasks/$($task.id)" -Headers $headers
Invoke-RestMethod -Uri "http://localhost:8080/v1/tasks/$($task.id)/attempts" -Headers $headers
```

Bash:

```bash
export API_KEY='replace-this-local-key'
curl -sS http://localhost:8080/v1/tasks \
  -H "X-API-Key: $API_KEY" -H 'Idempotency-Key: report-example-1' \
  -H 'Content-Type: application/json' \
  -d '{"type":"report.aggregate","payload":{"numbers":[1,2,3]}}'
```

The aggregate result is `{"count":3,"sum":6,"min":1,"max":3,"mean":2}`. A task can be queued with a future RFC3339 `run_at`. Unknown fields, duplicate JSON keys and trailing JSON values are rejected. Payload bodies are limited to 1 MiB; reports accept 1..10000 finite double-precision numbers. This handler is a demonstration of task execution, not financial decimal arithmetic. Numerical overflow produces a permanent error.

## API contract

[OpenAPI](api/openapi.yaml) defines schemas and responses.

| Method | Path | Purpose |
| --- | --- | --- |
| POST | /v1/tasks | Create; Idempotency-Key required |
| GET | /v1/tasks?limit=20&cursor=... | List own tasks; stable ascending pagination |
| GET | /v1/tasks/{id} | State and result |
| GET | /v1/tasks/{id}/attempts | Attempt history |
| POST | /v1/tasks/{id}/cancel | Cooperative cancellation |
| POST | /v1/tasks/{id}/retry | Create a new task from a failed task; Idempotency-Key required |
| GET | /healthz | Process liveness |
| GET | /readyz | Database connectivity |

API keys map to client identities through `API_KEYS=client-a:key-a,client-b:key-b`. Every task operation checks ownership. Invalid and foreign task IDs both return 404. Authorization failures return 401. Errors have `code`, `message` and `request_id`; HTTP responses include `X-Request-ID`.

An identical request with the same client and idempotency key returns the original task with 200 rather than creating another task (201). Different normalized content returns 409. Explicit defaults and UTC-equivalent run_at are normalized. Keys are retained with the task; automatic retention cleanup is not implemented. Retries have a new ID and `parent_task_id`; the old task and attempt history stay unchanged.

Rate limiting is local to each API instance: 100 authenticated requests per client per second, with a shared maximum of 64 in-flight client requests. It is not a distributed quota. Cancellation of a queued task is immediate; cancellation of a running task is observed on heartbeat. A cancellation committed before finalization wins; a completed task retains its terminal state.

## Queue guarantees

```mermaid
flowchart LR
  Client -->|POST + idempotency key| API[Gin API]
  API -->|transaction| DB[(PostgreSQL)]
  DB -->|SKIP LOCKED + lease| A[Worker A]
  DB -->|SKIP LOCKED + lease| B[Worker B]
  A -->|heartbeat / result| DB
  B -->|recover expired leases| DB
```

Claiming happens in a short transaction. Execution holds no database transaction or row lock. The claim increments both attempt count and generation and records attempt history atomically. A completion, retry or heartbeat must match the worker and generation and have an unexpired lease, using PostgreSQL's clock. Stale workers cannot finalize a task after expiry or replacement.

Delivery is **at least once**. A worker may have performed a side effect before dying, so another attempt can repeat it. Generation protects TaskRail's database state; external effects require receiver-side idempotency. Handlers must honor context cancellation. A non-cooperative handler cannot be forcibly stopped inside a Go process; shutdown exits after grace and lease recovery handles unfinished tasks.

State transitions: queued -> running -> succeeded / failed / canceled; a temporary error returns running -> queued with backoff. Expired leases return to queued, become failed after the attempt limit, or canceled when cancellation was requested. Failed tasks form the inspectable failed queue and can be retried manually. Recovery currently requeues immediately; regular handler retries use full-jitter exponential backoff.

## Configuration

The application reads environment variables; it does not automatically load .env. Compose loads .env for interpolation.

| Variable | Default | Meaning |
| --- | --- | --- |
| DATABASE_URL | required | PostgreSQL connection string |
| API_KEYS | required | client:key pairs; keys and client IDs must be unique |
| HTTP_ADDR | :8080 | API bind address |
| WORKER_ID | generated UUID | Worker identity |
| CONCURRENCY | 4 | Worker slots, 1..256 |
| LEASE | 30s | Ownership lease |
| HEARTBEAT | 10s | Renewal interval; must be less than lease |
| POLL | 500ms | Queue and recovery polling |
| SHUTDOWN_GRACE | 20s | Time allowed for active attempts |
| DEMO_MODE | false | Allow demo.wait handler |

Task defaults: max_attempts=5, timeout_ms=60000. Allowed ranges: 1..10 attempts, timeout 1000..300000 ms. Retry backoff starts at 1 second and caps at 60 seconds with full jitter. PostgreSQL errors during heartbeat cancel execution; the task is recovered when its lease expires. Shutdown stops claiming and grants active handlers time to finish with heartbeats, then cancels them.

## Logging

JSON slog records include request and task IDs; request payloads and credentials are excluded.

## Recovery demonstration

Enable `DEMO_MODE=true` in .env and recreate the app services:

```powershell
docker compose up -d --force-recreate api worker-a worker-b
$env:API_KEY='replace-this-local-key'
.\scripts\demo-recovery.ps1
```

Bash users need jq:

```bash
API_KEY='replace-this-local-key' bash scripts/demo-recovery.sh
```

The script queues a ten-second wait, discovers the claiming worker through attempt history, kills that service, and checks that a second worker completes the task after lease expiry. It checks the `lease_expired` history entry and restarts the killed service in cleanup. Use an otherwise idle local deployment for this demonstration; it intentionally stops a worker.

## Native development and checks

Use a disposable PostgreSQL database for integration tests; they create and remove independent schemas. Migrations must run from the repository root.

```powershell
$env:DATABASE_URL='postgres://taskrail:password@localhost:5432/taskrail?sslmode=disable'
$env:API_KEYS='demo:development-key'
go run ./cmd/migrate
go run ./cmd/api
# In another terminal, set the same variables and run:
go run ./cmd/worker
```

```powershell
$env:TEST_DATABASE_URL='postgres://taskrail:password@localhost:5432/taskrail_test?sslmode=disable'
go vet ./...
go test -race -count=1 ./...
go build ./cmd/...
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

Integration tests are skipped only when TEST_DATABASE_URL is unset. A configured but unavailable database fails the run. CI uses PostgreSQL and the race detector. Tests cover concurrent idempotency and claiming, ownership, expiry, stale completion, attempt exhaustion, cancellation ordering, manual retry and cursor pagination.

For a load test, install k6 and run `k6 run scripts/load.js` with API_KEY set. The script measures task-creation requests; it does not measure end-to-end task completion throughput. Run under a load compatible with the documented 100 requests/s/client API limit or interpret 429s as expected quota responses. Publish measurements with CPU/RAM/OS, PostgreSQL settings, task size, concurrency, test duration, throughput, latency percentiles and error/429 rate. No benchmark results are claimed without measurements.

## Scope

Implemented scope is a local v0.1 service. Periodic scheduling, priority queues, distributed API quotas and external executable jobs are future work.

