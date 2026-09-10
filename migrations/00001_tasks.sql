-- +goose Up
CREATE TABLE tasks (
 id uuid PRIMARY KEY, client_id text NOT NULL, type text NOT NULL,
 payload jsonb NOT NULL, result jsonb, state text NOT NULL DEFAULT 'queued'
 CHECK(state IN ('queued','running','succeeded','failed','canceled')),
 run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 max_attempts integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 10),
 timeout_ms integer NOT NULL CHECK(timeout_ms BETWEEN 1000 AND 300000),
 lease_until timestamptz, generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0),
 worker_id text, cancel_requested boolean NOT NULL DEFAULT false,
 idempotency_key text NOT NULL, request_hash text NOT NULL,
 parent_task_id uuid REFERENCES tasks(id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(client_id,idempotency_key),
 CHECK((state='running')=(lease_until IS NOT NULL AND worker_id IS NOT NULL)),
 CHECK(attempt_count<=max_attempts)
);
CREATE INDEX tasks_ready ON tasks(run_at,id) WHERE state='queued';
CREATE INDEX tasks_expired ON tasks(lease_until) WHERE state='running';
CREATE INDEX tasks_client_list ON tasks(client_id,created_at,id);
CREATE TABLE task_attempts (
 task_id uuid NOT NULL REFERENCES tasks(id), generation bigint NOT NULL,
 worker_id text NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz, outcome text NOT NULL DEFAULT 'running'
 CHECK(outcome IN ('running','succeeded','retry','failed','canceled','lease_expired')),
 error text NOT NULL DEFAULT '', PRIMARY KEY(task_id,generation)
);
-- Down migration deliberately lives in a separate goose section.
