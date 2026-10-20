-- Source schema for the walshadow throughput workload: 38-column hot table,
-- a handful of indexes, ~24 tables total.
--
-- Index count is the main throughput dial. Each index adds one WAL record and
-- one page write per row, so it multiplies walshadow's record rate while also
-- throttling the source. Three (PK + two) was the setting that let the source
-- reach ~11.5k commits/s while keeping disk util under 0.10.

CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS bench_workflow_run_steps CASCADE;
CREATE TABLE bench_workflow_run_steps (
    id              bigserial PRIMARY KEY,
    run_id          bigint      NOT NULL,
    workflow_id     bigint      NOT NULL,
    node_id         bigint      NOT NULL,
    step_index      int4        NOT NULL,
    attempt         int4        NOT NULL DEFAULT 0,
    status          text        NOT NULL DEFAULT 'pending',
    kind            text        NOT NULL DEFAULT 'task',
    queued_at       timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    finished_at     timestamptz,
    duration_ms     int4,
    retry_count     int4        NOT NULL DEFAULT 0,
    max_retries     int4        NOT NULL DEFAULT 3,
    priority        int4        NOT NULL DEFAULT 0,
    shard           int4        NOT NULL DEFAULT 0,
    tenant_id       bigint      NOT NULL DEFAULT 0,
    parent_step_id  bigint,
    cost_micros     bigint      NOT NULL DEFAULT 0,
    tokens_in       int4        NOT NULL DEFAULT 0,
    tokens_out      int4        NOT NULL DEFAULT 0,
    bytes_in        int4        NOT NULL DEFAULT 0,
    bytes_out       int4        NOT NULL DEFAULT 0,
    exit_code       int4,
    is_terminal     boolean     NOT NULL DEFAULT false,
    is_cached       boolean     NOT NULL DEFAULT false,
    region          text        NOT NULL DEFAULT 'region-a',
    runner          text        NOT NULL DEFAULT 'default',
    version         int4        NOT NULL DEFAULT 1,
    trace_id        text,
    span_id         text,
    idempotency_key text,
    input           jsonb,
    output          jsonb,
    error           text,
    metadata        jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bench_wrs_run_id      ON bench_workflow_run_steps (run_id);
CREATE INDEX bench_wrs_workflow_id ON bench_workflow_run_steps (workflow_id);
ALTER TABLE bench_workflow_run_steps ALTER COLUMN input SET STORAGE EXTERNAL;

DROP TABLE IF EXISTS bench_workflow_runs CASCADE;
CREATE TABLE bench_workflow_runs (
    id            bigserial PRIMARY KEY,
    workflow_id   bigint      NOT NULL,
    tenant_id     bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL DEFAULT 'running',
    trigger       text        NOT NULL DEFAULT 'api',
    step_count    int4        NOT NULL DEFAULT 0,
    failed_steps  int4        NOT NULL DEFAULT 0,
    duration_ms   int4,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    cost_micros   bigint      NOT NULL DEFAULT 0,
    priority      int4        NOT NULL DEFAULT 0,
    shard         int4        NOT NULL DEFAULT 0,
    version       int4        NOT NULL DEFAULT 1,
    inputs        jsonb,
    outputs       jsonb,
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bench_wr_workflow_id ON bench_workflow_runs (workflow_id);

DROP TABLE IF EXISTS bench_data_list_chunks CASCADE;
CREATE TABLE bench_data_list_chunks (
    id          bigserial PRIMARY KEY,
    list_id     bigint      NOT NULL,
    chunk_index int4        NOT NULL,
    row_count   int4        NOT NULL DEFAULT 0,
    payload     jsonb,
    checksum    text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bench_dlc_list_id ON bench_data_list_chunks (list_id, chunk_index);

DROP TABLE IF EXISTS bench_data_lists CASCADE;
CREATE TABLE bench_data_lists (
    id          bigserial PRIMARY KEY,
    tenant_id   bigint      NOT NULL DEFAULT 0,
    name        text        NOT NULL,
    row_count   bigint      NOT NULL DEFAULT 0,
    schema_json jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS bench_workflow_nodes CASCADE;
CREATE TABLE bench_workflow_nodes (
    id bigserial PRIMARY KEY, workflow_id bigint NOT NULL, kind text NOT NULL,
    config jsonb, created_at timestamptz NOT NULL DEFAULT now());

DROP TABLE IF EXISTS bench_workflow_edges CASCADE;
CREATE TABLE bench_workflow_edges (
    id bigserial PRIMARY KEY, workflow_id bigint NOT NULL, from_node bigint NOT NULL,
    to_node bigint NOT NULL, condition jsonb);

DROP TABLE IF EXISTS bench_workflow_snapshots CASCADE;
CREATE TABLE bench_workflow_snapshots (
    id bigserial PRIMARY KEY, workflow_id bigint NOT NULL, version int4 NOT NULL,
    document jsonb, created_at timestamptz NOT NULL DEFAULT now());

DROP TABLE IF EXISTS bench_workflow_test_samples CASCADE;
CREATE TABLE bench_workflow_test_samples (
    id bigserial PRIMARY KEY, workflow_id bigint NOT NULL, sample jsonb,
    created_at timestamptz NOT NULL DEFAULT now());

DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..16 LOOP
    EXECUTE format(
      'DROP TABLE IF EXISTS bench_aux_%s CASCADE;
       CREATE TABLE bench_aux_%s (
         id bigserial PRIMARY KEY, tenant_id bigint NOT NULL DEFAULT 0,
         label text, body jsonb, created_at timestamptz NOT NULL DEFAULT now());', i, i);
  END LOOP;
END $$;

-- Preflight refuses tracked relations without FULL identity
DO $$
DECLARE t record;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables
           WHERE schemaname = 'public' AND tablename LIKE 'bench\_%' LOOP
    EXECUTE format('ALTER TABLE public.%I REPLICA IDENTITY FULL', t.tablename);
  END LOOP;
END $$;
