-- Hot table shaped after a production workload's catalog statistics: 38
-- columns, six btree indexes (two partial on status), REPLICA IDENTITY
-- DEFAULT, lz4 jsonb columns of which payload_a and payload_b live out of
-- line. Swappable via SCHEMA=; seed.sql and workload.sql name ceramic_items,
-- ceramic_runs and ceramic_pool. 24 tables in all.
CREATE TABLE ceramic_items (
    id             bigserial PRIMARY KEY,
    parent_id      bigint      NOT NULL,
    group_id       bigint      NOT NULL,
    node_id        bigint      NOT NULL,
    list_id        bigint,
    seq_no         int4        NOT NULL DEFAULT 0,
    attempt        int4        NOT NULL DEFAULT 0,
    status         text        NOT NULL DEFAULT 'pending',
    kind           text        NOT NULL DEFAULT 'task',
    queued_at      timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    completed_at   timestamptz,
    deleted_at     timestamptz,
    duration_ms    int4,
    retry_count    int4        NOT NULL DEFAULT 0,
    priority       int4        NOT NULL DEFAULT 0,
    shard          int4        NOT NULL DEFAULT 0,
    owner_id       bigint      NOT NULL DEFAULT 0,
    parent_item_id bigint,
    cost           bigint      NOT NULL DEFAULT 0,
    units_in       int4        NOT NULL DEFAULT 0,
    units_out      int4        NOT NULL DEFAULT 0,
    exit_code      int4,
    is_terminal    boolean     NOT NULL DEFAULT false,
    is_cached      boolean     NOT NULL DEFAULT false,
    region         text        NOT NULL DEFAULT 'region-1',
    worker_name    text        NOT NULL DEFAULT 'default',
    version        int4        NOT NULL DEFAULT 1,
    trace_id       text,
    idempotency_key text,
    payload_a      jsonb,
    payload_b      jsonb,
    payload_c      jsonb,
    payload_d      jsonb,
    note           text,
    source_ids     text[],
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ceramic_items_parent_idx ON ceramic_items (parent_id);
CREATE INDEX ceramic_items_group_idx ON ceramic_items (group_id);
CREATE INDEX ceramic_items_trace_idx ON ceramic_items (trace_id);
CREATE INDEX ceramic_items_group_status_partial_idx
  ON ceramic_items (group_id, node_id, status, completed_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX ceramic_items_list_status_partial_idx
  ON ceramic_items (list_id, status, seq_no) WHERE list_id IS NOT NULL;

CREATE TABLE ceramic_runs (
    id          bigserial PRIMARY KEY,
    group_id    bigint      NOT NULL,
    owner_id    bigint      NOT NULL DEFAULT 0,
    status      text        NOT NULL DEFAULT 'running',
    trigger     text        NOT NULL DEFAULT 'api',
    item_count  int4        NOT NULL DEFAULT 0,
    duration_ms int4,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    shard       int4        NOT NULL DEFAULT 0,
    input_a     jsonb,
    state_a     jsonb,
    headers_a   jsonb,
    origin_a    jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ceramic_runs_group_idx ON ceramic_runs (group_id);

CREATE TABLE ceramic_queue (
    id bigserial PRIMARY KEY, list_id bigint NOT NULL, seq_no int4 NOT NULL,
    row_count int4 NOT NULL DEFAULT 0, payload jsonb, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ceramic_lists (
    id bigserial PRIMARY KEY, owner_id bigint NOT NULL DEFAULT 0, name text NOT NULL,
    row_count bigint NOT NULL DEFAULT 0, schema_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ceramic_nodes (
    id bigserial PRIMARY KEY, group_id bigint NOT NULL, kind text NOT NULL,
    config jsonb, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ceramic_edges (
    id bigserial PRIMARY KEY, group_id bigint NOT NULL, src_node bigint NOT NULL,
    dst_node bigint NOT NULL, condition jsonb);
CREATE TABLE ceramic_snapshots (
    id bigserial PRIMARY KEY, group_id bigint NOT NULL, version int4 NOT NULL,
    document jsonb, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ceramic_samples (
    id bigserial PRIMARY KEY, group_id bigint NOT NULL, sample jsonb,
    created_at timestamptz NOT NULL DEFAULT now());
DO $$
BEGIN
  FOR i IN 1..16 LOOP
    EXECUTE format('CREATE TABLE ceramic_aux_%s (id bigserial PRIMARY KEY, owner_id bigint NOT NULL DEFAULT 0,
      label text, body jsonb, created_at timestamptz NOT NULL DEFAULT now())', i);
  END LOOP;
END $$;
