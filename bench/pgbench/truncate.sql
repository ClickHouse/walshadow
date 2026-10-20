-- Reset between runs, keeping bench_payload_pool so run.sh need not rebuild it.
-- TRUNCATE replicates, so the ClickHouse tables empty too once the shadow
-- replays past it -- which lags if there is a backlog.
DO $$
DECLARE t text;
BEGIN
  SELECT string_agg(format('public.%I', tablename), ', ') INTO t
    FROM pg_tables
   WHERE schemaname = 'public' AND tablename LIKE 'bench\_%'
     AND tablename <> 'bench_payload_pool';
  IF t IS NOT NULL THEN
    EXECUTE 'TRUNCATE TABLE ' || t || ' RESTART IDENTITY';
  END IF;
END $$;
