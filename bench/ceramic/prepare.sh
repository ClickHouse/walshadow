#!/usr/bin/env bash
# Build the golden snapshot run.sh restores: seeded source, bootstrapped shadow
# ([toast] mode shadow), walshadow state and ClickHouse data, daemon stopped,
# a fixed WAL backlog generated behind it.
#
#   ./prepare.sh [boot-tree]     (default: this checkout)
#
# Bootstrap with the oldest tree you will compare: newer trees resume its state.
#
# Knobs (SQL files are swappable; see workload.sql for the variables it gets):
#   SCHEMA=file SEED=file WORKLOAD=file   (default schema.sql seed.sql workload.sql)
#   SEED_ROWS=N        hot-table rows with out-of-line payload_a and payload_b (default 300000)
#   UPD_REPEAT=X       status transitions per hot insert (default 3.5, as measured)
#   UPD_RECENT_FRAC=X  share of transitions aimed at the last RECENT_IDS hot rows
#                      rather than random seeded rows (default 0.5)
#   RECENT_IDS=N       recency window in hot-table ids (default 20000)
#   GEN_SECS=N         backlog seconds (default 40)
#   TPS=N              script runs/s while generating (default 8000)
#   CLIENTS=N          pgbench clients (default 32)
#   CHECKPOINT_S=N     CHECKPOINT interval while generating, sets the FPI share (default 2)
#   PG=18|17           source and shadow major (default 18)
set -euo pipefail
. "$(dirname "$0")/common.sh"
TREE=$(cd "${1:-$HERE/../..}" && pwd)
SCHEMA=${SCHEMA:-$HERE/schema.sql}
SEED=${SEED:-$HERE/seed.sql}
WORKLOAD=${WORKLOAD:-$HERE/workload.sql}
SEED_ROWS=${SEED_ROWS:-300000}
UPD_REPEAT=${UPD_REPEAT:-3.5}
UPD_RECENT_FRAC=${UPD_RECENT_FRAC:-0.5}
RECENT_IDS=${RECENT_IDS:-20000}
GEN_SECS=${GEN_SECS:-40}
TPS=${TPS:-8000}
CLIENTS=${CLIENTS:-32}
CHECKPOINT_S=${CHECKPOINT_S:-2}
BIN=$TREE/target/release/walshadow-stream
[[ -x $BIN ]] || BIN=$TREE/target/fast/release/walshadow-stream
BRIDGE=$("$HERE/pgext.sh" "$TREE" "$PG")

mkdir -p "$DISK"; exec 9>"$DISK/.lock"; flock 9
trap teardown EXIT
teardown
clear_dirs "$DRUN" "$DGOLD" "$SRUN" "$SGOLD"
mkdir -p "$DRUN"/{out,spill} "$SRUN"/{sock,shadow-sock}

# Checksums as on managed Postgres, so hint bits log FPI_FOR_HINT
initdb -D "$SRUN/src" --waldir "$DRUN/srcwal" -U postgres --data-checksums --no-sync >/dev/null
cat >>"$SRUN/src/postgresql.conf" <<EOF
port = $SRC_PORT
unix_socket_directories = '$SRUN/sock'
listen_addresses = ''
wal_level = logical
max_wal_senders = 8
wal_keep_size = '200GB'
max_wal_size = '64GB'
checkpoint_timeout = '1h'
shared_buffers = '2GB'
full_page_writes = on
wal_compression = lz4
default_toast_compression = lz4
synchronous_commit = off
max_connections = 300
EOF
src_start
psrc -f "$SCHEMA" >/dev/null
psrc -f "$HERE/pool.sql" >/dev/null
psrc -v rows="$SEED_ROWS" -f "$SEED"
psrc -c "VACUUM ANALYZE" -c CHECKPOINT
HOT=ceramic_items
TOAST=$(psrc -c "select reltoastrelid::regclass from pg_class where oid = '$HOT'::regclass")
echo "seeded $SEED_ROWS rows: heap $(psrc -c "select pg_size_pretty(pg_relation_size('$HOT'))"), toast" \
  "$(psrc -c "select pg_size_pretty(pg_relation_size('$TOAST')) || ', ' || round(count(*)::numeric / $SEED_ROWS, 2) || ' chunks/row' from $TOAST")"

ch_start
chq "CREATE DATABASE $CH_DB"
start_daemon "$BIN" "$BRIDGE"
for _ in $(seq 900); do
  grep -q "starting pump" "$DRUN/daemon.log" && break
  alive "$DAEMON" || { tail -30 "$DRUN/daemon.log"; exit 1; }
  sleep 1
done
sleep 3
stop_daemon
echo "bootstrapped with $(git -C "$TREE" rev-parse --short HEAD), daemon stopped"

# Keep WAL from the stopped daemon's resume floor only
FLOOR=$(awk -F'"' '/^floor/ {print $2}' "$DRUN/spill/manifest.toml")
KEEP_MB=$(psrc -c "select pg_wal_lsn_diff(pg_current_wal_lsn(), '$FLOOR')::bigint / 1048576 + 32")
psrc -c "ALTER SYSTEM SET wal_keep_size = $KEEP_MB" -c "select pg_reload_conf(), pg_sleep(1)" -c CHECKPOINT >/dev/null
psrc -c "ALTER SYSTEM RESET wal_keep_size" -c "select pg_reload_conf()" >/dev/null
START_LSN=$(psrc -c "select pg_current_wal_lsn()")
psrc -c "select pg_stat_reset_shared('wal')" >/dev/null
( while sleep "$CHECKPOINT_S"; do psrc -c CHECKPOINT 2>/dev/null; done ) & CKPT=$!
pgbench -h "$SRUN/sock" -p $SRC_PORT -U postgres -d postgres -n -M prepared -f "$WORKLOAD" \
  -D seed_rows="$SEED_ROWS" -D recent_ids="$RECENT_IDS" -D upd_pm=910 \
  -D ins_pm="$(bc <<<"910 / $UPD_REPEAT")" -D recent_pm="$(bc <<<"1000 * $UPD_RECENT_FRAC / 1")" \
  -R "$TPS" -c "$CLIENTS" -j 16 -T "$GEN_SECS" >"$DRUN/pgbench.log" 2>&1 || { tail "$DRUN/pgbench.log"; exit 1; }
kill $CKPT
grep -E "^(tps|number of transactions actually)" "$DRUN/pgbench.log"
FPI_SHARE=$(psrc -c "select round(100.0 * wal_fpi / greatest(wal_records, 1), 1) from pg_stat_wal")
# Idle source during drain, so every run sees the identical WAL
psrc -c "ALTER SYSTEM SET autovacuum = off" -c "select pg_reload_conf()" >/dev/null
END_LSN=$(psrc -c "select pg_current_wal_lsn()")
BACKLOG=$(psrc -c "select pg_wal_lsn_diff('$END_LSN', '$START_LSN')::bigint")
src_stop
pg_waldump -p "$DRUN/srcwal" -s "$START_LSN" -e "$END_LSN" --stats=record >"$DRUN/waldump.txt" 2>/dev/null || true
RECORDS=$(awk '$1=="Total" {print $2; exit}' "$DRUN/waldump.txt")
# Segments past the shutdown checkpoint are recycled files PG overwrites
END_SEG=$(pg_controldata "$SRUN/src" | awk -F': *' '/REDO WAL file/ {print $2}')
for f in $(ls "$DRUN/srcwal" | grep -E '^[0-9A-F]{24}$'); do [[ $f > $END_SEG ]] && rm -f "$DRUN/srcwal/$f"; done
ch_stop
trap - EXIT
cat >"$DRUN/meta.env" <<EOF
BOOT_REV=$(git -C "$TREE" rev-parse --short HEAD) GOLD_PG=$PG
GOLDEN="pg$PG seed=$SEED_ROWS repeat=$UPD_REPEAT recent=$UPD_RECENT_FRAC/$RECENT_IDS gen=${GEN_SECS}s tps=$TPS ckpt=${CHECKPOINT_S}s fpi=$FPI_SHARE% $(basename "$WORKLOAD")"
START_LSN=$START_LSN
BACKLOG=$BACKLOG
RECORDS=$RECORDS
END_SEG=$END_SEG
EOF
mv "$DRUN" "$DGOLD"; mv "$SRUN" "$SGOLD"
sync
printf 'golden: %.0f MiB WAL, %s records, %.0f B/rec, FPI %s%% of records, %s on disk, %s on tmpfs\n' \
  "$(bc -l <<<"$BACKLOG/1048576")" "$RECORDS" "$(bc -l <<<"$BACKLOG/$RECORDS")" "$FPI_SHARE" \
  "$(du -sh "$DGOLD" | cut -f1)" "$(du -sh "$SGOLD" | cut -f1)"
"$HERE/walmix.sh" "$DGOLD/waldump.txt"
