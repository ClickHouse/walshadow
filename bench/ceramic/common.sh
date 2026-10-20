# Shared paths, ports and helpers for prepare.sh and run.sh. Sourced, not run.
# Defaults keep ports above the Linux ephemeral range (32768-60999), so a
# client socket in TIME-WAIT cannot hold them
PG=${PG:-18}
PGBIN=${PGBIN:-/usr/lib/postgresql/$PG/bin}
export PATH=$PGBIN:$PATH
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Shadow, out, spill and source WAL share DISK, a real disk as in production.
# Source data files, sockets and ClickHouse sit on SHM: in production they are
# other machines, and tmpfs keeps the restore fast
DISK=${DISK:-/var/tmp/walshadow-ceramic}
SHM=${SHM:-/dev/shm/walshadow-ceramic}
DGOLD=$DISK/golden
DRUN=$DISK/run
SGOLD=$SHM/golden
SRUN=$SHM/run
SRC_PORT=${SRC_PORT:-62432}
SHADOW_PORT=${SHADOW_PORT:-62433}
WALSENDER_PORT=${WALSENDER_PORT:-62434}
METRICS=127.0.0.1:${METRICS_PORT:-62484}
CH_TCP=${CH_TCP:-62440}
CH_HTTP=${CH_HTTP:-62441}
CH_DB=ceramic
# taskset cpulists, empty for unpinned. Put the daemon (its shadow inherits),
# the source and ClickHouse on disjoint cores, ideally separate L3 domains
DAEMON_CPUS=${DAEMON_CPUS:-}
SRC_CPUS=${SRC_CPUS:-}
CH_CPUS=${CH_CPUS:-}

psrc() { PGOPTIONS="-c client_min_messages=warning" psql -h "$SRUN/sock" -p $SRC_PORT -U postgres -d postgres -tAXq -v ON_ERROR_STOP=1 "$@"; }
chq() { clickhouse client --port $CH_TCP -q "$@"; }
# awk exits at the match, so curl may die of SIGPIPE
metric() { { curl -s -m 2 "http://$METRICS/metrics" || true; } | awk -v n="$1" '$1==n {print $2; exit}'; }
# CPU ticks per thread name, summed over threads sharing a name
threadcpu() { cat /proc/"$1"/task/*/stat 2>/dev/null | sed 's/^[0-9]* (\([^)]*\)) /\1 /' | awk '{c[$1]+=$13+$14} END{for(t in c) print t, c[t]}' | sort; }
# A child not yet reaped is a zombie, which kill -0 still reports alive
alive() { local s; s=$(ps -o stat= -p "$1" 2>/dev/null) && [[ $s != Z* ]]; }

# Remove run or golden dirs, refusing anything outside $DISK and $SHM
clear_dirs() {
  local d
  for d in "$@"; do
    case $d in "$DISK"/?*|"$SHM"/?*) rm -rf -- "$d" ;; *) echo "refusing to remove $d" >&2; return 1 ;; esac
  done
}

# Servers start with fd 9 closed so they never hold the run lock
ch_start() {
  mkdir -p "$SRUN/ch/data" "$SRUN/ch/logs"
  # Embedded default config, so cwd must hold no config.xml
  (cd "$SRUN/ch" && exec setsid ${CH_CPUS:+taskset -c "$CH_CPUS"} clickhouse server -- \
    --tcp_port=$CH_TCP --http_port=$CH_HTTP --interserver_http_port=$((CH_HTTP + 1)) \
    --mysql_port= --postgresql_port= --grpc_port= --prometheus.port= --listen_host=127.0.0.1 \
    --path="$SRUN/ch/data/" --access_control_path="$SRUN/ch/data/access/" \
    --logger.log="$SRUN/ch/logs/server.log" --logger.errorlog="$SRUN/ch/logs/error.log" \
    --logger.level=warning) </dev/null >/dev/null 2>&1 9>&- &
  for _ in $(seq 300); do chq "SELECT 1" >/dev/null 2>&1 && return; sleep 0.1; done
  echo "clickhouse did not start" >&2; return 1
}
ch_stop() {
  local pat="clickhouse server -- --tcp_port=$CH_TCP "
  pkill -f -- "$pat" || return 0
  for _ in $(seq 100); do pgrep -f -- "$pat" >/dev/null || return 0; sleep 0.1; done
  pkill -9 -f -- "$pat"
}
src_start() { ${SRC_CPUS:+taskset -c "$SRC_CPUS"} pg_ctl -D "$SRUN/src" -l "$SRUN/src.log" -w start >/dev/null 9>&-; }
src_stop() { pg_ctl -D "$SRUN/src" -m "${1:-fast}" stop >/dev/null 2>&1 || true; }

# start_daemon <bin> <bridge-lib-dir> [cpulist]; pools default to production's
#   XACT_BUFFER_MAX=64M  --xact-buffer-max (v0.1.4 default 64M, main 512M)
#   INSERTERS=16 DECODERS=16  [ch] inserter_pool_size, decoder_pool_size
#   TOAST_MODE=shadow    [toast] mode, fixed at bootstrap
start_daemon() {
  local bin=$1 bridge=$2 pin=${3:-}
  cat >"$DRUN/ch-config.toml" <<EOF
[ch]
host = "127.0.0.1"
port = $CH_TCP
database = "$CH_DB"
compression = "lz4"
inserter_pool_size = ${INSERTERS:-16}
decoder_pool_size = ${DECODERS:-16}

[toast]
mode = "${TOAST_MODE:-shadow}"
EOF
  setsid ${pin:+taskset -c "$pin"} "$bin" \
    --host "$SRUN/sock" --port $SRC_PORT --user postgres --dbname postgres --sslmode disable \
    --out-dir "$DRUN/out" --spill-dir "$DRUN/spill" \
    --shadow-socket-dir "$SRUN/shadow-sock" --shadow-port $SHADOW_PORT \
    --shadow-user postgres --shadow-dbname postgres \
    --status-interval 1 --metrics-bind $METRICS \
    --walsender-bind 127.0.0.1:$WALSENDER_PORT --retention-bytes 0 \
    --ch-config "$DRUN/ch-config.toml" \
    --bootstrap-mode direct --bootstrap-shadow-data-dir "$DRUN/shadow-data" \
    --bootstrap-shadow-replay-timeout 600 \
    --bridge-lib-dir "$bridge" \
    ${XACT_BUFFER_MAX:+--xact-buffer-max "$(numfmt --from=iec "$XACT_BUFFER_MAX")"} \
    >>"$DRUN/daemon.log" 2>&1 9>&- &
  DAEMON=$!
}
stop_daemon() {
  [[ -z ${DAEMON:-} ]] && return
  kill -CONT "$DAEMON" 2>/dev/null; kill -TERM "$DAEMON" 2>/dev/null
  for _ in $(seq 300); do alive "$DAEMON" || break; sleep 0.1; done
  alive "$DAEMON" && kill -KILL -- "-$DAEMON" 2>/dev/null
  pg_ctl -D "$DRUN/shadow-data" -m fast stop >/dev/null 2>&1 || true
  DAEMON=
}
teardown() {
  stop_daemon
  pkill -f -- "--metrics-bind $METRICS" 2>/dev/null || true
  pg_ctl -D "$DRUN/shadow-data" -m immediate stop >/dev/null 2>&1 || true
  src_stop immediate
  ch_stop
}
