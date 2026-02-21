#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/logs/raft-redis"
PID_DIR="$RUN_DIR/pids"
OUT_DIR="$RUN_DIR/out"

CTRL_HOSTS="${CTRL_HOSTS:-127.0.0.1:8101,127.0.0.1:8102,127.0.0.1:8103}"
REDIS_PORT="${REDIS_PORT:-6381}"
CLEAN_BACKEND_STATE="${CLEAN_BACKEND_STATE:-0}"

BACKEND_BIN="$ROOT_DIR/bin/shardkv_backend"
REDISPROXY_BIN="$ROOT_DIR/bin/redisproxy"
BACKEND_PID_FILE="$PID_DIR/shardkv_backend.pid"
REDISPROXY_PID_FILE="$PID_DIR/redisproxy.pid"

mkdir -p "$PID_DIR" "$OUT_DIR" "$ROOT_DIR/logs/backend" "$ROOT_DIR/logs/redisproxy"

is_running() {
    local pid="$1"
    [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

read_pid() {
    local file="$1"
    [[ -f "$file" ]] && cat "$file" || true
}

stop_pid_file_if_running() {
    local file="$1"
    local pid
    pid="$(read_pid "$file")"
    if is_running "$pid"; then
        kill "$pid" >/dev/null 2>&1 || true
        sleep 0.2
        if is_running "$pid"; then
            kill -9 "$pid" >/dev/null 2>&1 || true
        fi
    fi
    rm -f "$file"
}

wait_port() {
    local host="$1"
    local port="$2"
    local timeout_s="${3:-15}"

    local start_ts now
    start_ts=$(date +%s)
    while true; do
        if timeout 1 bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
            return 0
        fi
        now=$(date +%s)
        if (( now - start_ts >= timeout_s )); then
            return 1
        fi
        sleep 0.2
    done
}

kill_stray_by_name() {
    local name="$1"
    local pids
    pids="$(pgrep -f "$ROOT_DIR/bin/$name" || true)"
    if [[ -n "$pids" ]]; then
        echo "[start] cleaning stale $name pids: $pids"
        kill $pids >/dev/null 2>&1 || true
        sleep 0.2
        pids="$(pgrep -f "$ROOT_DIR/bin/$name" || true)"
        if [[ -n "$pids" ]]; then
            kill -9 $pids >/dev/null 2>&1 || true
        fi
    fi
}

wait_ctrl_hosts_stable() {
    local timeout_s="${1:-6}"
    local end_ts now
    end_ts=$(( $(date +%s) + timeout_s ))

    while true; do
        local all_ok=1
        IFS=',' read -ra ctrl_arr <<< "$CTRL_HOSTS"
        for ctrl in "${ctrl_arr[@]}"; do
            local host="${ctrl%:*}"
            local port="${ctrl##*:}"
            if ! timeout 1 bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
                all_ok=0
                break
            fi
        done

        if (( all_ok == 1 )); then
            return 0
        fi

        now=$(date +%s)
        if (( now >= end_ts )); then
            return 1
        fi
        sleep 0.2
    done
}

if [[ ! -x "$BACKEND_BIN" || ! -x "$REDISPROXY_BIN" ]]; then
    echo "[start] missing binaries, building shardkv + redisproxy ..."
    make -C "$ROOT_DIR" shardkv redisproxy
fi

kill_stray_by_name "redisproxy"
kill_stray_by_name "shardkv_backend"

if [[ "$CLEAN_BACKEND_STATE" == "1" ]]; then
    echo "[start] CLEAN_BACKEND_STATE=1, removing persisted backend state ..."
    stop_pid_file_if_running "$REDISPROXY_PID_FILE"
    stop_pid_file_if_running "$BACKEND_PID_FILE"
    rm -rf "$ROOT_DIR/logs/backend/ShardCtrler"* "$ROOT_DIR/logs/backend/ShardKV"*
fi

backend_pid="$(read_pid "$BACKEND_PID_FILE")"
if is_running "$backend_pid"; then
    echo "[start] shardkv_backend already running (pid=$backend_pid)"
else
    echo "[start] launching shardkv_backend ..."
    nohup "$BACKEND_BIN" \
        --cluster_log_dir="$ROOT_DIR/logs/backend" \
        >"$OUT_DIR/shardkv_backend.out" 2>&1 &
    backend_pid=$!
    echo "$backend_pid" >"$BACKEND_PID_FILE"
fi

if ! wait_port 127.0.0.1 8101 20; then
    echo "[start] ERROR: shardctrler port 8101 not ready in time"
    echo "[start] stopping stale redisproxy (if any)"
    stop_pid_file_if_running "$REDISPROXY_PID_FILE"
    exit 1
fi

if ! wait_ctrl_hosts_stable 8; then
    echo "[start] ERROR: shardctrler hosts not stably reachable: $CTRL_HOSTS"
    echo "[start] stopping stale redisproxy (if any)"
    stop_pid_file_if_running "$REDISPROXY_PID_FILE"
    stop_pid_file_if_running "$BACKEND_PID_FILE"
    exit 1
fi

redisproxy_pid="$(read_pid "$REDISPROXY_PID_FILE")"
if is_running "$redisproxy_pid"; then
    echo "[start] redisproxy already running (pid=$redisproxy_pid)"
else
    echo "[start] launching redisproxy on :$REDIS_PORT ..."
    nohup "$REDISPROXY_BIN" \
        --redis_port="$REDIS_PORT" \
        --ctrler_hosts="$CTRL_HOSTS" \
        --redisproxy_log_dir="$ROOT_DIR/logs/redisproxy" \
        >"$OUT_DIR/redisproxy.out" 2>&1 &
    redisproxy_pid=$!
    echo "$redisproxy_pid" >"$REDISPROXY_PID_FILE"
fi

if ! wait_port 127.0.0.1 "$REDIS_PORT" 10; then
    echo "[start] ERROR: redisproxy port $REDIS_PORT not ready in time"
    exit 1
fi

if ! is_running "$backend_pid"; then
    echo "[start] ERROR: shardkv_backend exited unexpectedly"
    echo "[start] stopping redisproxy to avoid backend-unavailable state"
    stop_pid_file_if_running "$REDISPROXY_PID_FILE"
    rm -f "$BACKEND_PID_FILE"
    exit 1
fi

echo "[start] done"
echo "  backend pid:    $backend_pid"
echo "  redisproxy pid: $redisproxy_pid"
echo "  test command:   redis-cli -p $REDIS_PORT PING"
