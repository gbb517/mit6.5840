#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/logs/raft-redis"
PID_DIR="$RUN_DIR/pids"

BACKEND_PID_FILE="$PID_DIR/shardkv_backend.pid"
REDISPROXY_PID_FILE="$PID_DIR/redisproxy.pid"

stop_one() {
    local name="$1"
    local pid_file="$2"

    if [[ ! -f "$pid_file" ]]; then
        echo "[stop] $name pid file not found, skip"
        return 0
    fi

    local pid
    pid="$(cat "$pid_file")"
    if [[ -z "$pid" ]]; then
        rm -f "$pid_file"
        echo "[stop] $name pid empty, cleaned"
        return 0
    fi

    if ! kill -0 "$pid" >/dev/null 2>&1; then
        rm -f "$pid_file"
        echo "[stop] $name already stopped (pid=$pid)"
        return 0
    fi

    echo "[stop] stopping $name (pid=$pid) ..."
    kill "$pid" >/dev/null 2>&1 || true

    for _ in {1..20}; do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            rm -f "$pid_file"
            echo "[stop] $name stopped"
            return 0
        fi
        sleep 0.1
    done

    echo "[stop] force killing $name (pid=$pid)"
    kill -9 "$pid" >/dev/null 2>&1 || true
    rm -f "$pid_file"
}

stop_one "redisproxy" "$REDISPROXY_PID_FILE"
stop_one "shardkv_backend" "$BACKEND_PID_FILE"

echo "[stop] done"
