#!/usr/bin/env bash
# 停止 Celery 进程
# 用法: ./scripts/stop-celery.sh

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$REPO_DIR/celery.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "No PID file. Celery not running?"
  exit 0
fi

PID=$(cat "$PID_FILE")
if kill -0 "$PID" 2>/dev/null; then
  echo "Stopping Celery (PID: $PID)..."
  kill "$PID" 2>/dev/null || true
  sleep 2
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
  echo "✓ Celery stopped"
else
  echo "Process $PID not found. Removing stale PID file."
fi
rm -f "$PID_FILE"
