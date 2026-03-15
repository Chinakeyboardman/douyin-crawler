#!/bin/bash
#
# Cron 定时任务脚本，无超时限制
# 用法: ./scripts/run-scraper-cron.sh [videoCount]
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="$REPO_DIR/scraper.log"
SCRAPE_COUNT="${1:-100}"

cd "$REPO_DIR"

# 使用 caoxiaopeng 连接 PostgreSQL（macOS Homebrew 默认无 postgres 角色）
export PGUSER="${PGUSER:-caoxiaopeng}"
export PGDATABASE="${PGDATABASE:-douyin}"

{
  echo "=================================================="
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] scraper cron start (count=$SCRAPE_COUNT, no timeout)"
  node douyin-scraper.js "$SCRAPE_COUNT"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] scraper cron end"
} >> "$LOG_FILE" 2>&1
