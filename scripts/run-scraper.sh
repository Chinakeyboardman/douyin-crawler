#!/usr/bin/env bash
#
# 手动运行 douyin-scraper，无超时限制
# 用法: ./scripts/run-scraper.sh [videoCount]
# 示例: ./scripts/run-scraper.sh 100
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRAPE_COUNT="${1:-100}"

cd "$REPO_DIR"

# 使用 caoxiaopeng 连接 PostgreSQL（macOS Homebrew 默认无 postgres 角色）
export PGUSER="${PGUSER:-caoxiaopeng}"
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-douyin}"

# 检查数据库是否可连接
echo "🔍 Checking PostgreSQL..."
if psql -d "$PGDATABASE" -c "SELECT 1" >/dev/null 2>&1; then
  echo "✅ Database '$PGDATABASE' is reachable"
else
  echo "⚠️  Database '$PGDATABASE' not available. Videos will only be saved to files (SAVE_TO_FILE=true)."
  echo "   To fix: ensure PostgreSQL is running and run: node init-db.js"
fi

echo ""
echo "🎬 Starting scraper (no timeout, will run until ${SCRAPE_COUNT} videos collected)"
echo "   PGUSER=$PGUSER PGDATABASE=$PGDATABASE"
echo ""

# 无超时，直接运行
exec node douyin-scraper.js "$SCRAPE_COUNT"
