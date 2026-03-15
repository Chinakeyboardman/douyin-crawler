#!/usr/bin/env python3
"""
临时脚本：触发 Celery 的 download_yesterday_videos 任务并验证功能。

用法：
  cd /Users/caoxiaopeng/Desktop/git/douyin-crawler/worker
  python ../scripts/trigger_download_yesterday.py

依赖：
  - Redis 运行中
  - Douyin_TikTok_Download_API 服务运行在 127.0.0.1:8000
  - PostgreSQL 可连接
"""

import os
import sys

# 确保 worker 目录在 path 中
WORKER_DIR = os.path.join(os.path.dirname(__file__), '..', 'worker')
os.chdir(WORKER_DIR)
sys.path.insert(0, WORKER_DIR)

def main():
    print("=" * 60)
    print("1. 触发 download_yesterday_videos 任务")
    print("=" * 60)

    from tasks import download_yesterday_videos

    # 使用 apply 同步执行，便于验证（不依赖 Celery worker 进程）
    result = download_yesterday_videos.apply(kwargs={'limit': 20})
    print(f"\n任务结果: {result.result}")
    print()

    print("=" * 60)
    print("2. 验证数据库：昨天视频的 local_file_path")
    print("=" * 60)

    from db import get_connection
    from psycopg2.extras import RealDictCursor

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT video_id, share_link, local_file_path, updated_at
                FROM douyin_videos
                WHERE created_at::date = CURRENT_DATE - INTERVAL '1 day'
                ORDER BY created_at
            """)
            rows = cur.fetchall()

        print(f"\n昨天视频数: {len(rows)}")
        for r in rows:
            has_file = "✅" if r['local_file_path'] else "❌"
            print(f"  {has_file} {r['video_id']}")
            print(f"     share_link: {r['share_link'][:50]}...")
            print(f"     local_file_path: {r['local_file_path'] or '(未下载)'}")
            print()
    finally:
        conn.close()

    print("=" * 60)
    print("验证完成")
    print("=" * 60)


if __name__ == '__main__':
    main()
