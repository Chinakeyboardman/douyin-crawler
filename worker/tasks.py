from celery_app import app
from db import (
    get_videos_with_local_file_without_summary,
    get_video_by_id_with_local_path,
    get_videos_without_local_file,
    get_task_status,
    create_or_get_task,
    start_step,
    complete_step,
    reset_stale_tasks as db_reset_stale_tasks,
    get_videos_created_yesterday_without_local_file,
    update_video_local_path,
    create_or_update_video_summary,
    update_video_summary_result,
)
from datetime import datetime
import json
import logging
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request

try:
    from config import DOWNLOAD_API_BASE_URL, DOWNLOAD_SAVE_DIR, WEBGEMINI_API_URL
except ImportError:
    DOWNLOAD_API_BASE_URL = 'http://127.0.0.1:8000'
    DOWNLOAD_SAVE_DIR = './downloads'
    WEBGEMINI_API_URL = 'http://127.0.0.1:8200'

# Repo root (parent of worker/)
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPER_SCRIPT = os.path.join(REPO_DIR, 'douyin-scraper.js')

logger = logging.getLogger(__name__)


def parse_video_id_from_url(url):
    """
    Parse Douyin video link to extract video_id.
    Supports: https://www.douyin.com/video/7611533789604433190,
              https://v.douyin.com/xxx (requires video_id from DB),
              share text containing links.
    """
    if not url or not url.strip():
        return None
    url = url.strip()
    # Full URL: https://www.douyin.com/video/7611533789604433190
    m = re.search(r'douyin\.com/video/(\d+)', url)
    if m:
        return m.group(1)
    # Short link /iesdouyin: video id in path
    m = re.search(r'/(\d{15,})/', url)
    if m:
        return m.group(1)
    return None

STEPS = ['download', 'submit', 'get_summary']

# 统一使用的 webgemini 视频概括 prompt
WEBGEMINI_SUMMARY_PROMPT = "这段视频讲了什么？请简要概括。"


def _run_webgemini_summary_for_video(video_id):
    """
    Inline: submit one video to webgemini, poll until done, store result.
    Used by process_pending_videos for sequential processing (submit one, wait one, next).
    """
    video = get_video_by_id_with_local_path(video_id)
    if not video:
        logger.error("Video %s not found or has no local_file_path", video_id)
        return {'status': 'failed', 'error': 'No local file'}

    local_path = _resolve_video_path(video['local_file_path'])
    if not local_path or not os.path.isfile(local_path):
        logger.error("Video file not found: %s", local_path)
        create_or_update_video_summary(
            video_id, _get_douyin_url(video), status='failed',
        )
        return {'status': 'failed', 'error': f'File not found: {local_path}'}

    douyin_url = _get_douyin_url(video)

    try:
        job_id = _submit_webgemini_chat(WEBGEMINI_SUMMARY_PROMPT, [local_path])
        create_or_update_video_summary(
            video_id, douyin_url, webgemini_job_id=job_id, status='processing',
        )
        logger.info("Submitted to webgemini, job_id=%s", job_id)

        status, text, error = _poll_webgemini_chat(job_id)
        if status == 'completed':
            update_video_summary_result(video_id, text, status='completed')
            logger.info("Webgemini summary completed for %s", video_id)
            return {'status': 'completed', 'video_id': video_id, 'summary': text[:200]}
        else:
            update_video_summary_result(video_id, error or 'Unknown', status='failed')
            logger.error("Webgemini failed for %s: %s", video_id, error)
            return {'status': 'failed', 'video_id': video_id, 'error': error}
    except Exception as e:
        logger.exception("Webgemini summary failed for %s", video_id)
        create_or_update_video_summary(video_id, douyin_url, status='failed')
        return {'status': 'failed', 'video_id': video_id, 'error': str(e)}


@app.task(bind=True, name='tasks.process_pending_videos')
def process_pending_videos(self, batch_size=20):
    """
    Scheduled task: Fetch downloaded videos without webgemini summary, upload to webgemini
    for "这段视频讲了什么" analysis, poll job_id, store result in douyin_video_summaries.
    严格串行：提交一个 -> 等待完成 -> 再处理下一个（不一次性全部提交）。
    """
    logger.info("Starting scheduled job: processing up to %s videos with webgemini (sequential)", batch_size)

    videos = get_videos_with_local_file_without_summary(limit=batch_size)
    # Filter to only videos with valid local files
    valid_videos = []
    for v in videos:
        path = _resolve_video_path(v.get("local_file_path") or "")
        if path and os.path.isfile(path):
            valid_videos.append(v)
        else:
            logger.info("Skipping %s: file not found at %s", v["video_id"], path or v.get("local_file_path"))
    videos = valid_videos
    logger.info("Found %s videos to process (have valid local file, no summary)", len(videos))

    completed = 0
    failed = 0
    results = []

    for video in videos:
        video_id = video['video_id']
        logger.info("Processing video %s for webgemini summary (submit -> wait -> next)", video_id)
        try:
            outcome = _run_webgemini_summary_for_video(video_id)
            if outcome.get('status') == 'completed':
                completed += 1
                results.append({'video_id': video_id, 'status': 'completed'})
            else:
                failed += 1
                results.append({'video_id': video_id, 'status': 'failed', 'error': outcome.get('error', '')})
        except Exception as e:
            failed += 1
            logger.error("Webgemini summary failed for %s: %s", video_id, e)
            results.append({'video_id': video_id, 'status': 'failed', 'error': str(e)})

    return {
        'completed': completed,
        'failed': failed,
        'total': len(videos),
        'results': results,
        'timestamp': datetime.now().isoformat(),
    }


def _resolve_video_path(local_path):
    """Resolve relative path to absolute. Tries REPO_DIR and REPO_DIR/worker (celery cwd)."""
    if not local_path or not local_path.strip():
        return None
    path = local_path.strip()
    if os.path.isabs(path):
        return path
    # Try REPO_DIR first (e.g. DOWNLOAD_SAVE_DIR=./downloads when run from repo root)
    p1 = os.path.normpath(os.path.join(REPO_DIR, path))
    if os.path.isfile(p1):
        return p1
    # Try REPO_DIR/worker (celery worker cwd is worker/)
    p2 = os.path.normpath(os.path.join(REPO_DIR, 'worker', path))
    if os.path.isfile(p2):
        return p2
    return p1  # Return first guess for error message if not found


def _get_douyin_url(video):
    """Get douyin URL from video record."""
    share_link = (video.get('share_link') or '').strip()
    short_link = (video.get('short_link') or '').strip()
    video_id = video['video_id']
    if share_link and ('douyin.com' in share_link or 'iesdouyin.com' in share_link):
        return share_link
    if short_link:
        return short_link
    return f"https://www.douyin.com/video/{video_id}"


def _submit_webgemini_chat(prompt, attachments):
    """POST to webgemini /chat, return job_id."""
    url = f"{WEBGEMINI_API_URL.rstrip('/')}/chat"
    logger.info(
        "[webgemini] POST /chat before: url=%s prompt=%r attachments_count=%d attachments=%s",
        url, prompt[:80] + "..." if len(prompt) > 80 else prompt, len(attachments or []),
        [os.path.basename(p) for p in (attachments or [])],
    )
    data = json.dumps({'prompt': prompt, 'attachments': attachments}).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")
        body = json.loads(resp.read().decode())
        job_id = body.get('job_id')
        logger.info("[webgemini] POST /chat after: job_id=%s status=%s", job_id, body.get('status', 'N/A'))
        return job_id


def _poll_webgemini_chat(job_id, poll_interval=5, max_wait=600):
    """Poll GET /chat/{job_id} until completed or failed. Return (status, text, error)."""
    url = f"{WEBGEMINI_API_URL.rstrip('/')}/chat/{job_id}"
    logger.info("[webgemini] GET /chat/{job_id} before: job_id=%s url=%s", job_id, url)
    start = time.time()
    poll_count = 0
    while time.time() - start < max_wait:
        poll_count += 1
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
        status = body.get('status', '')
        if status == 'completed':
            text = body.get('text') or ''
            logger.info(
                "[webgemini] GET /chat after: job_id=%s status=completed poll_count=%d text_len=%d text_preview=%s",
                job_id, poll_count, len(text), (text[:100] + "...") if len(text) > 100 else text,
            )
            return ('completed', text, None)
        if status == 'failed':
            err = body.get('error') or 'Unknown error'
            logger.info(
                "[webgemini] GET /chat after: job_id=%s status=failed poll_count=%d error=%s",
                job_id, poll_count, err,
            )
            return ('failed', None, err)
        if poll_count <= 3 or poll_count % 10 == 0:
            logger.info("[webgemini] GET /chat polling: job_id=%s poll_count=%d status=%s", job_id, poll_count, status)
        time.sleep(poll_interval)
    logger.info("[webgemini] GET /chat after: job_id=%s status=timeout poll_count=%d", job_id, poll_count)
    return ('failed', None, 'Poll timeout')


@app.task(bind=True, name='tasks.process_webgemini_summary')
def process_webgemini_summary(self, video_id):
    """
    Single-video task: upload to webgemini, poll job_id, store result.
    For batch processing use process_pending_videos (sequential: submit one, wait one, next).
    """
    logger.info("Processing webgemini summary for video %s", video_id)
    return _run_webgemini_summary_for_video(video_id)


@app.task(bind=True, name='tasks.process_video_pipeline')
def process_video_pipeline(self, video_id):
    """
    Process a single video through the pipeline.
    Skips already completed steps. Runs all steps in sequence.
    """
    logger.info(f"Processing video {video_id}")

    # Get or create task
    create_or_get_task(video_id)

    # Get current status
    status = get_task_status(video_id)
    completed_steps = status['completed_steps']
    step_results = status['step_results']

    logger.info(f"Video {video_id} - completed steps: {completed_steps}")

    # Process each step in order, skipping completed ones
    for step in STEPS:
        if step in completed_steps:
            logger.info(f"Skipping {step} - already completed")
            continue

        logger.info(f"Executing step: {step}")

        try:
            if step == 'download':
                result = _execute_download(video_id)
            elif step == 'submit':
                download_result = step_results.get('download', {})
                result = _execute_submit(video_id, download_result)
            elif step == 'get_summary':
                submit_result = step_results.get('submit', {})
                result = _execute_get_summary(video_id, submit_result)

            # Update step_results for next iteration
            step_results[step] = result

        except Exception as e:
            logger.error(f"Step {step} failed for video {video_id}: {e}")
            return {'status': 'failed', 'step': step, 'error': str(e)}

    logger.info(f"Video {video_id} pipeline completed")
    return {'status': 'completed', 'video_id': video_id}


def _execute_download(video_id):
    """
    Step 1: Download video.
    TODO: Implement actual download logic.
    """
    logger.info(f"Downloading video {video_id}")
    start_step(video_id, 'download')

    try:
        # TODO: Implement actual video download logic
        # Example:
        # - Fetch video URL from douyin_videos table
        # - Download video file
        # - Store locally or upload to cloud storage

        result = {
            'video_id': video_id,
            'downloaded_at': datetime.now().isoformat(),
            'file_path': f'/tmp/videos/{video_id}.mp4',  # Placeholder
            'status': 'success'
        }

        complete_step(video_id, 'download', result)
        return result

    except Exception as e:
        logger.error(f"Download failed for {video_id}: {e}")
        complete_step(video_id, 'download', None, str(e))
        raise


def _execute_submit(video_id, download_result):
    """
    Step 2: Submit video to webgemini for summary.
    Uses same logic as _run_webgemini_summary_for_video (submit only).
    """
    logger.info(f"Submitting video {video_id} to webgemini")
    start_step(video_id, 'submit')

    try:
        video = get_video_by_id_with_local_path(video_id)
        if not video:
            raise ValueError(f"Video {video_id} not found or has no local_file_path")

        local_path = _resolve_video_path(video['local_file_path'])
        if not local_path or not os.path.isfile(local_path):
            raise FileNotFoundError(f"Video file not found: {local_path}")

        douyin_url = _get_douyin_url(video)
        job_id = _submit_webgemini_chat(WEBGEMINI_SUMMARY_PROMPT, [local_path])
        create_or_update_video_summary(
            video_id, douyin_url, webgemini_job_id=job_id, status='processing',
        )
        logger.info("Submitted to webgemini, job_id=%s", job_id)

        result = {
            'video_id': video_id,
            'webgemini_job_id': job_id,
            'submitted_at': datetime.now().isoformat(),
            'status': 'success',
        }
        complete_step(video_id, 'submit', result)
        return result

    except Exception as e:
        logger.error(f"Submit failed for {video_id}: {e}")
        complete_step(video_id, 'submit', None, str(e))
        raise


def _execute_get_summary(video_id, submit_result):
    """
    Step 3: Poll webgemini for summary result.
    Uses same logic as _run_webgemini_summary_for_video (poll only).
    """
    logger.info(f"Getting summary for video {video_id} from webgemini")
    start_step(video_id, 'get_summary')

    try:
        job_id = submit_result.get('webgemini_job_id') or submit_result.get('submission_id')
        if not job_id:
            raise ValueError("No webgemini_job_id in submit_result")

        status, text, error = _poll_webgemini_chat(job_id)
        if status == 'completed':
            update_video_summary_result(video_id, text, status='completed')
            logger.info("Webgemini summary completed for %s", video_id)
            result = {
                'video_id': video_id,
                'retrieved_at': datetime.now().isoformat(),
                'summary': text[:500] if text else '',
                'status': 'success',
            }
        else:
            update_video_summary_result(video_id, error or 'Unknown', status='failed')
            logger.error("Webgemini failed for %s: %s", video_id, error)
            result = {
                'video_id': video_id,
                'retrieved_at': datetime.now().isoformat(),
                'error': error,
                'status': 'failed',
            }

        complete_step(video_id, 'get_summary', result)
        return result

    except Exception as e:
        logger.error(f"Get summary failed for {video_id}: {e}")
        complete_step(video_id, 'get_summary', None, str(e))
        raise


@app.task(name='tasks.reset_stale_tasks')
def reset_stale_tasks():
    """Reset tasks that have been stuck in processing state."""
    logger.info("Resetting stale tasks")
    db_reset_stale_tasks(hours=24)
    return {'status': 'completed', 'timestamp': datetime.now().isoformat()}


def _download_videos(videos):
    """Shared logic: download videos via API and update local_file_path."""
    os.makedirs(DOWNLOAD_SAVE_DIR, exist_ok=True)
    success_count = 0
    fail_count = 0
    for v in videos:
        video_id = v['video_id']
        share_link = (v.get('share_link') or '').strip()
        short_link = (v.get('short_link') or '').strip()
        if share_link and ('douyin.com' in share_link or 'iesdouyin.com' in share_link):
            url = share_link
        elif short_link:
            url = short_link
        else:
            url = f"https://www.douyin.com/video/{video_id}"
        download_url = f"{DOWNLOAD_API_BASE_URL.rstrip('/')}/api/download"
        params = f"url={urllib.parse.quote(url)}"
        file_path = os.path.join(DOWNLOAD_SAVE_DIR, f"douyin_{video_id}.mp4")
        try:
            with urllib.request.urlopen(f"{download_url}?{params}", timeout=300) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")
                with open(file_path, 'wb') as f:
                    f.write(resp.read())
            update_video_local_path(video_id, file_path)
            success_count += 1
            logger.info(f"Downloaded video {video_id} -> {file_path}")
        except Exception as e:
            fail_count += 1
            logger.error(f"Download failed for {video_id}: {e}")
    return success_count, fail_count


@app.task(name='tasks.download_pending_videos')
def download_pending_videos(limit=20):
    """
    手动触发：下载未下载的视频（含今天创建的）。
    用于刚爬取完立即下载。
    """
    logger.info("Starting download_pending_videos: fetching videos without local file (include today)")
    videos = get_videos_without_local_file(limit=limit, include_today=True)
    logger.info("Found %s videos to download", len(videos))
    success_count, fail_count = _download_videos(videos)
    return {
        'success': success_count,
        'failed': fail_count,
        'total': len(videos),
        'timestamp': datetime.now().isoformat(),
    }


@app.task(name='tasks.download_yesterday_videos')
def download_yesterday_videos(limit=500):
    """
    每天凌晨5点执行：找出昨天创建且未下载的视频，调用下载服务，将本地路径写入 local_file_path。
    """
    logger.info("Starting download_yesterday_videos: fetching yesterday's videos without local file")
    videos = get_videos_created_yesterday_without_local_file(limit=limit)
    logger.info("Found %s videos to download", len(videos))
    success_count, fail_count = _download_videos(videos)
    return {
        'success': success_count,
        'failed': fail_count,
        'total': len(videos),
        'timestamp': datetime.now().isoformat(),
    }


@app.task(name='tasks.scrape_douyin_daily')
def scrape_douyin_daily(count=100):
    """
    每天凌晨 2 点执行（原 crontab 定时任务）。
    直接调用 node douyin-scraper.js 抓取新视频，不再依赖 server.js。
    """
    count = min(500, max(1, int(count)))
    logger.info("Starting scrape_douyin_daily: running node douyin-scraper.js %s", count)
    if not os.path.isfile(SCRAPER_SCRIPT):
        raise FileNotFoundError(f"Scraper script not found: {SCRAPER_SCRIPT}")
    try:
        env = os.environ.copy()
        env.setdefault('PGUSER', 'caoxiaopeng')
        result = subprocess.run(
            ['node', SCRAPER_SCRIPT, str(count)],
            cwd=REPO_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=None,  # 无超时，直到收集满 count 个视频
        )
        if result.returncode != 0:
            logger.error("Scraper failed: %s", result.stderr or result.stdout)
            return {
                'status': 'failed',
                'returncode': result.returncode,
                'stderr': result.stderr,
                'stdout': result.stdout,
                'timestamp': datetime.now().isoformat(),
            }
        logger.info("Scraper completed successfully")
        return {
            'status': 'completed',
            'count': count,
            'timestamp': datetime.now().isoformat(),
        }
    except subprocess.TimeoutExpired:
        logger.error("Scraper timed out (should not happen with timeout=None)")
        raise


# Manual trigger task (for testing)
@app.task(bind=True, name='tasks.trigger_batch_now')
def trigger_batch_now(self, batch_size=20):
    """Manually trigger batch processing (for testing)."""
    return process_pending_videos(batch_size)
