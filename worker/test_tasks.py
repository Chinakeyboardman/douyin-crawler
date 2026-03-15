#!/usr/bin/env python3
"""
Unit tests for tasks.py

Run with: uv run python -m pytest test_tasks.py -v
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


class TestProcessVideoPipeline:
    """Test the video processing pipeline."""

    @patch('tasks.update_video_summary_result')
    @patch('tasks._poll_webgemini_chat')
    @patch('tasks.create_or_update_video_summary')
    @patch('tasks._submit_webgemini_chat')
    @patch('tasks.get_video_by_id_with_local_path')
    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    @patch('tasks.get_task_status')
    @patch('tasks.create_or_get_task')
    def test_skips_completed_steps(
        self, mock_create, mock_status, mock_start, mock_complete,
        mock_get_video, mock_submit, mock_summary, mock_poll, mock_update
    ):
        """Pipeline should skip already completed steps."""
        from tasks import process_video_pipeline

        mock_create.return_value = {'video_id': 'vid1'}
        mock_status.return_value = {
            'completed_steps': ['download'],
            'step_results': {'download': {'file_path': '/tmp/vid1.mp4'}}
        }
        mock_get_video.return_value = {
            'video_id': 'vid1', 'local_file_path': '/tmp/vid1.mp4',
            'share_link': 'https://www.douyin.com/video/vid1',
        }
        mock_submit.return_value = 'job_abc'
        mock_poll.return_value = ('completed', 'Summary text', None)

        with patch('os.path.isfile', return_value=True):
            result = process_video_pipeline('vid1')

        start_calls = [c[0][1] for c in mock_start.call_args_list]
        assert 'download' not in start_calls
        assert 'submit' in start_calls
        assert 'get_summary' in start_calls
        assert result['status'] == 'completed'

    @patch('tasks.update_video_summary_result')
    @patch('tasks._poll_webgemini_chat')
    @patch('tasks.create_or_update_video_summary')
    @patch('tasks._submit_webgemini_chat')
    @patch('tasks.get_video_by_id_with_local_path')
    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    @patch('tasks.get_task_status')
    @patch('tasks.create_or_get_task')
    def test_runs_all_steps_for_new_video(
        self, mock_create, mock_status, mock_start, mock_complete,
        mock_get_video, mock_submit, mock_summary, mock_poll, mock_update
    ):
        """New video should run all 3 steps (download placeholder, submit+get_summary via webgemini)."""
        from tasks import process_video_pipeline

        mock_create.return_value = {'video_id': 'vid1'}
        mock_status.return_value = {'completed_steps': [], 'step_results': {}}
        mock_get_video.return_value = {
            'video_id': 'vid1', 'local_file_path': '/tmp/vid1.mp4',
            'share_link': 'https://www.douyin.com/video/vid1',
        }
        mock_submit.return_value = 'job_abc'
        mock_poll.return_value = ('completed', 'Summary text', None)

        with patch('os.path.isfile', return_value=True):
            result = process_video_pipeline('vid1')

        start_calls = [c[0][1] for c in mock_start.call_args_list]
        assert start_calls == ['download', 'submit', 'get_summary']
        assert result['status'] == 'completed'

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    @patch('tasks.get_task_status')
    @patch('tasks.create_or_get_task')
    def test_stops_on_step_failure(self, mock_create, mock_status, mock_start, mock_complete):
        """Pipeline should stop and return error on step failure."""
        from tasks import process_video_pipeline

        mock_create.return_value = {'video_id': 'vid1'}
        mock_status.return_value = {
            'completed_steps': [],
            'step_results': {}
        }

        # Make start_step raise on 'submit'
        def start_side_effect(video_id, step):
            if step == 'submit':
                raise Exception("Submit failed")

        mock_start.side_effect = start_side_effect

        result = process_video_pipeline('vid1')

        assert result['status'] == 'failed'
        assert result['step'] == 'submit'
        assert 'Submit failed' in result['error']


class TestProcessPendingVideos:
    """Test batch processing of pending videos (webgemini)."""

    @patch('tasks._run_webgemini_summary_for_video')
    @patch('tasks.get_videos_with_local_file_without_summary')
    def test_processes_all_found_videos(self, mock_get_videos, mock_run_summary):
        """Should process all videos returned by get_videos_with_local_file_without_summary."""
        from tasks import process_pending_videos

        mock_get_videos.return_value = [
            {'video_id': 'vid1'},
            {'video_id': 'vid2'},
            {'video_id': 'vid3'},
        ]
        mock_run_summary.return_value = {'status': 'completed'}

        result = process_pending_videos(batch_size=10)

        assert result['total'] == 3
        assert result['completed'] == 3
        assert mock_run_summary.call_count == 3

    @patch('tasks._run_webgemini_summary_for_video')
    @patch('tasks.get_videos_with_local_file_without_summary')
    def test_respects_batch_size(self, mock_get_videos, mock_run_summary):
        """Should pass batch_size to get_videos_with_local_file_without_summary."""
        from tasks import process_pending_videos

        mock_get_videos.return_value = []
        mock_run_summary.return_value = {'status': 'completed'}

        process_pending_videos(batch_size=5)

        mock_get_videos.assert_called_once_with(limit=5)


class TestExecuteDownload:
    """Test the download step execution."""

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_returns_file_path(self, mock_start, mock_complete):
        """Download should return file_path in result."""
        from tasks import _execute_download

        result = _execute_download('vid1')

        assert 'file_path' in result
        assert 'vid1' in result['file_path']
        assert result['status'] == 'success'

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_calls_start_and_complete(self, mock_start, mock_complete):
        """Should call start_step and complete_step."""
        from tasks import _execute_download

        _execute_download('vid1')

        mock_start.assert_called_once_with('vid1', 'download')
        mock_complete.assert_called_once()
        # Verify complete was called with success (no error)
        args = mock_complete.call_args[0]
        assert args[0] == 'vid1'
        assert args[1] == 'download'
        assert args[2] is not None  # result
        assert len(mock_complete.call_args[0]) == 3  # no error param


class TestExecuteSubmit:
    """Test the submit step execution (webgemini)."""

    @patch('tasks.create_or_update_video_summary')
    @patch('tasks._submit_webgemini_chat')
    @patch('tasks.get_video_by_id_with_local_path')
    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_submits_to_webgemini(self, mock_start, mock_complete, mock_get_video, mock_submit, mock_summary):
        """Submit should call webgemini and return job_id."""
        from tasks import _execute_submit

        mock_get_video.return_value = {
            'video_id': 'vid1',
            'local_file_path': '/tmp/vid1.mp4',
            'share_link': 'https://www.douyin.com/video/vid1',
        }
        mock_submit.return_value = 'job_abc123'

        with patch('os.path.isfile', return_value=True):
            result = _execute_submit('vid1', {})

        assert result['webgemini_job_id'] == 'job_abc123'
        assert result['status'] == 'success'
        mock_submit.assert_called_once()

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_fails_without_local_file(self, mock_start, mock_complete):
        """Submit should fail when video has no local file."""
        from tasks import _execute_submit

        with patch('tasks.get_video_by_id_with_local_path', return_value=None):
            with pytest.raises(ValueError, match='not found or has no local_file_path'):
                _execute_submit('vid1', {})


class TestExecuteGetSummary:
    """Test the get_summary step execution (webgemini)."""

    @patch('tasks.update_video_summary_result')
    @patch('tasks._poll_webgemini_chat')
    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_polls_webgemini_and_returns_summary(self, mock_start, mock_complete, mock_poll, mock_update):
        """Get summary should poll webgemini and store result."""
        from tasks import _execute_get_summary

        mock_poll.return_value = ('completed', '这是视频的概括内容', None)
        submit_result = {'webgemini_job_id': 'job_abc123'}

        result = _execute_get_summary('vid1', submit_result)

        assert result['summary'] == '这是视频的概括内容'
        assert result['status'] == 'success'
        mock_poll.assert_called_once_with('job_abc123')
        mock_update.assert_called_once_with('vid1', '这是视频的概括内容', status='completed')

    @patch('tasks.update_video_summary_result')
    @patch('tasks._poll_webgemini_chat')
    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_handles_failed_poll(self, mock_start, mock_complete, mock_poll, mock_update):
        """Get summary should handle failed webgemini poll."""
        from tasks import _execute_get_summary

        mock_poll.return_value = ('failed', None, 'API error')
        submit_result = {'webgemini_job_id': 'job_abc123'}

        result = _execute_get_summary('vid1', submit_result)

        assert result['status'] == 'failed'
        assert result['error'] == 'API error'


class TestResetStaleTasks:
    """Test the reset stale tasks Celery task."""

    @patch('tasks.db_reset_stale_tasks')
    def test_calls_db_reset(self, mock_db_reset):
        """Should call db reset function with 24 hours."""
        from tasks import reset_stale_tasks

        result = reset_stale_tasks()

        mock_db_reset.assert_called_once_with(hours=24)
        assert result['status'] == 'completed'


class TestTriggerBatchNow:
    """Test manual batch trigger."""

    @patch('tasks.process_pending_videos')
    def test_delegates_to_process_pending(self, mock_process):
        """Should call process_pending_videos with batch_size."""
        from tasks import trigger_batch_now

        mock_process.return_value = {'queued': 5}

        result = trigger_batch_now(batch_size=10)

        mock_process.assert_called_once_with(10)


class TestStepOrder:
    """Test that steps are executed in correct order."""

    def test_steps_constant_order(self):
        """STEPS should be in correct order."""
        from tasks import STEPS

        assert STEPS == ['download', 'submit', 'get_summary']
        assert len(STEPS) == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
