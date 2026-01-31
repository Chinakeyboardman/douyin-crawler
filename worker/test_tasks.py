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

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    @patch('tasks.get_task_status')
    @patch('tasks.create_or_get_task')
    def test_skips_completed_steps(self, mock_create, mock_status, mock_start, mock_complete):
        """Pipeline should skip already completed steps."""
        from tasks import process_video_pipeline

        mock_create.return_value = {'video_id': 'vid1'}
        mock_status.return_value = {
            'completed_steps': ['download'],  # download already done
            'step_results': {'download': {'file_path': '/tmp/vid1.mp4'}}
        }

        # Run pipeline
        result = process_video_pipeline('vid1')

        # Should NOT have started download step
        start_calls = [c[0][1] for c in mock_start.call_args_list]
        assert 'download' not in start_calls
        # Should have started submit and get_summary
        assert 'submit' in start_calls
        assert 'get_summary' in start_calls

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    @patch('tasks.get_task_status')
    @patch('tasks.create_or_get_task')
    def test_runs_all_steps_for_new_video(self, mock_create, mock_status, mock_start, mock_complete):
        """New video should run all 3 steps."""
        from tasks import process_video_pipeline

        mock_create.return_value = {'video_id': 'vid1'}
        mock_status.return_value = {
            'completed_steps': [],
            'step_results': {}
        }

        result = process_video_pipeline('vid1')

        # Should have started all 3 steps
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
    """Test batch processing of pending videos."""

    @patch('tasks.process_video_pipeline')
    @patch('tasks.get_videos_without_summary')
    def test_queues_all_found_videos(self, mock_get_videos, mock_pipeline):
        """Should queue all videos returned by get_videos_without_summary."""
        from tasks import process_pending_videos

        mock_get_videos.return_value = [
            {'video_id': 'vid1'},
            {'video_id': 'vid2'},
            {'video_id': 'vid3'},
        ]
        mock_pipeline.delay = MagicMock()

        result = process_pending_videos(batch_size=10)

        assert result['queued'] == 3
        assert mock_pipeline.delay.call_count == 3

    @patch('tasks.process_video_pipeline')
    @patch('tasks.get_videos_without_summary')
    def test_respects_batch_size(self, mock_get_videos, mock_pipeline):
        """Should pass batch_size to get_videos_without_summary."""
        from tasks import process_pending_videos

        mock_get_videos.return_value = []
        mock_pipeline.delay = MagicMock()

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
    """Test the submit step execution."""

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_uses_download_result(self, mock_start, mock_complete):
        """Submit should receive download result."""
        from tasks import _execute_submit

        download_result = {'file_path': '/tmp/vid1.mp4'}
        result = _execute_submit('vid1', download_result)

        assert 'submission_id' in result
        assert result['status'] == 'success'

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_handles_empty_download_result(self, mock_start, mock_complete):
        """Submit should handle empty download result gracefully."""
        from tasks import _execute_submit

        # This tests the current placeholder implementation
        result = _execute_submit('vid1', {})

        assert result['status'] == 'success'


class TestExecuteGetSummary:
    """Test the get_summary step execution."""

    @patch('tasks.complete_step')
    @patch('tasks.start_step')
    def test_uses_submit_result(self, mock_start, mock_complete):
        """Get summary should receive submit result."""
        from tasks import _execute_get_summary

        submit_result = {'submission_id': 'sub_123'}
        result = _execute_get_summary('vid1', submit_result)

        assert 'summary' in result
        assert result['status'] == 'success'


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
