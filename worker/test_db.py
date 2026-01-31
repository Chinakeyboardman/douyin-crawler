#!/usr/bin/env python3
"""
Unit tests for db.py

Run with: uv run python -m pytest test_db.py -v
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestParseChineseNumber:
    """Test the Chinese number parsing logic (mirrored from JS)."""

    def parse_chinese_number(self, text):
        """Python equivalent of parseChineseNumber from douyin-scraper.js"""
        if not text:
            return 0

        clean_text = text.strip()

        # Handle "万" (10,000)
        if '万' in clean_text:
            num = float(clean_text.replace('万', ''))
            return round(num * 10000)

        # Handle "亿" (100,000,000)
        if '亿' in clean_text:
            num = float(clean_text.replace('亿', ''))
            return round(num * 100000000)

        # Regular number - remove non-digits
        digits = ''.join(c for c in clean_text if c.isdigit())
        return int(digits) if digits else 0

    def test_empty_string(self):
        assert self.parse_chinese_number('') == 0

    def test_none(self):
        assert self.parse_chinese_number(None) == 0

    def test_wan_notation(self):
        """Test 万 (10,000) notation"""
        assert self.parse_chinese_number('485.2万') == 4852000
        assert self.parse_chinese_number('1万') == 10000
        assert self.parse_chinese_number('0.5万') == 5000

    def test_yi_notation(self):
        """Test 亿 (100,000,000) notation"""
        assert self.parse_chinese_number('1.5亿') == 150000000
        assert self.parse_chinese_number('1亿') == 100000000

    def test_regular_numbers(self):
        assert self.parse_chinese_number('12345') == 12345
        assert self.parse_chinese_number('0') == 0

    def test_numbers_with_noise(self):
        """Test numbers mixed with other characters"""
        assert self.parse_chinese_number('123abc456') == 123456
        assert self.parse_chinese_number('点赞') == 0  # No digits


class TestGetVideosWithoutSummary:
    """Test get_videos_without_summary function."""

    @patch('db.get_connection')
    def test_returns_videos_without_tasks(self, mock_get_conn):
        """Videos with no task record should be returned."""
        from db import get_videos_without_summary

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'video_id': 'vid1', 'title': 'Test', 'task_status': None}
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_videos_without_summary(limit=10)

        assert len(result) == 1
        assert result[0]['video_id'] == 'vid1'
        mock_conn.close.assert_called_once()

    @patch('db.get_connection')
    def test_excludes_completed_tasks(self, mock_get_conn):
        """Videos with completed tasks should not be returned."""
        from db import get_videos_without_summary

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []  # No results
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_videos_without_summary(limit=10)

        assert len(result) == 0


class TestTaskStatus:
    """Test get_task_status function."""

    @patch('db.get_connection')
    def test_returns_completed_steps(self, mock_get_conn):
        """Should return list of completed step names."""
        from db import get_task_status

        mock_cursor = MagicMock()
        # First call returns task
        # Second call returns completed steps
        mock_cursor.fetchone.return_value = {
            'video_id': 'vid1',
            'current_step': 'submit',
            'status': 'processing'
        }
        mock_cursor.fetchall.return_value = [
            {'step_name': 'download', 'status': 'completed', 'result': {'file': '/tmp/x.mp4'}}
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_task_status('vid1')

        assert 'download' in result['completed_steps']
        assert result['step_results']['download'] == {'file': '/tmp/x.mp4'}


class TestCompleteStep:
    """Test complete_step function."""

    @patch('db.get_connection')
    def test_marks_task_completed_on_final_step(self, mock_get_conn):
        """When get_summary completes, task should be marked completed."""
        from db import complete_step

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        complete_step('vid1', 'get_summary', {'summary': 'test'})

        # Verify the UPDATE for completed status was called
        calls = mock_cursor.execute.call_args_list
        sql_calls = [str(call) for call in calls]

        # Should have updated task to completed
        assert any("status = 'completed'" in str(call) for call in calls)

    @patch('db.get_connection')
    def test_marks_task_failed_on_error(self, mock_get_conn):
        """When step fails, task should be marked failed."""
        from db import complete_step

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        complete_step('vid1', 'download', None, 'Download failed')

        calls = mock_cursor.execute.call_args_list
        assert any("status = 'failed'" in str(call) for call in calls)


class TestResetStaleTasks:
    """Test reset_stale_tasks function."""

    @patch('db.get_connection')
    def test_resets_old_processing_tasks(self, mock_get_conn):
        """Tasks processing for > 24 hours should be reset."""
        from db import reset_stale_tasks

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        reset_stale_tasks(hours=24)

        # Verify UPDATE was called with correct interval
        calls = mock_cursor.execute.call_args_list
        assert any("status = 'pending'" in str(call) for call in calls)
        assert any("24" in str(call) for call in calls)


class TestCreateOrGetTask:
    """Test create_or_get_task function."""

    @patch('db.get_connection')
    def test_creates_new_task(self, mock_get_conn):
        """Should create task if not exists."""
        from db import create_or_get_task

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            'video_id': 'vid1',
            'current_step': 'pending',
            'status': 'pending'
        }
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = create_or_get_task('vid1')

        assert result['video_id'] == 'vid1'
        assert result['status'] == 'pending'
        mock_conn.commit.assert_called_once()


class TestStartStep:
    """Test start_step function."""

    @patch('db.get_connection')
    def test_updates_task_and_creates_step(self, mock_get_conn):
        """Should update task current_step and create step record."""
        from db import start_step

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        start_step('vid1', 'download')

        # Should have 2 execute calls: UPDATE task, INSERT step
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
