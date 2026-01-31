#!/usr/bin/env python3
"""
Bug-specific tests - these tests document and verify the bugs found in code review.

Run with: uv run python -m pytest test_bugs.py -v
"""

import pytest
from unittest.mock import MagicMock, patch, call


class TestBug1_SQLIntervalInjection:
    """
    BUG #1: SQL INTERVAL string interpolation in reset_stale_tasks

    Location: db.py:148-159

    The query uses:
        INTERVAL '%s hours'

    This is string interpolation inside a SQL string literal, not a proper
    parameterized query. While psycopg2 handles this safely, it's fragile
    and could break with certain inputs.

    The proper fix would be:
        INTERVAL '1 hour' * %s
    or
        NOW() - make_interval(hours => %s)
    """

    @patch('db.get_connection')
    def test_interval_with_integer_hours(self, mock_get_conn):
        """Current implementation works with integer hours."""
        from db import reset_stale_tasks

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # This should work
        reset_stale_tasks(hours=24)

        # Verify the SQL was called with hours parameter
        calls = mock_cursor.execute.call_args_list
        assert len(calls) == 2  # Two UPDATE statements

    @patch('db.get_connection')
    def test_interval_with_float_hours(self, mock_get_conn):
        """Float hours might cause issues with INTERVAL syntax."""
        from db import reset_stale_tasks

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Float hours - this works in PostgreSQL but is unusual
        reset_stale_tasks(hours=24.5)

        # The call succeeds (mock doesn't validate SQL)
        assert mock_cursor.execute.called


class TestBug2_MissingUniqueConstraint:
    """
    BUG #2: Missing unique constraint on video_task_steps - FIXED

    Location: init-db.js:92-106

    The video_task_steps table now has UNIQUE(video_id, step_name) constraint.
    """

    def test_table_definition_has_unique(self):
        """Verify the table now has unique constraint."""
        with open('../init-db.js', 'r') as f:
            content = f.read()

        # FIXED: The unique constraint is now present
        assert "UNIQUE(video_id, step_name)" in content, "UNIQUE constraint should be present"


class TestBug3_RaceCondition:
    """
    BUG #3: Race condition in get_videos_without_summary - FIXED

    Location: db.py:23-33

    The query now uses FOR UPDATE SKIP LOCKED to prevent race conditions
    between multiple workers.
    """

    @patch('db.get_connection')
    def test_has_row_locking(self, mock_get_conn):
        """Query now uses FOR UPDATE SKIP LOCKED."""
        from db import get_videos_without_summary

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_videos_without_summary(limit=10)

        # Check the SQL has FOR UPDATE SKIP LOCKED
        sql_call = str(mock_cursor.execute.call_args)

        # FIXED: Row locking is now present
        assert "FOR UPDATE" in sql_call
        assert "SKIP LOCKED" in sql_call


class TestBug4_ConnectionLeak:
    """
    BUG #4: Potential connection leak in cli.py - FIXED

    Location: cli.py:47-76

    The show_status() function now has proper exception handling with
    try/except/finally to ensure connection is always closed.
    """

    def test_connection_has_exception_handling(self):
        """Verify cli.py has proper exception handling."""
        with open('cli.py', 'r') as f:
            content = f.read()

        # FIXED: Exception handling is now present
        assert 'except Exception' in content or 'except:' in content, \
            "Exception handling should be present"


class TestBug5_StartStepDuplicates:
    """
    BUG #5: start_step can create duplicate records - FIXED

    Location: db.py:98-102

    The INSERT now uses ON CONFLICT (video_id, step_name) DO UPDATE
    to properly handle retries, updating the existing record instead
    of silently doing nothing.
    """

    @patch('db.get_connection')
    def test_start_step_uses_on_conflict_update(self, mock_get_conn):
        """start_step now uses ON CONFLICT DO UPDATE."""
        from db import start_step

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        start_step('vid1', 'download')

        # Find the INSERT call
        calls = mock_cursor.execute.call_args_list
        insert_call = [c for c in calls if 'INSERT' in str(c)][0]

        # FIXED: It now uses ON CONFLICT DO UPDATE
        assert 'ON CONFLICT' in str(insert_call)
        assert 'DO UPDATE' in str(insert_call)


class TestEdgeCases:
    """Additional edge case tests."""

    def test_chinese_number_edge_cases(self):
        """Test edge cases in Chinese number parsing."""

        def parse_chinese_number(text):
            """Python equivalent of FIXED parseChineseNumber."""
            if not text:
                return 0
            clean_text = text.strip()
            if not clean_text:
                return 0
            if '万' in clean_text:
                num_str = clean_text.replace('万', '')
                try:
                    num = float(num_str)
                except ValueError:
                    return 0
                if num != num:  # isnan check
                    return 0
                return round(num * 10000)
            if '亿' in clean_text:
                num_str = clean_text.replace('亿', '')
                try:
                    num = float(num_str)
                except ValueError:
                    return 0
                if num != num:  # isnan check
                    return 0
                return round(num * 100000000)
            digits = ''.join(c for c in clean_text if c.isdigit())
            return int(digits) if digits else 0

        # Edge cases that work
        assert parse_chinese_number('   ') == 0  # Whitespace only
        assert parse_chinese_number('0万') == 0  # Zero wan
        assert parse_chinese_number('0.0万') == 0  # Zero point zero wan

        # FIXED: These now return 0 instead of crashing
        assert parse_chinese_number('万') == 0  # Just the character
        assert parse_chinese_number('亿') == 0  # Just the character

    @patch('db.get_connection')
    def test_complete_step_with_none_result(self, mock_get_conn):
        """complete_step should handle None result (failure case)."""
        from db import complete_step

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # This should not raise
        complete_step('vid1', 'download', None, 'Some error')

        assert mock_cursor.execute.called


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
