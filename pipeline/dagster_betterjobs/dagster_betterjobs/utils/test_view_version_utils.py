"""
Unit tests for View Version Management Utilities

Tests the hash-based change detection system for database views.

Part of ENHANCEMENT-029: Hash-Based View Update Management
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from .view_version_utils import (
    calculate_view_content_hash,
    normalize_sql_content,
    view_needs_update,
    extract_view_name_from_file,
    get_objects_directory
)


class TestContentHashing:
    """Test content hashing and normalization functions"""

    def test_content_hash_consistency(self):
        """Test that identical content produces identical hashes"""
        sql1 = "CREATE VIEW test AS SELECT * FROM table"
        sql2 = "CREATE VIEW test AS SELECT * FROM table"

        assert calculate_view_content_hash(sql1) == calculate_view_content_hash(sql2)

    def test_content_normalization(self):
        """Test SQL content normalization removes comments and whitespace"""
        sql_with_comments = """
        -- This is a comment
        CREATE VIEW test AS
        SELECT * FROM table
        -- Another comment
        """

        sql_clean = "CREATE VIEW test AS\nSELECT * FROM table"

        normalized_commented = normalize_sql_content(sql_with_comments)
        normalized_clean = normalize_sql_content(sql_clean)

        assert normalized_commented == normalized_clean

    def test_hash_change_detection(self):
        """Test that content changes produce different hashes"""
        sql1 = "CREATE VIEW test AS SELECT col1 FROM table"
        sql2 = "CREATE VIEW test AS SELECT col1, col2 FROM table"

        assert calculate_view_content_hash(sql1) != calculate_view_content_hash(sql2)

    def test_normalize_sql_content_removes_comments(self):
        """Test that SQL normalization properly removes comments"""
        sql_with_comments = """
        -- Header comment
        CREATE VIEW analytics_test AS
        SELECT
            col1, -- inline comment
            col2
        FROM table_name
        /* Block comment */
        WHERE condition = 1;
        """

        expected = "CREATE VIEW analytics_test AS\nSELECT\ncol1, -- inline comment\ncol2\nFROM table_name\nWHERE condition = 1;"
        result = normalize_sql_content(sql_with_comments)

        # Should remove only line comments starting with --
        assert "-- Header comment" not in result
        assert "/* Block comment */" not in result
        assert "col1, -- inline comment" in result  # Inline comments on same line kept

    def test_normalize_sql_content_removes_empty_lines(self):
        """Test that SQL normalization removes empty lines"""
        sql_with_empty_lines = """
        CREATE VIEW test AS

        SELECT *

        FROM table
        """

        result = normalize_sql_content(sql_with_empty_lines)
        lines = result.split('\n')

        # Should not contain empty lines
        assert all(line.strip() for line in lines)


class TestViewNameExtraction:
    """Test view name extraction from file names"""

    def test_analytics_view_name_extraction(self):
        """Test analytics view name extraction"""
        filename = "analytics_job_metrics.sql"
        expected = "ANALYTICS.JOB_METRICS"

        assert extract_view_name_from_file(filename) == expected

    def test_stage_view_name_extraction(self):
        """Test stage view name extraction"""
        filename = "stage_data_validation_report.sql"
        expected = "STAGE.DATA_VALIDATION_REPORT"

        assert extract_view_name_from_file(filename) == expected

    def test_raw_view_name_extraction(self):
        """Testraw view name extraction"""
        filename = "raw_job_postings_view.sql"
        expected = "RAW.JOB_POSTINGS_VIEW"

        assert extract_view_name_from_file(filename) == expected

    def test_default_schema_extraction(self):
        """Test default schema for unprefixed files"""
        filename = "custom_view.sql"
        expected = "ANALYTICS.CUSTOM_VIEW"

        assert extract_view_name_from_file(filename) == expected

    def test_complex_view_name_extraction(self):
        """Test complex view names with underscores"""
        filename = "analytics_weekly_market_summary.sql"
        expected = "ANALYTICS.WEEKLY_MARKET_SUMMARY"

        assert extract_view_name_from_file(filename) == expected


class TestViewUpdateLogic:
    """Test view update decision logic"""

    @patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash')
    def test_view_needs_update_when_hash_differs(self, mock_get_hash):
        """Test that view needs update when hashes differ"""
        mock_get_hash.return_value = "old_hash_123"
        mock_snowflake = Mock()

        result = view_needs_update("ANALYTICS.TEST", "new_hash_456", mock_snowflake)

        assert result is True
        mock_get_hash.assert_called_once_with("ANALYTICS.TEST", mock_snowflake)

    @patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash')
    def test_view_does_not_need_update_when_hash_same(self, mock_get_hash):
        """Test that view doesn't need update when hashes are same"""
        mock_get_hash.return_value = "same_hash_123"
        mock_snowflake = Mock()

        result = view_needs_update("ANALYTICS.TEST", "same_hash_123", mock_snowflake)

        assert result is False
        mock_get_hash.assert_called_once_with("ANALYTICS.TEST", mock_snowflake)

    @patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash')
    def test_view_needs_update_when_no_stored_hash(self, mock_get_hash):
        """Test that view needs update when no stored hash exists"""
        mock_get_hash.return_value = None
        mock_snowflake = Mock()

        result = view_needs_update("ANALYTICS.NEW_VIEW", "new_hash_123", mock_snowflake)

        assert result is True
        mock_get_hash.assert_called_once_with("ANALYTICS.NEW_VIEW", mock_snowflake)


class TestDatabaseInteraction:
    """Test database interaction functions"""

    def test_get_stored_view_hash_with_result(self):
        """Test retrieving stored hash when record exists"""
        # Mock database connection and cursor
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("stored_hash_123",)

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)

        mock_snowflake = Mock()
        mock_snowflake.get_connection.return_value = mock_conn

        from .view_version_utils import get_stored_view_hash
        result = get_stored_view_hash("ANALYTICS.TEST", mock_snowflake)

        assert result == "stored_hash_123"
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_get_stored_view_hash_no_result(self):
        """Test retrieving stored hash when no record exists"""
        # Mock database connection and cursor
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)

        mock_snowflake = Mock()
        mock_snowflake.get_connection.return_value = mock_snowflake

        from .view_version_utils import get_stored_view_hash
        result = get_stored_view_hash("ANALYTICS.NONEXISTENT", mock_snowflake)

        assert result is None


class TestPathUtilities:
    """Test path and directory utilities"""

    def test_get_objects_directory(self):
        """Test that objects directory path is correctly constructed"""
        result = get_objects_directory()

        # Should end with pipeline/sql/objects
        assert result.name == "objects"
        assert result.parent.name == "sql"
        assert result.parent.parent.name == "pipeline"

    def test_get_objects_directory_is_path(self):
        """Test that objects directory returns Path object"""
        result = get_objects_directory()

        assert isinstance(result, Path)


class TestIntegrationScenarios:
    """Test end-to-end integration scenarios"""

    def test_new_view_workflow(self):
        """Test complete workflow for new view"""
        sql_content = "CREATE VIEW analytics_test AS SELECT * FROM table"
        content_hash = calculate_view_content_hash(sql_content)

        # Mock no existing hash (new view)
        with patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash') as mock_get_hash:
            mock_get_hash.return_value = None
            mock_snowflake = Mock()

            needs_update = view_needs_update("ANALYTICS.TEST", content_hash, mock_snowflake)

            assert needs_update is True
            assert len(content_hash) == 32  # MD5 hash length

    def test_unchanged_view_workflow(self):
        """Test complete workflow for unchanged view"""
        sql_content = "CREATE VIEW analytics_test AS SELECT * FROM table"
        content_hash = calculate_view_content_hash(sql_content)

        # Mock existing hash (unchanged view)
        with patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash') as mock_get_hash:
            mock_get_hash.return_value = content_hash
            mock_snowflake = Mock()

            needs_update = view_needs_update("ANALYTICS.TEST", content_hash, mock_snowflake)

            assert needs_update is False

    def test_changed_view_workflow(self):
        """Test complete workflow for changed view"""
        old_sql = "CREATE VIEW analytics_test AS SELECT col1 FROM table"
        new_sql = "CREATE VIEW analytics_test AS SELECT col1, col2 FROM table"

        old_hash = calculate_view_content_hash(old_sql)
        new_hash = calculate_view_content_hash(new_sql)

        # Verify hashes are different
        assert old_hash != new_hash

        # Mock existing old hash
        with patch('pipeline.dagster_betterjobs.dagster_betterjobs.utils.view_version_utils.get_stored_view_hash') as mock_get_hash:
            mock_get_hash.return_value = old_hash
            mock_snowflake = Mock()

            needs_update = view_needs_update("ANALYTICS.TEST", new_hash, mock_snowflake)

            assert needs_update is True


if __name__ == "__main__":
    pytest.main([__file__])