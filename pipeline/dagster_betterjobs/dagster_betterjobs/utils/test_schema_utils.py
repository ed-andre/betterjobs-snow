"""
Quick Test Cases for Schema-as-Code Utility Functions

These are lightweight test cases to validate the core utility functions
work as expected. More comprehensive integration tests will be done
after asset migration.

Usage:
    python -m pytest pipeline/dagster_betterjobs/dagster_betterjobs/utils/test_schema_utils.py

    Or run individual tests:
    from dagster_betterjobs.utils.test_schema_utils import test_extract_object_name_from_file
    test_extract_object_name_from_file()
"""

import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from dagster_betterjobs.utils.schema_utils import (
    extract_object_name_from_file,
    get_schema_layer_from_object_name,
    execute_sql_file
)


def test_extract_object_name_from_file():
    """Test file path to object name conversion using naming convention"""

    # Test cases for different schema layers and object types
    test_cases = [
        # Tables
        ("tables/raw_bamboohr_jobs.sql", "BETTERJOBS_DB.RAW.bamboohr_jobs"),
        ("tables/raw_greenhouse_companies.sql", "BETTERJOBS_DB.RAW.greenhouse_companies"),
        ("tables/stage_jobs_unified.sql", "BETTERJOBS_DB.STAGE.jobs_unified"),
        ("tables/stage_companies_standardized.sql", "BETTERJOBS_DB.STAGE.companies_standardized"),
        ("tables/analytics_job_metrics.sql", "BETTERJOBS_DB.ANALYTICS.job_metrics"),

        # Views
        ("views/stage_jobs_active_view.sql", "BETTERJOBS_DB.STAGE.jobs_active_view"),
        ("views/analytics_company_summary_view.sql", "BETTERJOBS_DB.ANALYTICS.company_summary_view"),

        # Complex names with multiple underscores
        ("tables/stage_llm_skills_raw_extraction.sql", "BETTERJOBS_DB.STAGE.llm_skills_raw_extraction"),
        ("views/analytics_jobs_trending_by_location.sql", "BETTERJOBS_DB.ANALYTICS.jobs_trending_by_location"),
    ]

    for file_path, expected_object_name in test_cases:
        result = extract_object_name_from_file(file_path)
        assert result == expected_object_name, f"Expected {expected_object_name}, got {result} for {file_path}"
        print(f"✅ {file_path} → {result}")


def test_extract_object_name_from_file_with_custom_database():
    """Test object name extraction with custom database name from environment"""

    # Mock environment variable for custom database
    with patch.dict(os.environ, {'SNOWFLAKE_DATABASE': 'CUSTOM_DB'}):
        result = extract_object_name_from_file("tables/raw_bamboohr_jobs.sql")
        expected = "CUSTOM_DB.RAW.bamboohr_jobs"
        assert result == expected, f"Expected {expected}, got {result}"
        print(f"✅ Custom database: {result}")


def test_extract_object_name_from_file_invalid_format():
    """Test error handling for invalid file naming conventions"""

    invalid_cases = [
        "tables/noschema.sql",  # No underscore in filename
        "tables/.sql",  # Empty filename
    ]

    for invalid_file in invalid_cases:
        try:
            extract_object_name_from_file(invalid_file)
            assert False, f"Should have raised ValueError for {invalid_file}"
        except ValueError as e:
            print(f"✅ Correctly caught error for {invalid_file}: {str(e)}")

    # Test that actually valid files work (even if they seem weird)
    valid_weird_cases = [
        ("invalid_file.sql", "BETTERJOBS_DB.INVALID.file"),  # This is actually valid per our convention
        ("test_table.sql", "BETTERJOBS_DB.TEST.table"),     # This works too
        ("tables/no_extension", "BETTERJOBS_DB.NO.extension"),  # No .sql extension but still works
    ]

    for file_path, expected in valid_weird_cases:
        result = extract_object_name_from_file(file_path)
        assert result == expected, f"Expected {expected}, got {result}"
        print(f"✅ Valid weird case: {file_path} → {result}")


def test_get_schema_layer_from_object_name():
    """Test schema layer extraction for error messaging"""

    test_cases = [
        ("BETTERJOBS_DB.RAW.bamboohr_jobs", "raw"),
        ("BETTERJOBS_DB.STAGE.jobs_unified", "stage"),
        ("BETTERJOBS_DB.ANALYTICS.company_metrics", "analytics"),
        ("CUSTOM_DB.DEV.test_table", "dev"),
        ("invalid.format", "unknown"),
        ("", "unknown"),
    ]

    for object_name, expected_schema in test_cases:
        result = get_schema_layer_from_object_name(object_name)
        assert result == expected_schema, f"Expected {expected_schema}, got {result} for {object_name}"
        print(f"✅ {object_name} → schema: {result}")


def test_execute_sql_file_parsing():
    """Test SQL file parsing and statement extraction"""

    # Create a temporary SQL file for testing
    test_sql_content = """-- This is a comment
CREATE TABLE test_table (
    id INTEGER,
    name STRING
);

/* Multi-line comment
   should be ignored */

CREATE VIEW test_view AS
SELECT * FROM test_table;

-- Another comment
INSERT INTO test_table VALUES (1, 'test');"""

    # Mock file operations and Snowflake connection
    mock_context = MagicMock()
    mock_snowflake = MagicMock()
    mock_conn = MagicMock()
    mock_snowflake.get_connection.return_value.__enter__.return_value = mock_conn

    # Mock file reading with proper mock_open
    from unittest.mock import mock_open
    with patch('builtins.open', mock_open(read_data=test_sql_content)):
        result = execute_sql_file(mock_snowflake, "/fake/path/test.sql", mock_context)

    # Verify results
    assert result["status"] == "success"
    assert result["statements_executed"] >= 3  # Should have at least 3 non-comment statements
    assert len(result["results"]) >= 3  # Should have at least 3 statement results

    # Verify Snowflake connection was used
    assert mock_snowflake.get_connection.called
    assert mock_conn.execute.called

    print(f"✅ SQL file parsing: {result['statements_executed']} statements executed")
    print(f"✅ Total statements parsed: {len(result['results'])}")

    # Print parsed statements for debugging
    for i, stmt_result in enumerate(result["results"]):
        print(f"   Statement {i+1}: {stmt_result['statement'][:50]}...")


# Removed mock_open_read_data function - using mock_open directly now


def run_all_tests():
    """Run all test cases manually (for quick validation without pytest)"""

    print("🧪 Running Schema Utils Test Cases...")
    print("=" * 50)

    try:
        test_extract_object_name_from_file()
        print("\n✅ Object name extraction tests passed")

        test_extract_object_name_from_file_with_custom_database()
        print("\n✅ Custom database tests passed")

        test_extract_object_name_from_file_invalid_format()
        print("\n✅ Invalid format error handling tests passed")

        test_get_schema_layer_from_object_name()
        print("\n✅ Schema layer extraction tests passed")

        test_execute_sql_file_parsing()
        print("\n✅ SQL file parsing tests passed")

        print("\n" + "=" * 50)
        print("🎉 All schema utility tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Run tests manually for quick validation
    run_all_tests()