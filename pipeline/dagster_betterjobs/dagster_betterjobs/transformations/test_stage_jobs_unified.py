"""
Test module for stage_jobs_unified asset functionality.

This module tests the core transformation logic used in the stage_jobs_unified asset.
"""

import pandas as pd
import json
from datetime import datetime, date
import pytest

from dagster_betterjobs.transformations.text_cleaning import clean_text_fields
from dagster_betterjobs.transformations.language_detection import LanguageDetector
from dagster_betterjobs.transformations.platform_mapping import PlatformMapper


def test_stage_transformation_pipeline():
    """Test the complete transformation pipeline for stage jobs unified."""

    # Sample raw data simulating different platforms
    raw_bamboohr_data = pd.DataFrame([
        {
            'job_id': 'bamboo_001',
            'company_id': 'company_1',
            'job_title': '  Software Engineer   ',
            'job_description': '<p>We are looking for a <strong>Python developer</strong>...</p>',
            'job_url': 'https://company1.bamboohr.com/job/001',
            'location': 'San Francisco, CA',
            'department': 'Engineering',
            'employment_status': 'Full-time',
            'date_posted': '2024-01-15',
            'date_retrieved': '2024-01-16 10:00:00',
            'is_active': True,
            'raw_data': '{"original": "data"}',
            'partition_key': 'A',
            'compensation': '$120k-160k',
            'work_type': 'Remote'
        }
    ])

    raw_greenhouse_data = pd.DataFrame([
        {
            'job_id': 'greenhouse_001',
            'company_id': 'company_2',
            'job_title': 'Senior Data Scientist',
            'job_description': 'Join our team to work on machine learning projects...',
            'job_url': 'https://company2.greenhouse.io/job/001',
            'location': 'New York, NY',
            'department': 'Data Science',
            'department_id': 'dept_123',
            'published_at': '2024-01-16',
            'date_retrieved': '2024-01-17 10:00:00',
            'updated_at': '2024-01-17 10:00:00',
            'requisition_id': 'REQ123',
            'is_active': True,
            'raw_data': '{"source": "greenhouse"}',
            'partition_key': 'A',
            'work_type': 'Hybrid',
            'compensation': '$140k-180k'
        }
    ])

    # Initialize transformation modules
    language_detector = LanguageDetector(confidence_threshold=0.8)
    platform_mapper = PlatformMapper()

    # Test transformation pipeline for each platform
    platforms_data = [
        ('bamboohr', raw_bamboohr_data),
        ('greenhouse', raw_greenhouse_data)
    ]

    transformed_data = []

    for platform, raw_df in platforms_data:
        print(f"\nTesting {platform} transformation pipeline...")

        # Step 1: Text cleaning
        cleaned_df = clean_text_fields(raw_df)

        # Verify text cleaning worked
        assert 'job_title_clean' in cleaned_df.columns
        assert 'job_description_clean' in cleaned_df.columns
        assert len(cleaned_df['job_title_clean'].iloc[0]) > 0  # Has content
        assert len(cleaned_df['job_description_clean'].iloc[0]) > 0  # Has content

        # Step 2: Language detection
        language_df = language_detector.process_dataframe(cleaned_df, 'job_description_clean')

        # Verify language detection
        assert 'detected_language' in language_df.columns
        assert 'is_english' in language_df.columns
        assert 'language_confidence' in language_df.columns
        assert language_df['detected_language'].iloc[0] == 'en'
        assert language_df['is_english'].iloc[0] == True

        # Step 3: Platform field mapping
        unified_df = platform_mapper.map_platform_data(language_df, platform)

        # Verify platform mapping
        assert 'platform' in unified_df.columns
        assert 'date_posted' in unified_df.columns
        assert 'source_raw_table' in unified_df.columns
        assert unified_df['platform'].iloc[0] == platform
        assert unified_df['source_raw_table'].iloc[0] == f'RAW.{platform.upper()}_JOBS'

        # Verify platform-specific data preservation
        assert 'platform_specific_data' in unified_df.columns
        platform_data = json.loads(unified_df['platform_specific_data'].iloc[0])
        print(f"Platform specific data: {platform_data}")
        print(f"Platform: {platform}")

        # Check that platform-specific data contains expected fields for each platform
        if platform == 'bamboohr':
            assert 'employment_status' in platform_data
            assert 'compensation' in platform_data
            assert 'work_type' in platform_data
        elif platform == 'greenhouse':
            assert 'department_id' in platform_data
            assert 'requisition_id' in platform_data

        transformed_data.append(unified_df)

        print(f"✓ {platform} transformation completed successfully")

    # Test combining multiple platforms
    combined_df = pd.concat(transformed_data, ignore_index=True)

    # Debug: Print columns in combined DataFrame
    print(f"Combined DF columns: {list(combined_df.columns)}")

    # Verify combined data
    assert len(combined_df) == 2  # Two jobs from two platforms
    assert set(combined_df['platform'].unique()) == {'bamboohr', 'greenhouse'}
    assert len(combined_df['job_id'].unique()) == 2  # No duplicates

    # Test data quality validation
    critical_fields = ['job_id', 'job_title_clean', 'platform', 'company_id']
    for field in critical_fields:
        missing_count = combined_df[field].isna().sum()
        assert missing_count == 0, f"Missing values found in critical field: {field}"

    # Test quality score calculation
    total_fields = len(combined_df.columns)
    quality_scores = combined_df.apply(
        lambda row: (total_fields - row.isna().sum()) / total_fields, axis=1
    )

    assert all(score >= 0.8 for score in quality_scores), "Data quality scores too low"

    print("\n✓ Complete stage transformation pipeline test passed!")

    return combined_df


def test_unified_schema_structure():
    """Test that the unified schema has all required fields."""

    # Sample data
    raw_data = pd.DataFrame([{
        'job_id': 'test_001',
        'company_id': 'test_company',
        'job_title': 'Test Job',
        'job_description': 'This is a test job description.',
        'job_url': 'https://test.com/job/001',
        'location': 'Test City, ST',
        'department': 'Test Department',
        'employment_status': 'Full-time',
        'date_posted': '2024-01-15',
        'date_retrieved': '2024-01-16 10:00:00',
        'is_active': True,
        'raw_data': '{}',
        'partition_key': 'A'
    }])

    # Apply transformations
    language_detector = LanguageDetector()
    platform_mapper = PlatformMapper()

    cleaned_df = clean_text_fields(raw_data)
    language_df = language_detector.process_dataframe(cleaned_df, 'job_description_clean')
    unified_df = platform_mapper.map_platform_data(language_df, 'bamboohr')

    # Check required unified schema fields
    required_fields = [
        'job_id', 'company_id', 'platform',
        'job_title_clean', 'job_description_clean', 'location_standardized',
        'job_url', 'date_posted', 'is_active', 'employment_status',
        'detected_language', 'language_confidence', 'is_english',
        'platform_specific_data', 'source_raw_table'
    ]

    for field in required_fields:
        assert field in unified_df.columns, f"Required field missing: {field}"

    # Verify data types are appropriate for Snowflake
    assert isinstance(unified_df['is_active'].iloc[0], bool)
    assert isinstance(unified_df['is_english'].iloc[0], bool)
    assert isinstance(unified_df['language_confidence'].iloc[0], float)

    print("✓ Unified schema structure test passed!")


def test_error_handling():
    """Test error handling in transformation pipeline."""

    # Test with missing critical fields
    incomplete_data = pd.DataFrame([{
        'job_id': None,  # Missing critical field
        'job_title': 'Test Job',
        'job_description': 'Test description'
    }])

    # Transformations should handle missing data gracefully
    cleaned_df = clean_text_fields(incomplete_data)

    # Should still have the cleaned fields
    assert 'job_title_clean' in cleaned_df.columns
    assert 'job_description_clean' in cleaned_df.columns

    # Test with non-English content
    non_english_data = pd.DataFrame([{
        'job_id': 'spanish_001',
        'company_id': 'test_company',
        'job_title': 'Ingeniero de Software',
        'job_description': 'Buscamos un desarrollador con experiencia en Python...',
        'job_url': 'https://test.com/job/spanish',
        'location': 'Madrid, España',
        'date_retrieved': '2024-01-16 10:00:00',
        'is_active': True,
        'raw_data': '{}'
    }])

    language_detector = LanguageDetector()
    cleaned_df = clean_text_fields(non_english_data)
    language_df = language_detector.process_dataframe(cleaned_df, 'job_description_clean')

    # Should detect as non-English
    assert language_df['detected_language'].iloc[0] != 'en'
    assert language_df['is_english'].iloc[0] == False

    print("✓ Error handling test passed!")


if __name__ == "__main__":
    print("Running stage_jobs_unified transformation tests...")

    # Run tests
    test_stage_transformation_pipeline()
    test_unified_schema_structure()
    test_error_handling()

    print("\n🎉 All stage_jobs_unified tests passed!")