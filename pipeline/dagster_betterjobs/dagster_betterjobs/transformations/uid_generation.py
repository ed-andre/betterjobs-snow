"""
UID Generation Utilities

This module provides utilities for generating deterministic UIDs for job records
to replace composite key dependencies and provide true uniqueness.
"""

import hashlib
from datetime import datetime, date
from typing import Optional, Union
import pandas as pd


def generate_job_uid(
    job_id: str,
    platform: str,
    company_id: str,
    date_posted: Optional[Union[str, date, datetime]] = None
) -> str:
    """
    Generate deterministic UID for job uniqueness.

    This function creates a consistent UID based on job identifying information,
    ensuring the same job always gets the same UID across processing runs.

    Args:
        job_id: Platform-specific job identifier
        platform: ATS platform name (workday, greenhouse, etc.)
        company_id: Company identifier
        date_posted: Job posting date (optional, for handling reposts)

    Returns:
        32-character hexadecimal UID string (increased from 16 to reduce collisions)

    Example:
        >>> generate_job_uid("12345", "workday", "company_1", "2024-01-15")
        "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
    """
    # Ensure all inputs are strings and handle None values
    job_id_str = str(job_id).strip() if job_id else "NULL_JOB_ID"
    platform_str = str(platform).lower().strip() if platform else "NULL_PLATFORM"
    company_id_str = str(company_id).strip() if company_id else "NULL_COMPANY"

    # Handle date_posted - convert to string format
    if date_posted:
        if isinstance(date_posted, (datetime, date)):
            date_str = date_posted.strftime("%Y-%m-%d")
        else:
            date_str = str(date_posted).strip()
    else:
        date_str = "NULL_DATE"

    # Create composite key with explicit separators to avoid ambiguity
    composite_key = f"JOB_ID:{job_id_str}|PLATFORM:{platform_str}|COMPANY:{company_id_str}|DATE:{date_str}"

    # Generate SHA256 hash and return first 32 characters (128-bit) instead of 16 (64-bit)
    # This dramatically reduces collision probability from 2^64 to 2^128 space
    hash_object = hashlib.sha256(composite_key.encode('utf-8'))
    return hash_object.hexdigest()[:32]


def add_job_uids_to_dataframe(df: pd.DataFrame, uid_column: str = 'job_uid') -> pd.DataFrame:
    """
    Add job UIDs to a DataFrame containing job data.

    Args:
        df: DataFrame with job data
        uid_column: Name of the UID column to add

    Returns:
        DataFrame with UID column added

    Raises:
        ValueError: If required columns are missing
    """
    # Check for required columns
    required_columns = ['job_id', 'platform', 'company_id']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Required columns missing: {missing_columns}")

    # Create a copy to avoid modifying original
    result_df = df.copy()

    # Generate UIDs for each row
    result_df[uid_column] = result_df.apply(
        lambda row: generate_job_uid(
            job_id=row['job_id'],
            platform=row['platform'],
            company_id=row['company_id'],
            date_posted=row.get('date_posted')
        ),
        axis=1
    )

    return result_df


def validate_uid_uniqueness(df: pd.DataFrame, uid_column: str = 'job_uid') -> dict:
    """
    Validate that all UIDs in a DataFrame are unique.

    Args:
        df: DataFrame with UID column
        uid_column: Name of the UID column

    Returns:
        Dictionary with validation results
    """
    if uid_column not in df.columns:
        return {
            'is_valid': False,
            'error': f"Column '{uid_column}' not found in DataFrame"
        }

    total_count = len(df)
    unique_count = df[uid_column].nunique()
    duplicate_count = total_count - unique_count

    # Find duplicates if any
    duplicates = []
    if duplicate_count > 0:
        duplicate_uids = df[df[uid_column].duplicated(keep=False)][uid_column].unique()
        duplicates = duplicate_uids.tolist()

    return {
        'is_valid': duplicate_count == 0,
        'total_records': total_count,
        'unique_uids': unique_count,
        'duplicate_count': duplicate_count,
        'duplicate_uids': duplicates
    }


def get_uid_collision_report(df: pd.DataFrame, uid_column: str = 'job_uid') -> pd.DataFrame:
    """
    Generate a detailed report of UID collisions if any exist.

    Args:
        df: DataFrame with UID column
        uid_column: Name of the UID column

    Returns:
        DataFrame with collision details
    """
    if uid_column not in df.columns:
        return pd.DataFrame()

    # Find duplicated UIDs
    duplicated_mask = df[uid_column].duplicated(keep=False)

    if not duplicated_mask.any():
        return pd.DataFrame()  # No collisions

    # Get records with duplicate UIDs
    collision_records = df[duplicated_mask].copy()

    # Sort by UID for easier analysis
    collision_records = collision_records.sort_values(uid_column)

    # Select relevant columns for the report
    report_columns = [uid_column, 'job_id', 'platform', 'company_id']
    if 'date_posted' in collision_records.columns:
        report_columns.append('date_posted')
    if 'job_title_clean' in collision_records.columns:
        report_columns.append('job_title_clean')

    available_columns = [col for col in report_columns if col in collision_records.columns]

    return collision_records[available_columns]


def generate_company_platform_id(company_name: str, platform: str) -> str:
    """
    Generate deterministic company ID from company name + platform composite.

    This replaces the previous company-name-only ID generation to eliminate
    hash collisions when the same company appears on different ATS platforms.

    Args:
        company_name: Company name (will be normalized)
        platform: ATS platform name (workday, greenhouse, etc.) or "PROFILE" for company profiles

    Returns:
        12-character hexadecimal company ID string

    Example:
        >>> generate_company_platform_id("Google Inc", "workday")
        "a1b2c3d4e5f6"
        >>> generate_company_platform_id("Google Inc", "greenhouse")
        "x9y8z7w6v5u4"  # Different ID for same company on different platform
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(str(company_name).lower().split()) if company_name else "NULL_COMPANY"

    # Normalize platform (lowercase, remove extra spaces)
    normalized_platform = str(platform).lower().strip() if platform else "NULL_PLATFORM"

    # Create composite key with explicit separator to avoid ambiguity
    composite_key = f"{normalized_name}|{normalized_platform}"

    # Generate SHA256 hash and return first 12 characters (48-bit space)
    # This provides good uniqueness while maintaining reasonable ID length
    hash_object = hashlib.sha256(composite_key.encode('utf-8'))
    return hash_object.hexdigest()[:12]


def validate_company_id_uniqueness(df: pd.DataFrame, company_id_column: str = 'company_id') -> dict:
    """
    Validate that all company IDs in a DataFrame are unique.

    Args:
        df: DataFrame with company ID column
        company_id_column: Name of the company ID column

    Returns:
        Dictionary with validation results
    """
    if company_id_column not in df.columns:
        return {
            'is_valid': False,
            'error': f"Column '{company_id_column}' not found in DataFrame"
        }

    total_count = len(df)
    unique_count = df[company_id_column].nunique()
    duplicate_count = total_count - unique_count

    # Find duplicates if any
    duplicates = []
    if duplicate_count > 0:
        duplicate_ids = df[df[company_id_column].duplicated(keep=False)][company_id_column].unique()
        duplicates = duplicate_ids.tolist()

    return {
        'is_valid': duplicate_count == 0,
        'total_records': total_count,
        'unique_company_ids': unique_count,
        'duplicate_count': duplicate_count,
        'duplicate_company_ids': duplicates
    }


# Validation functions for testing
def test_uid_generation():
    """Test UID generation functionality."""
    print("Testing UID Generation...")

    # Test basic job UID generation
    uid1 = generate_job_uid("12345", "workday", "company_1", "2024-01-15")
    uid2 = generate_job_uid("12345", "workday", "company_1", "2024-01-15")

    assert uid1 == uid2, "Same inputs should generate same UID"
    assert len(uid1) == 32, "UID should be 32 characters"

    # Test different inputs generate different UIDs
    uid3 = generate_job_uid("12346", "workday", "company_1", "2024-01-15")
    assert uid1 != uid3, "Different inputs should generate different UIDs"

    # Test company platform ID generation
    company_id1 = generate_company_platform_id("Google Inc", "workday")
    company_id2 = generate_company_platform_id("Google Inc", "workday")

    assert company_id1 == company_id2, "Same company + platform should generate same ID"
    assert len(company_id1) == 12, "Company ID should be 12 characters"

    # Test different platforms generate different IDs for same company
    company_id3 = generate_company_platform_id("Google Inc", "greenhouse")
    assert company_id1 != company_id3, "Same company on different platforms should generate different IDs"

    # Test company profiles platform designation
    profile_id1 = generate_company_platform_id("Google Inc", "PROFILE")
    assert profile_id1 != company_id1, "Profile designation should generate different ID"
    assert len(profile_id1) == 12, "Profile ID should be 12 characters"

    # Test with DataFrame
    test_df = pd.DataFrame({
        'job_id': ['job1', 'job2', 'job1'],
        'platform': ['workday', 'greenhouse', 'workday'],
        'company_id': ['comp1', 'comp2', 'comp1'],
        'date_posted': ['2024-01-01', '2024-01-02', '2024-01-01']
    })

    result_df = add_job_uids_to_dataframe(test_df)
    assert 'job_uid' in result_df.columns, "UID column should be added"
    assert len(result_df) == 3, "Should preserve all rows"

    # Test validation
    validation = validate_uid_uniqueness(result_df)
    expected_unique = len(result_df['job_uid'].unique())
    assert validation['unique_uids'] == expected_unique, "Validation should count unique UIDs correctly"

    # Test company ID validation
    company_df = pd.DataFrame({
        'company_name': ['Google', 'Microsoft', 'Google'],
        'platform': ['workday', 'greenhouse', 'greenhouse'],
    })
    company_df['company_id'] = company_df.apply(
        lambda row: generate_company_platform_id(row['company_name'], row['platform']), axis=1
    )

    company_validation = validate_company_id_uniqueness(company_df)
    assert company_validation['is_valid'], "All company IDs should be unique"
    assert company_validation['unique_company_ids'] == 3, "Should have 3 unique company IDs"

    print("✅ All UID generation tests passed!")
    print("✅ All company ID generation tests passed!")


if __name__ == "__main__":
    test_uid_generation()