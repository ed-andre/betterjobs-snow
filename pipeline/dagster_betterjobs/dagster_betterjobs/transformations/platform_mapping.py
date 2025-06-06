"""
Platform Field Mapping Module

This module provides standardized field mapping from platform-specific raw schemas
to the unified STAGE layer schema. It handles field name variations, data type
conversions, and platform-specific data preservation.

Usage:
    from transformations.platform_mapping import PlatformMapper

    mapper = PlatformMapper()
    standardized_data = mapper.map_platform_data(raw_data, platform='workday')
"""

import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime
import json


class PlatformMapper:
    """
    Handles mapping from platform-specific raw data to standardized STAGE schema.
    """

    def __init__(self):
        """Initialize the platform mapper with field mappings."""
        self.platform_mappings = self._initialize_mappings()

    def _initialize_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Initialize field mappings for each platform.

        Returns:
            Dictionary mapping platform names to field mapping dictionaries
        """
        return {
            'bamboohr': {
                # Standard fields
                'job_id': 'JOB_ID',
                'company_id': 'COMPANY_ID',
                'job_title': 'JOB_TITLE',
                'job_description': 'JOB_DESCRIPTION',
                'job_url': 'JOB_URL',
                'location': 'LOCATION',
                'department': 'DEPARTMENT',
                'date_posted': 'DATE_POSTED',  # BambooHR uses DATE_POSTED
                'date_retrieved': 'DATE_RETRIEVED',
                'is_active': 'IS_ACTIVE',
                'raw_data': 'RAW_DATA',
                'partition_key': 'PARTITION_KEY',
                # Platform-specific fields
                'employment_status': 'EMPLOYMENT_STATUS',
                'compensation': 'COMPENSATION',
                'work_type': 'WORK_TYPE'
            },
            'greenhouse': {
                # Standard fields
                'job_id': 'JOB_ID',
                'company_id': 'COMPANY_ID',
                'job_title': 'JOB_TITLE',
                'job_description': 'JOB_DESCRIPTION',
                'job_url': 'JOB_URL',
                'location': 'LOCATION',
                'department': 'DEPARTMENT',
                'date_posted': 'PUBLISHED_AT',  # Greenhouse uses PUBLISHED_AT
                'date_retrieved': 'DATE_RETRIEVED',
                'is_active': 'IS_ACTIVE',
                'raw_data': 'RAW_DATA',
                'partition_key': 'PARTITION_KEY',
                # Platform-specific fields
                'department_id': 'DEPARTMENT_ID',
                'updated_at': 'UPDATED_AT',
                'requisition_id': 'REQUISITION_ID',
                'work_type': 'WORK_TYPE',
                'compensation': 'COMPENSATION'
            },
            'smartrecruiters': {
                # Standard fields
                'job_id': 'JOB_ID',
                'company_id': 'COMPANY_ID',
                'job_title': 'JOB_TITLE',
                'job_description': 'JOB_DESCRIPTION',
                'job_url': 'JOB_URL',
                'location': 'LOCATION',
                'department': 'DEPARTMENT',
                'date_posted': 'PUBLISHED_AT',  # SmartRecruiters uses PUBLISHED_AT
                'date_retrieved': 'DATE_RETRIEVED',
                'is_active': 'IS_ACTIVE',
                'raw_data': 'RAW_DATA',
                'partition_key': 'PARTITION_KEY',
                # Platform-specific fields
                'requisition_id': 'REQUISITION_ID'
                # Note: SmartRecruiters lacks COMPENSATION, WORK_TYPE, EMPLOYMENT_STATUS
            },
            'workday': {
                # Standard fields
                'job_id': 'JOB_ID',
                'company_id': 'COMPANY_ID',
                'job_title': 'JOB_TITLE',
                'job_description': 'JOB_DESCRIPTION',
                'job_url': 'JOB_URL',
                'location': 'LOCATION',
                'date_posted': 'PUBLISHED_AT',  # Workday uses PUBLISHED_AT
                'date_retrieved': 'DATE_RETRIEVED',
                'is_active': 'IS_ACTIVE',
                'raw_data': 'RAW_DATA',
                'partition_key': 'PARTITION_KEY',
                # Platform-specific fields
                'time_type': 'TIME_TYPE',
                'employment_status': 'EMPLOYMENT_TYPE',  # Workday uses EMPLOYMENT_TYPE
                'valid_through': 'VALID_THROUGH',
                'work_type': 'WORK_TYPE',
                'compensation': 'COMPENSATION'
                # Note: Workday lacks DEPARTMENT field
            }
        }

    def map_platform_data(self, df: pd.DataFrame, platform: str) -> pd.DataFrame:
        """
        Map platform-specific data to standardized STAGE schema.

        Args:
            df: DataFrame with platform-specific schema
            platform: Platform name ('bamboohr', 'greenhouse', 'smartrecruiters', 'workday')

        Returns:
            DataFrame with standardized STAGE schema
        """
        if platform not in self.platform_mappings:
            raise ValueError(f"Unsupported platform: {platform}. Supported: {list(self.platform_mappings.keys())}")

        mapping = self.platform_mappings[platform]
        result_df = pd.DataFrame()

        # Map common fields
        for stage_field, raw_field in mapping.items():
            if raw_field in df.columns:
                result_df[stage_field] = df[raw_field]
            else:
                # Handle missing fields with null values
                result_df[stage_field] = None

        # Add platform identifier
        result_df['platform'] = platform

        # Handle platform-specific transformations
        result_df = self._apply_platform_transformations(result_df, platform, df)

        return result_df

    def _apply_platform_transformations(self, df: pd.DataFrame, platform: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply platform-specific transformations and handle special cases.

        Args:
            df: Partially mapped DataFrame
            platform: Platform name
            raw_df: Original raw DataFrame

        Returns:
            DataFrame with platform-specific transformations applied
        """
        # Standardize employment status field
        df = self._standardize_employment_status(df, platform)

        # Create platform-specific data JSON
        df = self._create_platform_specific_data(df, platform, raw_df)

        # Handle date formatting
        df = self._standardize_dates(df)

        # Add source tracking
        df['source_raw_table'] = f"RAW.{platform.upper()}_JOBS"
        df['partition_date'] = pd.to_datetime(df['date_retrieved']).dt.date

        return df

    def _standardize_employment_status(self, df: pd.DataFrame, platform: str) -> pd.DataFrame:
        """
        Standardize employment status field across platforms.

        Args:
            df: DataFrame to standardize
            platform: Platform name

        Returns:
            DataFrame with standardized employment_status field
        """
        # Create a unified employment_status field
        if platform == 'bamboohr':
            # BambooHR already has employment_status
            pass
        elif platform == 'workday':
            # Workday has employment_status mapped from EMPLOYMENT_TYPE
            pass
        elif platform in ['greenhouse', 'smartrecruiters']:
            # These platforms don't have employment status, set to null
            df['employment_status'] = None

        # Standardize values
        if 'employment_status' in df.columns:
            df['employment_status'] = df['employment_status'].apply(self._normalize_employment_status)

        return df

    def _normalize_employment_status(self, value: Optional[str]) -> Optional[str]:
        """
        Normalize employment status values to standard format.

        Args:
            value: Raw employment status value

        Returns:
            Normalized employment status value
        """
        if pd.isna(value) or value is None:
            return None

        value_lower = str(value).lower().strip()

        # Map variations to standard values
        if value_lower in ['full-time', 'fulltime', 'full_time', 'ft']:
            return 'Full-time'
        elif value_lower in ['part-time', 'parttime', 'part_time', 'pt']:
            return 'Part-time'
        elif value_lower in ['contract', 'contractor', 'temp', 'temporary']:
            return 'Contract'
        elif value_lower in ['intern', 'internship', 'co-op', 'coop']:
            return 'Internship'
        else:
            return value  # Return original if no mapping found

    def _create_platform_specific_data(self, df: pd.DataFrame, platform: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create platform_specific_data JSON column with platform-unique fields.

        Args:
            df: Mapped DataFrame
            platform: Platform name
            raw_df: Original raw DataFrame

        Returns:
            DataFrame with platform_specific_data column
        """
        platform_data_list = []

        for idx, row in raw_df.iterrows():
            platform_data = {}

            if platform == 'bamboohr':
                platform_data = {
                    'employment_status': row.get('EMPLOYMENT_STATUS'),
                    'compensation': row.get('COMPENSATION'),
                    'work_type': row.get('WORK_TYPE')
                }
            elif platform == 'greenhouse':
                platform_data = {
                    'department_id': row.get('DEPARTMENT_ID'),
                    'updated_at': str(row.get('UPDATED_AT')) if row.get('UPDATED_AT') else None,
                    'requisition_id': row.get('REQUISITION_ID'),
                    'work_type': row.get('WORK_TYPE'),
                    'compensation': row.get('COMPENSATION')
                }
            elif platform == 'smartrecruiters':
                platform_data = {
                    'requisition_id': row.get('REQUISITION_ID')
                }
            elif platform == 'workday':
                platform_data = {
                    'time_type': row.get('TIME_TYPE'),
                    'employment_type': row.get('EMPLOYMENT_TYPE'),
                    'valid_through': str(row.get('VALID_THROUGH')) if row.get('VALID_THROUGH') else None,
                    'work_type': row.get('WORK_TYPE'),
                    'compensation': row.get('COMPENSATION')
                }

            # Remove None values
            platform_data = {k: v for k, v in platform_data.items() if v is not None}

            platform_data_list.append(json.dumps(platform_data) if platform_data else None)

        df['platform_specific_data'] = platform_data_list
        return df

    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize date fields to consistent format.

        Args:
            df: DataFrame with date fields

        Returns:
            DataFrame with standardized date fields
        """
        # Ensure date_posted is in DATE format
        if 'date_posted' in df.columns:
            df['date_posted'] = pd.to_datetime(df['date_posted']).dt.date

        # Ensure date_retrieved is in TIMESTAMP format
        if 'date_retrieved' in df.columns:
            df['date_retrieved'] = pd.to_datetime(df['date_retrieved'])

        return df

    def get_unified_schema_fields(self) -> List[str]:
        """
        Get the list of unified schema fields for STAGE layer.

        Returns:
            List of field names in the unified schema
        """
        return [
            # Core identifiers
            'job_id', 'company_id', 'platform',
            # Standardized core fields
            'job_title', 'job_description', 'location', 'job_url',
            # Date fields
            'date_posted', 'date_retrieved',
            # Status and classification
            'is_active', 'employment_status', 'department',
            # Platform-specific data
            'platform_specific_data',
            # Source tracking
            'source_raw_table', 'raw_data', 'partition_key', 'partition_date'
        ]

    def validate_mapping(self, df: pd.DataFrame, platform: str) -> Dict[str, Any]:
        """
        Validate the mapping results and provide quality metrics.

        Args:
            df: Mapped DataFrame
            platform: Platform name

        Returns:
            Dictionary with validation results and metrics
        """
        total_rows = len(df)
        required_fields = ['job_id', 'company_id', 'job_title', 'job_description']

        validation_results = {
            'platform': platform,
            'total_rows': total_rows,
            'missing_data': {},
            'data_quality_score': 0.0
        }

        # Check for missing required fields
        for field in required_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                missing_pct = (missing_count / total_rows) * 100 if total_rows > 0 else 0
                validation_results['missing_data'][field] = {
                    'missing_count': missing_count,
                    'missing_percentage': round(missing_pct, 2)
                }

        # Calculate overall data quality score
        total_missing_pct = sum([v['missing_percentage'] for v in validation_results['missing_data'].values()])
        avg_missing_pct = total_missing_pct / len(required_fields) if required_fields else 0
        validation_results['data_quality_score'] = round(max(0, 100 - avg_missing_pct), 2)

        return validation_results


def map_all_platforms(platform_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Map data from all platforms to unified schema and concatenate.

    Args:
        platform_data: Dictionary mapping platform names to DataFrames

    Returns:
        Unified DataFrame with all platform data
    """
    mapper = PlatformMapper()
    unified_dataframes = []

    for platform, df in platform_data.items():
        if df is not None and not df.empty:
            mapped_df = mapper.map_platform_data(df, platform)
            unified_dataframes.append(mapped_df)

    if unified_dataframes:
        return pd.concat(unified_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()


def get_platform_field_differences() -> Dict[str, Dict[str, List[str]]]:
    """
    Get a summary of field differences across platforms.

    Returns:
        Dictionary showing which fields are present/missing in each platform
    """
    mapper = PlatformMapper()

    differences = {
        'unique_to_platform': {},
        'missing_from_platform': {},
        'common_fields': []
    }

    all_fields = set()
    platform_fields = {}

    # Collect all fields from all platforms
    for platform, mapping in mapper.platform_mappings.items():
        fields = set(mapping.keys())
        platform_fields[platform] = fields
        all_fields.update(fields)

    # Find common fields (present in all platforms)
    common_fields = all_fields.copy()
    for platform, fields in platform_fields.items():
        common_fields = common_fields.intersection(fields)

    differences['common_fields'] = list(common_fields)

    # Find unique and missing fields for each platform
    for platform, fields in platform_fields.items():
        unique_fields = fields - (all_fields - fields)
        missing_fields = all_fields - fields

        differences['unique_to_platform'][platform] = list(unique_fields - common_fields)
        differences['missing_from_platform'][platform] = list(missing_fields)

    return differences


if __name__ == "__main__":
    # Example usage and testing
    print("Platform Field Mapping Module")
    print("=" * 50)

    # Show field differences
    differences = get_platform_field_differences()
    print("\nField Differences Across Platforms:")
    print(f"Common fields: {differences['common_fields']}")

    for platform in ['bamboohr', 'greenhouse', 'smartrecruiters', 'workday']:
        unique = differences['unique_to_platform'][platform]
        missing = differences['missing_from_platform'][platform]
        print(f"\n{platform.upper()}:")
        print(f"  Unique fields: {unique}")
        print(f"  Missing fields: {missing}")

    # Test mapper initialization
    mapper = PlatformMapper()
    print(f"\nUnified schema fields: {mapper.get_unified_schema_fields()}")

    print("\nPlatform Field Mapping Module Ready!")