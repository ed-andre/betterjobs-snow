"""
Test module for platform field mapping functionality.
"""

import pytest
import pandas as pd
import json
from datetime import datetime, date
from .platform_mapping import (
    PlatformMapper,
    map_all_platforms,
    get_platform_field_differences
)


class TestPlatformMapper:
    """Test cases for the PlatformMapper class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mapper = PlatformMapper()

    def test_platform_mapper_initialization(self):
        """Test that platform mapper initializes correctly."""
        assert 'bamboohr' in self.mapper.platform_mappings
        assert 'greenhouse' in self.mapper.platform_mappings
        assert 'smartrecruiters' in self.mapper.platform_mappings
        assert 'workday' in self.mapper.platform_mappings

    def test_bamboohr_mapping(self):
        """Test BambooHR data mapping."""
        # Create test BambooHR data
        bamboohr_data = pd.DataFrame({
            'JOB_ID': ['job_001'],
            'COMPANY_ID': ['comp_001'],
            'JOB_TITLE': ['Software Engineer'],
            'JOB_DESCRIPTION': ['We are looking for a software engineer...'],
            'JOB_URL': ['https://company.bamboohr.com/jobs/job_001'],
            'LOCATION': ['San Francisco, CA'],
            'DEPARTMENT': ['Engineering'],
            'EMPLOYMENT_STATUS': ['Full-time'],
            'DATE_POSTED': [date(2024, 1, 15)],
            'DATE_RETRIEVED': [datetime(2024, 1, 16, 10, 0, 0)],
            'IS_ACTIVE': [True],
            'RAW_DATA': ['{"original": "data"}'],
            'PARTITION_KEY': ['2024-01-16'],
            'COMPENSATION': ['$100,000 - $130,000'],
            'WORK_TYPE': ['Remote']
        })

        # Map the data
        result = self.mapper.map_platform_data(bamboohr_data, 'bamboohr')

        # Verify core mappings
        assert result['job_id'].iloc[0] == 'job_001'
        assert result['company_id'].iloc[0] == 'comp_001'
        assert result['job_title'].iloc[0] == 'Software Engineer'
        assert result['platform'].iloc[0] == 'bamboohr'
        assert result['employment_status'].iloc[0] == 'Full-time'
        assert result['source_raw_table'].iloc[0] == 'RAW.BAMBOOHR_JOBS'

        # Verify platform-specific data
        platform_data = json.loads(result['platform_specific_data'].iloc[0])
        assert platform_data['employment_status'] == 'Full-time'
        assert platform_data['compensation'] == '$100,000 - $130,000'
        assert platform_data['work_type'] == 'Remote'

    def test_greenhouse_mapping(self):
        """Test Greenhouse data mapping."""
        # Create test Greenhouse data
        greenhouse_data = pd.DataFrame({
            'JOB_ID': ['gh_job_001'],
            'COMPANY_ID': ['comp_002'],
            'JOB_TITLE': ['Data Scientist'],
            'JOB_DESCRIPTION': ['We are seeking a data scientist...'],
            'JOB_URL': ['https://boards.greenhouse.io/company/jobs/gh_job_001'],
            'LOCATION': ['New York, NY'],
            'DEPARTMENT': ['Data Science'],
            'DEPARTMENT_ID': ['dept_ds_001'],
            'PUBLISHED_AT': [date(2024, 1, 10)],
            'UPDATED_AT': [datetime(2024, 1, 12, 14, 30, 0)],
            'REQUISITION_ID': ['req_001'],
            'DATE_RETRIEVED': [datetime(2024, 1, 16, 11, 0, 0)],
            'IS_ACTIVE': [True],
            'RAW_DATA': ['{"greenhouse": "data"}'],
            'PARTITION_KEY': ['2024-01-16'],
            'WORK_TYPE': ['Hybrid'],
            'COMPENSATION': ['Competitive salary']
        })

        # Map the data
        result = self.mapper.map_platform_data(greenhouse_data, 'greenhouse')

        # Verify core mappings
        assert result['job_id'].iloc[0] == 'gh_job_001'
        assert result['job_title'].iloc[0] == 'Data Scientist'
        assert result['platform'].iloc[0] == 'greenhouse'
        assert result['date_posted'].iloc[0] == date(2024, 1, 10)
        assert result['employment_status'].iloc[0] is None  # Greenhouse doesn't have this
        assert result['source_raw_table'].iloc[0] == 'RAW.GREENHOUSE_JOBS'

        # Verify platform-specific data
        platform_data = json.loads(result['platform_specific_data'].iloc[0])
        assert platform_data['department_id'] == 'dept_ds_001'
        assert platform_data['requisition_id'] == 'req_001'
        assert platform_data['work_type'] == 'Hybrid'

    def test_smartrecruiters_mapping(self):
        """Test SmartRecruiters data mapping."""
        # Create test SmartRecruiters data
        smartrecruiters_data = pd.DataFrame({
            'JOB_ID': ['sr_job_001'],
            'COMPANY_ID': ['comp_003'],
            'JOB_TITLE': ['Product Manager'],
            'JOB_DESCRIPTION': ['Looking for a product manager...'],
            'JOB_URL': ['https://jobs.smartrecruiters.com/company/sr_job_001'],
            'LOCATION': ['Austin, TX'],
            'DEPARTMENT': ['Product'],
            'PUBLISHED_AT': [date(2024, 1, 8)],
            'REQUISITION_ID': ['sr_req_001'],
            'DATE_RETRIEVED': [datetime(2024, 1, 16, 12, 0, 0)],
            'IS_ACTIVE': [True],
            'RAW_DATA': ['{"smartrecruiters": "data"}'],
            'PARTITION_KEY': ['2024-01-16']
        })

        # Map the data
        result = self.mapper.map_platform_data(smartrecruiters_data, 'smartrecruiters')

        # Verify core mappings
        assert result['job_id'].iloc[0] == 'sr_job_001'
        assert result['job_title'].iloc[0] == 'Product Manager'
        assert result['platform'].iloc[0] == 'smartrecruiters'
        assert result['date_posted'].iloc[0] == date(2024, 1, 8)
        assert result['employment_status'].iloc[0] is None  # SmartRecruiters doesn't have this
        assert result['source_raw_table'].iloc[0] == 'RAW.SMARTRECRUITERS_JOBS'

        # Verify platform-specific data
        platform_data = json.loads(result['platform_specific_data'].iloc[0])
        assert platform_data['requisition_id'] == 'sr_req_001'
        # SmartRecruiters has minimal platform-specific data

    def test_workday_mapping(self):
        """Test Workday data mapping."""
        # Create test Workday data
        workday_data = pd.DataFrame({
            'JOB_ID': ['wd_job_001'],
            'COMPANY_ID': ['comp_004'],
            'JOB_TITLE': ['Marketing Manager'],
            'JOB_DESCRIPTION': ['We need a marketing manager...'],
            'JOB_URL': ['https://company.wd1.myworkdayjobs.com/careers/job/wd_job_001'],
            'LOCATION': ['Seattle, WA'],
            'TIME_TYPE': ['Full_Time'],
            'EMPLOYMENT_TYPE': ['Regular'],
            'PUBLISHED_AT': [date(2024, 1, 5)],
            'VALID_THROUGH': [date(2024, 3, 5)],
            'DATE_RETRIEVED': [datetime(2024, 1, 16, 13, 0, 0)],
            'IS_ACTIVE': [True],
            'RAW_DATA': ['{"workday": "data"}'],
            'PARTITION_KEY': ['2024-01-16'],
            'WORK_TYPE': ['On-site'],
            'COMPENSATION': ['$80,000 - $95,000']
        })

        # Map the data
        result = self.mapper.map_platform_data(workday_data, 'workday')

        # Verify core mappings
        assert result['job_id'].iloc[0] == 'wd_job_001'
        assert result['job_title'].iloc[0] == 'Marketing Manager'
        assert result['platform'].iloc[0] == 'workday'
        assert result['date_posted'].iloc[0] == date(2024, 1, 5)
        assert result['employment_status'].iloc[0] == 'Regular'
        assert result['department'].iloc[0] is None  # Workday doesn't have department
        assert result['source_raw_table'].iloc[0] == 'RAW.WORKDAY_JOBS'

        # Verify platform-specific data
        platform_data = json.loads(result['platform_specific_data'].iloc[0])
        assert platform_data['time_type'] == 'Full_Time'
        assert platform_data['employment_type'] == 'Regular'
        assert platform_data['valid_through'] == '2024-03-05'
        assert platform_data['work_type'] == 'On-site'
        assert platform_data['compensation'] == '$80,000 - $95,000'

    def test_employment_status_normalization(self):
        """Test employment status normalization."""
        test_cases = [
            ('Full-time', 'Full-time'),
            ('full-time', 'Full-time'),
            ('FULL-TIME', 'Full-time'),
            ('fulltime', 'Full-time'),
            ('FT', 'Full-time'),
            ('Part-time', 'Part-time'),
            ('part-time', 'Part-time'),
            ('PT', 'Part-time'),
            ('Contract', 'Contract'),
            ('contractor', 'Contract'),
            ('temp', 'Contract'),
            ('Intern', 'Internship'),
            ('internship', 'Internship'),
            ('co-op', 'Internship'),
            ('Unknown', 'Unknown'),  # No mapping, return original
            (None, None),
            ('', None)
        ]

        for input_val, expected in test_cases:
            result = self.mapper._normalize_employment_status(input_val)
            assert result == expected, f"Failed for input: {input_val}"

    def test_missing_platform_error(self):
        """Test error handling for unsupported platform."""
        test_data = pd.DataFrame({'JOB_ID': ['test']})

        with pytest.raises(ValueError, match="Unsupported platform: invalid_platform"):
            self.mapper.map_platform_data(test_data, 'invalid_platform')

    def test_missing_fields_handling(self):
        """Test handling of missing fields in source data."""
        # Create minimal data (missing optional fields)
        minimal_data = pd.DataFrame({
            'JOB_ID': ['job_001'],
            'COMPANY_ID': ['comp_001'],
            'JOB_TITLE': ['Test Job'],
            'DATE_RETRIEVED': [datetime.now()]
        })

        result = self.mapper.map_platform_data(minimal_data, 'bamboohr')

        # Should handle missing fields gracefully
        assert result['job_id'].iloc[0] == 'job_001'
        assert pd.isna(result['job_description'].iloc[0]) or result['job_description'].iloc[0] is None
        assert pd.isna(result['compensation'].iloc[0]) or result['compensation'].iloc[0] is None

    def test_validation_functionality(self):
        """Test data validation functionality."""
        test_data = pd.DataFrame({
            'JOB_ID': ['job_001', 'job_002', None],  # One missing job_id
            'COMPANY_ID': ['comp_001', 'comp_002', 'comp_003'],
            'JOB_TITLE': ['Job 1', None, 'Job 3'],  # One missing title
            'JOB_DESCRIPTION': ['Desc 1', 'Desc 2', 'Desc 3'],
            'DATE_RETRIEVED': [datetime.now()] * 3
        })

        mapped_data = self.mapper.map_platform_data(test_data, 'bamboohr')
        validation_results = self.mapper.validate_mapping(mapped_data, 'bamboohr')

        assert validation_results['platform'] == 'bamboohr'
        assert validation_results['total_rows'] == 3
        assert validation_results['missing_data']['job_id']['missing_count'] == 1
        assert validation_results['missing_data']['job_title']['missing_count'] == 1
        assert validation_results['data_quality_score'] < 100  # Should be penalized for missing data

    def test_get_unified_schema_fields(self):
        """Test getting unified schema fields."""
        fields = self.mapper.get_unified_schema_fields()

        expected_fields = [
            'job_id', 'company_id', 'platform',
            'job_title', 'job_description', 'location', 'job_url',
            'date_posted', 'date_retrieved',
            'is_active', 'employment_status', 'department',
            'platform_specific_data',
            'source_raw_table', 'raw_data', 'partition_key', 'partition_date'
        ]

        for field in expected_fields:
            assert field in fields


class TestUtilityFunctions:
    """Test cases for utility functions."""

    def test_map_all_platforms(self):
        """Test mapping data from all platforms."""
        # Create test data for multiple platforms
        platform_data = {
            'bamboohr': pd.DataFrame({
                'JOB_ID': ['bhr_001'],
                'COMPANY_ID': ['comp_001'],
                'JOB_TITLE': ['BambooHR Job'],
                'JOB_DESCRIPTION': ['BambooHR job description'],
                'DATE_POSTED': [date(2024, 1, 1)],
                'DATE_RETRIEVED': [datetime.now()],
                'IS_ACTIVE': [True],
                'EMPLOYMENT_STATUS': ['Full-time'],
                'RAW_DATA': ['{}'],
                'PARTITION_KEY': ['2024-01-01']
            }),
            'greenhouse': pd.DataFrame({
                'JOB_ID': ['gh_001'],
                'COMPANY_ID': ['comp_002'],
                'JOB_TITLE': ['Greenhouse Job'],
                'JOB_DESCRIPTION': ['Greenhouse job description'],
                'PUBLISHED_AT': [date(2024, 1, 2)],
                'DATE_RETRIEVED': [datetime.now()],
                'IS_ACTIVE': [True],
                'RAW_DATA': ['{}'],
                'PARTITION_KEY': ['2024-01-02']
            })
        }

        result = map_all_platforms(platform_data)

        # Should have 2 rows (one from each platform)
        assert len(result) == 2
        assert 'bamboohr' in result['platform'].values
        assert 'greenhouse' in result['platform'].values

    def test_get_platform_field_differences(self):
        """Test getting platform field differences."""
        differences = get_platform_field_differences()

        assert 'common_fields' in differences
        assert 'unique_to_platform' in differences
        assert 'missing_from_platform' in differences

        # Check that we have data for all platforms
        platforms = ['bamboohr', 'greenhouse', 'smartrecruiters', 'workday']
        for platform in platforms:
            assert platform in differences['unique_to_platform']
            assert platform in differences['missing_from_platform']

        # Common fields should include basic job fields
        common_fields = differences['common_fields']
        assert 'job_id' in common_fields
        assert 'company_id' in common_fields
        assert 'job_title' in common_fields

    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames."""
        empty_data = {'bamboohr': pd.DataFrame()}
        result = map_all_platforms(empty_data)

        assert result.empty

    def test_none_dataframe_handling(self):
        """Test handling of None DataFrames."""
        none_data = {'bamboohr': None}
        result = map_all_platforms(none_data)

        assert result.empty


if __name__ == "__main__":
    # Run some basic tests
    print("Running basic platform mapping tests...")

    # Test initialization
    mapper = PlatformMapper()
    print("✓ Platform mapper initialized successfully")

    # Test field differences
    differences = get_platform_field_differences()
    print("✓ Platform field differences retrieved")
    print(f"  Common fields: {len(differences['common_fields'])}")

    # Test basic mapping for each platform
    platforms = ['bamboohr', 'greenhouse', 'smartrecruiters', 'workday']
    for platform in platforms:
        test_data = pd.DataFrame({
            'JOB_ID': ['test_001'],
            'COMPANY_ID': ['comp_001'],
            'JOB_TITLE': [f'{platform.title()} Test Job'],
            'JOB_DESCRIPTION': ['Test description'],
            'DATE_RETRIEVED': [datetime.now()]
        })

        # Add platform-specific date field
        if platform == 'bamboohr':
            test_data['DATE_POSTED'] = [date.today()]
        else:
            test_data['PUBLISHED_AT'] = [date.today()]

        try:
            result = mapper.map_platform_data(test_data, platform)
            print(f"✓ {platform.title()} mapping successful")
        except Exception as e:
            print(f"✗ {platform.title()} mapping failed: {e}")

    print("\nBasic platform mapping tests completed!")