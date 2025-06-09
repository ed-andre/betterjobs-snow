"""
Job Definitions for BetterJobs Pipeline

ENHANCEMENT-005 COMPATIBILITY NOTES:
The search_jobs asset has been migrated from RAW to STAGE data dependency.
- Enhanced search capabilities with quality scoring and language detection
- Better performance using single unified table
- Cross-platform deduplication built-in
- Cleaned and standardized data for improved search accuracy

Job Updates:
- data_engineering_job: Updated with enhanced STAGE parameters
- enhanced_data_engineering_job: New job showcasing full STAGE capabilities
- full_jobs_discovery_and_search_job: Updated to include full pipeline from RAW to STAGE to SEARCH
"""

from dagster import (
    AssetSelection,
    define_asset_job,
    OpExecutionContext,
    op,
    job,
    Config,
    In,
    Out,
    RunConfig,
    Definitions,
    static_partitioned_config
)
from dagster_betterjobs.assets.bamboohr_jobs_discovery import alpha_partitions as bamboo_partitions_def
from dagster_betterjobs.assets.greenhouse_jobs_discovery import alpha_partitions as greenhouse_partitions_def
from dagster_betterjobs.assets.workday_jobs_discovery import alpha_partitions as workday_partitions_def
from dagster_betterjobs.assets.smartrecruiters_jobs_discovery import alpha_partitions as smartrecruiters_partitions_def
from dagster_betterjobs.assets.icims_jobs_discovery import alpha_partitions as icims_partitions_def
import os

# Use bamboo_partitions_def for alpha_partitions
alpha_partitions = bamboo_partitions_def

# Create partitioned configs for each platform
@static_partitioned_config(partition_keys=bamboo_partitions_def.get_partition_keys())
def bamboohr_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=greenhouse_partitions_def.get_partition_keys())
def greenhouse_partitioned_config(partition_key: str):
    return {
        "ops": {
            "greenhouse_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=workday_partitions_def.get_partition_keys())
def workday_partitioned_config(partition_key: str):
    return {
        "ops": {
            "workday_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=smartrecruiters_partitions_def.get_partition_keys())
def smartrecruiters_partitioned_config(partition_key: str):
    return {
        "ops": {
            "smartrecruiters_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=icims_partitions_def.get_partition_keys())
def icims_partitioned_config(partition_key: str):
    return {
        "ops": {
            "icims_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

# Define jobs for job discovery by platform
bamboohr_jobs_discovery_job = define_asset_job(
    name="bamboohr_jobs_discovery_job",
    selection=AssetSelection.assets("bamboohr_company_jobs_discovery"),
    description="Job that discovers and collects job listings from BambooHR career sites",
    partitions_def=bamboo_partitions_def,
    config=bamboohr_partitioned_config
)

greenhouse_jobs_discovery_job = define_asset_job(
    name="greenhouse_jobs_discovery_job",
    selection=AssetSelection.assets("greenhouse_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Greenhouse career sites",
    partitions_def=greenhouse_partitions_def,
    config=greenhouse_partitioned_config
)

smartrecruiters_jobs_discovery_job = define_asset_job(
    name="smartrecruiters_jobs_discovery_job",
    selection=AssetSelection.assets("smartrecruiters_company_jobs_discovery"),
    description="Job that discovers and collects job listings from SmartRecruiters career sites",
    partitions_def=smartrecruiters_partitions_def,
    config=smartrecruiters_partitioned_config
)

workday_jobs_discovery_job = define_asset_job(
    name="workday_jobs_discovery_job",
    selection=AssetSelection.assets("workday_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Workday career sites",
    partitions_def=workday_partitions_def,
    config=workday_partitioned_config
)

# Define a job for all job discovery across platforms
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}},
        }
    }

full_jobs_discovery_job = define_asset_job(
    name="full_jobs_discovery_job",
    selection=AssetSelection.groups("job_discovery"),
    description="Job that discovers and collects job listings from all supported platforms",
    partitions_def=alpha_partitions,
    config=full_jobs_partitioned_config
)

# For all job discovery except ICIMS
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_except_icims_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}}
        }
    }

full_jobs_discovery_except_icims_job = define_asset_job(
    name="full_jobs_discovery_except_icims_job",
    selection=AssetSelection.assets(
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
    ),
    partitions_def=alpha_partitions,
    config=full_jobs_except_icims_partitioned_config
)

# Define a job for data/database engineering position search
data_engineering_job = define_asset_job(
    name="data_engineering_job",
    selection=AssetSelection.assets("search_jobs"),
    description="Job that searches for SQL Developer, Database Developer, and Data Engineer positions in NY, NJ, or remote using enhanced STAGE data",
    config=RunConfig(
        ops={
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "Data", "ETL", "pipeline", "data engineer", "snowflake", "dbt", "airflow", "dagster", "BigQuery", "SQL Server", "SSIS"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore", "India"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 10,
                    "max_results": 500,
                    "min_match_score": 0.1,
                    "platforms": ["greenhouse", "bamboohr", "smartrecruiters", "workday"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "data_engineering_jobs_{date}.html"),
                    "include_descriptions": True,
                    "job_name": "Data Engineering Job",
                    # Enhanced STAGE data parameters for better results
                    "min_quality_score": 0.3,  # Lower than default to avoid filtering too aggressively
                    "language_filter": "english",  # Focus on English jobs for US market
                    "language_confidence_min": 0.7,  # Slightly lower confidence threshold
                    "rank_by_quality": True,  # Prioritize high-quality job postings
                    "rank_by_recency": True  # Also prioritize recent postings
                }
            }
        }
    )
)

# Define a job for enhanced data engineering position search using existing STAGE data
enhanced_data_engineering_job = define_asset_job(
    name="enhanced_data_engineering_job",
    selection=AssetSelection.assets("search_jobs"),
    description="Enhanced job search using cleaned STAGE data for SQL Developer, Database Developer, and Data Engineer positions - no discovery needed",
    config=RunConfig(
        ops={
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "Data", "ETL", "pipeline", "data engineer", "snowflake", "dbt", "airflow", "dagster", "BigQuery", "SQL Server", "SSIS"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst", "Scientist"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore", "intern", "unpaid", "India"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 14,
                    "max_results": 1000,
                    "min_match_score": 0.2,
                    "platforms": ["all"],  # Search all available platforms
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "enhanced_data_engineering_jobs_{date}.html"),
                    "include_descriptions": True,
                    "job_name": "Enhanced Data Engineering Job",
                    # Take full advantage of STAGE data enhancements
                    "min_quality_score": 0.4,  # Higher quality threshold for better results
                    "language_filter": "english",  # English jobs for US market
                    "language_confidence_min": 0.8,  # High confidence in language detection
                    "rank_by_quality": True,  # Prioritize high-quality job postings
                    "rank_by_recency": True  # Prioritize recent postings
                }
            }
        }
    )
)

# Define a job for Snowflake master company URLs processing
snowflake_master_company_urls_job = define_asset_job(
    name="snowflake_master_company_urls_job",
    selection=AssetSelection.assets("snowflake_master_company_urls"),
    description="Job that processes and maintains master company URLs in Snowflake from S3 and local CSV sources",
    config=RunConfig(
        ops={
            "snowflake_master_company_urls": {
                "config": {
                    "batch_size": 1000,
                    "enable_s3_processing": True,
                    "enable_local_processing": True,
                    "deduplicate_on_load": True
                }
            }
        }
    )
)

# Define a job for all job discovery across platforms plus job search
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_discovery_and_search_partitioned_config(partition_key: str):
    return {
        "ops": {
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}},
            "bamboohr_company_jobs_discovery": {"config": {}},
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "ETL", "pipeline", "data engineer"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 5,
                    "max_results": 500,
                    "min_match_score": 0.1,
                    "platforms": ["greenhouse", "workday", "bamboohr", "smartrecruiters"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "data_engineering_jobs_{date}.html"),
                    "include_descriptions": True,
                    "job_name": "Full Jobs Discovery and Search",
                    # Enhanced STAGE data parameters for better results
                    "min_quality_score": 0.3,  # Lower than default to avoid filtering too aggressively
                    "language_filter": "english",  # Focus on English jobs for US market
                    "language_confidence_min": 0.7,  # Slightly lower confidence threshold
                    "rank_by_quality": True,  # Prioritize high-quality job postings
                    "rank_by_recency": True  # Also prioritize recent postings
                }
            }
        }
    }

full_jobs_discovery_and_search_job = define_asset_job(
    name="full_jobs_discovery_and_search_job",
    selection=[
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
        "stage_jobs_bamboohr",
        "stage_jobs_greenhouse",
        "stage_jobs_workday",
        "stage_jobs_smartrecruiters",
        "stage_jobs_unified",
        "search_jobs"
    ],
    description="Job that discovers and collects job listings from all supported platforms, processes them through STAGE layer, and performs enhanced job search",
    partitions_def=alpha_partitions,
    config=full_jobs_discovery_and_search_partitioned_config
)



