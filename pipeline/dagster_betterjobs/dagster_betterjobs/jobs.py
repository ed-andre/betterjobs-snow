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
- full_jobs_discovery_enrichment_search_job: Updated to  STAGE to SEARCH
"""

from dagster import (
    AssetSelection,
    define_asset_job,
    RunConfig,
    static_partitioned_config
)
from dagster_betterjobs.partitions import company_alpha_partitions
import os

# All discovery assets now use the same universal partitions
alpha_partitions = company_alpha_partitions

# job for infrastructure_setup group
infrastructure_setup_job = define_asset_job(
    name="infrastructure_setup_job",
    selection=AssetSelection.groups("infrastructure_setup"),
    description="Job that sets up the infrastructure for the pipeline",
)


# JOB DISCOVERY - BAMBOOHR
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
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
## JOB
bamboohr_jobs_discovery_job = define_asset_job(
    name="bamboohr_jobs_discovery_job",
    selection=AssetSelection.assets("bamboohr_company_jobs_discovery"),
    description="Job that discovers and collects job listings from BambooHR career sites",
    partitions_def=alpha_partitions,
    config=bamboohr_partitioned_config
)

# JOB DISCOVERY - GREENHOUSE
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
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
## JOB
greenhouse_jobs_discovery_job = define_asset_job(
    name="greenhouse_jobs_discovery_job",
    selection=AssetSelection.assets("greenhouse_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Greenhouse career sites",
    partitions_def=alpha_partitions,
    config=greenhouse_partitioned_config
)

# JOB DISCOVERY - WORKDAY
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
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
## JOB
workday_jobs_discovery_job = define_asset_job(
    name="workday_jobs_discovery_job",
    selection=AssetSelection.assets("workday_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Workday career sites",
    partitions_def=alpha_partitions,
    config=workday_partitioned_config
)

# JOB DISCOVERY - SMARTRECRUITERS
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
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
## JOB
smartrecruiters_jobs_discovery_job = define_asset_job(
    name="smartrecruiters_jobs_discovery_job",
    selection=AssetSelection.assets("smartrecruiters_company_jobs_discovery"),
    description="Job that discovers and collects job listings from SmartRecruiters career sites",
    partitions_def=alpha_partitions,
    config=smartrecruiters_partitioned_config
)


# JOB DISCOVERY - ALL PLATFORMS
## CONFIG
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
## JOB
full_jobs_discovery_job = define_asset_job(
    name="full_jobs_discovery_job",
    selection=AssetSelection.assets(
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
    ),
    description="Job that discovers and collects job listings from all supported platforms",
    partitions_def=alpha_partitions,
    config=full_jobs_partitioned_config
)

# JOB ENRICHMENT - ALL PLATFORM + UNIFIED
## JOB
stage_jobs_unified_job = define_asset_job(
    name="stage_jobs_unified_job",
    selection=AssetSelection.assets("stage_jobs_bamboohr", "stage_jobs_greenhouse", "stage_jobs_workday", "stage_jobs_smartrecruiters", "stage_jobs_unified"),
    description="Job that materialize the assets that monitor and validate completion of all platform job discovery assets",
)

# STAGE LLM STANDARDIZATION VALIDATION GROUP
## JOB
stage_llm_standardization_validation_job = define_asset_job(
    name="stage_llm_standardization_validation_job",
    selection=AssetSelection.groups("2b_stage_llm_standardization_validation"),
    description="Job that validates the standardization of job listings from all platforms",
)

# ANALYTICS DIMENSIONS GROUP
## JOB
analytics_dimensions_job = define_asset_job(
    name="analytics_dimensions_job",
    selection=AssetSelection.groups("3a_analytics_dimensions"),
    description="Job that materializes the analytics dimensions",
)

# ANALYTICS FACTS AGGREGATES ANALYSIS GROUP
## JOB
analytics_facts_aggregates_analysis_job = define_asset_job(
    name="analytics_facts_aggregates_analysis_job",
    selection=AssetSelection.groups("3b_analytics_facts_aggregates_analysis"),
    description="Job that materializes the analytics facts aggregates and analysis",
)

# DATA QUALITY GOVERNANCE
## JOB
stage_data_quality_governance_job = define_asset_job(
    name="stage_data_quality_governance_job",
    selection=AssetSelection.groups("2c_stage_data_quality_governance"),
    description="Job that materializes the stage data quality governance",
)

# JOB ENRICHMENT - ALL PLATFORMS
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def llm_enrichment_partitioned_config(partition_key: str):
    return {
        "ops": {
            "stage_jobs_llm_enriched_bamboohr": {"config": {}},
            "stage_jobs_llm_enriched_greenhouse": {"config": {}},
            "stage_jobs_llm_enriched_workday": {"config": {}},
            "stage_jobs_llm_enriched_smartrecruiters": {"config": {}},
            "stage_jobs_llm_enriched_unified": {"config": {}},
        }
    }
## JOB
stage_jobs_llm_enriched_job = define_asset_job(
    name="stage_jobs_llm_enriched_job",
    selection=AssetSelection.assets("stage_jobs_llm_enriched_bamboohr", "stage_jobs_llm_enriched_greenhouse", "stage_jobs_llm_enriched_workday", "stage_jobs_llm_enriched_smartrecruiters", "stage_jobs_llm_enriched_unified"),
    description="Job that unifies all job listings from all platforms into a single table",
    partitions_def=alpha_partitions,
    config=llm_enrichment_partitioned_config
)

# JOB ENRICHMENT - UNIFIED
## JOB
stage_jobs_llm_enriched_unified_job = define_asset_job(
    name="stage_jobs_llm_enriched_unified_job",
    selection=AssetSelection.assets("stage_jobs_llm_enriched_unified"),
    description="Job for asset that monitor and validate completion of all platform LLM enrichment assets",
)

# JOB SEARCH: DATA ENGINEERING JOBS
## JOB
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
                    "days_back": 14,
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

# JOB SEARCH: ENHANCED DATA ENGINEERING JOBS
## JOB
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

# JOB SEARCH: LEGAL POSITIONS
## JOB
legal_positions_job = define_asset_job(
    name="legal_positions_job",
    selection=AssetSelection.assets("search_jobs"),
    description="Job that searches for Legal Counsel, Human Rights, International Law, and Legal Affairs positions",
    config=RunConfig(
        ops={
            "search_jobs": {
                "config": {
                    "keywords": ["legal counsel", "legal advisor", "international law", "human rights", "humanitarian law", "legal affairs", "rule of law", "international arbitration", "ESG legal", "compliance", "ethics", "corporate social responsibility", "business and human rights", "transnational justice", "public policy law", "access to justice", "peacebuilding", "gender justice", "UN legal", "NGO legal", "Africa legal", "Geneva legal", "Brussels legal", "remote legal", "sustainability", "sustainability law", "corporate social responsibility", "Public International Law"],
                    "job_titles": ["Legal", "Counsel", "Advisor", "Officer", "Consultant", "Lawyer", "Specialist", "Human Rights", "International", "Rule of Law", "Legal Affairs", "Compliance", "Ethics", "ESG", "Corporate Social Responsibility", "Justice", "Arbitration", "Public Policy", "Humanitarian", "Peacebuilding", "Gender", "UN", "NGO", ""],
                    "excluded_keywords": [],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Washington DC", "Geneva", "Brussels", "Paris", "Madrid", "Europe", "Africa", "Caribbean", "Latin America", "Location", ""],
                    "remote": True,
                    "days_back": 28,
                    "max_results": 500,
                    "min_match_score": 0.3,
                    "platforms": ["greenhouse", "bamboohr", "smartrecruiters", "workday"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "legal_positions_jobs_{date}.html"),
                    "include_descriptions": True
                }
            }
        }
    )
)

# MASTER COMPANY URLS
## JOB
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

# JOB DISCOVERY + JOB ENRICHMENT + JOB SEARCH: DATA ENGINEERING
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_discovery_enrichment_search_partitioned_config(partition_key: str):
    return {
        "ops": {
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}},
            "bamboohr_company_jobs_discovery": {"config": {}},
            "stage_jobs_llm_enriched_bamboohr": {"config": {}},
            "stage_jobs_llm_enriched_greenhouse": {"config": {}},
            "stage_jobs_llm_enriched_workday": {"config": {}},
            "stage_jobs_llm_enriched_smartrecruiters": {"config": {}},
            "stage_jobs_llm_enriched_unified": {"config": {}},
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "ETL", "pipeline", "data engineer"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 14,
                    "max_results": 500,
                    "min_match_score": 0.1,
                    "platforms": ["greenhouse", "workday", "bamboohr", "smartrecruiters"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "data_engineering_jobs_enriched_{date}.html"),
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

## JOB
full_jobs_discovery_enrichment_search_job = define_asset_job(
    name="full_jobs_discovery_enrichment_search_job",
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
        "stage_jobs_llm_enriched_bamboohr",
        "stage_jobs_llm_enriched_greenhouse",
        "stage_jobs_llm_enriched_workday",
        "stage_jobs_llm_enriched_smartrecruiters",
        "stage_jobs_llm_enriched_unified",
        "search_jobs"
    ],
    description="Job that discovers and collects job listings from all supported platforms, processes them through STAGE layer, and performs enhanced job search",
    # partitions_def=alpha_partitions,
    config=full_jobs_discovery_enrichment_search_partitioned_config
)

# JOB DISCOVERY + JOB SEARCH: DATA ENGINEERING
## CONFIG
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_discovery_search_partitioned_config(partition_key: str):
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
                    "days_back": 14,
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
## JOB
full_jobs_discovery_search_job = define_asset_job(
    name="full_jobs_discovery_search_job",
    selection=[
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
        "search_jobs"
    ],
    description="Job that discovers and collects job listings from all supported platforms, and performs enhanced job search",
    partitions_def=alpha_partitions,
    config=full_jobs_discovery_search_partitioned_config
)

