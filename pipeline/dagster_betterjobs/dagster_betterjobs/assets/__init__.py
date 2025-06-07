from dagster_betterjobs.assets.adhoc_company_urls import adhoc_company_urls

from dagster_betterjobs.assets.bamboohr_jobs_discovery import bamboohr_company_jobs_discovery
from dagster_betterjobs.assets.greenhouse_jobs_discovery import greenhouse_company_jobs_discovery
from dagster_betterjobs.assets.smartrecruiters_jobs_discovery import smartrecruiters_company_jobs_discovery
from dagster_betterjobs.assets.workday_jobs_discovery import workday_company_jobs_discovery
from dagster_betterjobs.assets.raw_company_profiles import raw_company_profiles
from dagster_betterjobs.assets.job_search import search_jobs

# ENHANCEMENT-001: Individual platform assets for parallel processing
from dagster_betterjobs.assets.stage_jobs_bamboohr import stage_jobs_bamboohr
from dagster_betterjobs.assets.stage_jobs_greenhouse import stage_jobs_greenhouse
from dagster_betterjobs.assets.stage_jobs_workday import stage_jobs_workday
from dagster_betterjobs.assets.stage_jobs_smartrecruiters import stage_jobs_smartrecruiters

# ENHANCEMENT-001: Lightweight combiner for cross-platform validation
from dagster_betterjobs.assets.stage_jobs_unified import stage_jobs_unified

from dagster_betterjobs.assets.snowflake_master_company_urls import snowflake_master_company_urls

__all__ = [
    "adhoc_company_urls",
    "job_search_results",
    "bamboohr_company_jobs_discovery",
    "greenhouse_company_jobs_discovery",
    "smartrecruiters_company_jobs_discovery",
    "workday_company_jobs_discovery",
    "raw_company_profiles",
    "search_jobs",
    # Individual platform assets for parallel processing
    "stage_jobs_bamboohr",
    "stage_jobs_greenhouse",
    "stage_jobs_workday",
    "stage_jobs_smartrecruiters",
    # Cross-platform combiner
    "stage_jobs_unified",
    "snowflake_master_company_urls"
]
