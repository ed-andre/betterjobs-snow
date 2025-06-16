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

# PHASE-1.5: Company profiles transformation
from dagster_betterjobs.assets.stage_company_profiles import stage_company_profiles

# PHASE-2.3: LLM enrichment for structured information extraction
# from dagster_betterjobs.assets.stage_jobs_llm_enriched import stage_jobs_llm_enriched
# Removed as part of ENHANCEMENT-010

# ENHANCEMENT-010: Platform-specific LLM enrichment assets for parallel processing
from dagster_betterjobs.assets.stage_jobs_llm_enriched_bamboohr import stage_jobs_llm_enriched_bamboohr
from dagster_betterjobs.assets.stage_jobs_llm_enriched_greenhouse import stage_jobs_llm_enriched_greenhouse
from dagster_betterjobs.assets.stage_jobs_llm_enriched_workday import stage_jobs_llm_enriched_workday
from dagster_betterjobs.assets.stage_jobs_llm_enriched_smartrecruiters import stage_jobs_llm_enriched_smartrecruiters

# ENHANCEMENT-010: LLM enrichment coordinator asset
from dagster_betterjobs.assets.stage_jobs_llm_enriched_unified import stage_jobs_llm_enriched_unified

# PHASE-3: LLM Data Standardization Assets
from dagster_betterjobs.assets.llm_standardization.skills_normalization import (
    stage_llm_skills_raw_extraction,
    stage_skills_standardization_rules,
    stage_skills_normalized,
    stage_job_skills_bridge
)

from dagster_betterjobs.assets.llm_standardization.keywords_normalization import (
    stage_llm_keywords_raw_extraction,
    stage_keywords_standardization_rules,
    stage_keyword_type_mapping,
    stage_keywords_normalized,
    stage_job_keywords_bridge
)

from dagster_betterjobs.assets.llm_standardization.locations_normalization import (
    stage_llm_locations_raw_extraction,
    stage_location_standardization_rules,
    stage_countries_mapping,
    stage_us_states_mapping,
    stage_locations_normalized,
    stage_job_locations_bridge
)

# PHASE-4: Data Quality and Validation Assets
from dagster_betterjobs.assets.llm_standardization.data_quality import (
    stage_llm_data_quality_validation,
    stage_llm_quality_metrics,
    stage_llm_coverage_analysis,
    stage_llm_confidence_monitoring,
    stage_llm_manual_review_queue
)

from dagster_betterjobs.assets.snowflake_master_company_urls import snowflake_master_company_urls

# Infrastructure Setup Assets
from dagster_betterjobs.assets.snowflake_setup import (
    database_schema_setup,
    raw_schema_setup,
    stage_schema_setup,
    analytics_schema_setup,
    static_data_population,
    setup_validation
)

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
    # Company profiles transformation
    "stage_company_profiles",
    # LLM enrichment asset
    "stage_jobs_llm_enriched",
    # Platform-specific LLM enrichment assets for parallel processing
    "stage_jobs_llm_enriched_bamboohr",
    "stage_jobs_llm_enriched_greenhouse",
    "stage_jobs_llm_enriched_workday",
    "stage_jobs_llm_enriched_smartrecruiters",
    # LLM enrichment coordinator
    "stage_jobs_llm_enriched_unified",
    # LLM data standardization assets - Phase 1: Skills
    "stage_llm_skills_raw_extraction",
    "stage_skills_standardization_rules",
    "stage_skills_normalized",
    "stage_job_skills_bridge",
    # LLM data standardization assets - Phase 2: Keywords
    "stage_llm_keywords_raw_extraction",
    "stage_keywords_standardization_rules",
    "stage_keyword_type_mapping",
    "stage_keywords_normalized",
    "stage_job_keywords_bridge",
    # LLM data standardization assets - Phase 3: Locations
    "stage_llm_locations_raw_extraction",
    "stage_location_standardization_rules",
    "stage_countries_mapping",
    "stage_us_states_mapping",
    "stage_locations_normalized",
    "stage_job_locations_bridge",
    # LLM data standardization assets - Phase 4: Data Quality and Validation
    "stage_llm_data_quality_validation",
    "stage_llm_quality_metrics",
    "stage_llm_coverage_analysis",
    "stage_llm_confidence_monitoring",
    "stage_llm_manual_review_queue",
    "snowflake_master_company_urls",
    # Infrastructure setup assets
    "database_schema_setup",
    "raw_schema_setup",
    "stage_schema_setup",
    "analytics_schema_setup",
    "static_data_population",
    "setup_validation"
]
