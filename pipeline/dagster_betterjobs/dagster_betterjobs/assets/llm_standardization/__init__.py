"""
LLM Data Standardization Assets

This package contains assets for normalizing and standardizing LLM-extracted VARIANT data
from the STAGE layer into proper relational structures for analytics.

Phase 1: Skills Normalization Assets
Phase 2: Keywords Normalization Assets
Phase 3: Location Standardization Assets
Phase 4: Experience Normalization Assets (BUG-015)
Phase 5: Salary Normalization Assets (ENHANCEMENT-022)
Phase 6: Data Quality and Validation Assets
"""

from .skills_normalization import (
    stage_llm_skills_raw_extraction,
    stage_skills_normalized,
    stage_manual_skill_taxonomy,
    stage_job_skills_bridge
)



from .keywords_normalization import (
    stage_llm_keywords_raw_extraction,
    stage_keywords_standardization_rules,
    stage_keyword_type_mapping,
    stage_keywords_normalized,
    stage_job_keywords_bridge
)

from .locations_normalization import (
    stage_llm_locations_raw_extraction,
    stage_location_standardization_rules,
    stage_countries_mapping,
    stage_us_states_mapping,
    stage_locations_normalized,
    stage_job_locations_bridge
)

from .experience_normalization import (
    stage_llm_experience_raw_extraction,
    stage_experience_standardization_rules,
    stage_experience_normalized,
    stage_job_experience_bridge
)

from .salary_normalization import (
    stage_salary_raw_extraction,
    stage_salary_normalized,
    stage_job_salary_bridge
)

from .data_quality import (
    stage_llm_data_quality_validation,
    stage_llm_quality_metrics,
    stage_llm_coverage_analysis,
    stage_llm_confidence_monitoring,
    stage_llm_manual_review_queue
)

__all__ = [
    # Phase 1: Skills Normalization Assets
    "stage_llm_skills_raw_extraction",
    "stage_skills_normalized",
    "stage_manual_skill_taxonomy",
    "stage_job_skills_bridge",

    # Phase 2: Keywords Normalization Assets
    "stage_llm_keywords_raw_extraction",
    "stage_keywords_standardization_rules",
    "stage_keyword_type_mapping",
    "stage_keywords_normalized",
    "stage_job_keywords_bridge",

    # Phase 3: Location Standardization Assets
    "stage_llm_locations_raw_extraction",
    "stage_location_standardization_rules",
    "stage_countries_mapping",
    "stage_us_states_mapping",
    "stage_locations_normalized",
    "stage_job_locations_bridge",

    # Phase 4: Experience Normalization Assets (BUG-015)
    "stage_llm_experience_raw_extraction",
    "stage_experience_standardization_rules",
    "stage_experience_normalized",
    "stage_job_experience_bridge",

    # Phase 5: Salary Normalization Assets (ENHANCEMENT-022)
    "stage_salary_raw_extraction",
    "stage_salary_normalized",
    "stage_job_salary_bridge",

    # Phase 6: Data Quality and Validation Assets
    "stage_llm_data_quality_validation",
    "stage_llm_quality_metrics",
    "stage_llm_coverage_analysis",
    "stage_llm_confidence_monitoring",
    "stage_llm_manual_review_queue"
]