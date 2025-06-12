"""
LLM Data Standardization Assets

This package contains assets for normalizing and standardizing LLM-extracted VARIANT data
from the STAGE layer into proper relational structures for analytics.

Phase 1: Skills Normalization Assets
Phase 2: Keywords Normalization Assets
"""

from .skills_normalization import (
    stage_llm_skills_raw_extraction,
    stage_skills_standardization_rules,
    stage_skills_normalized,
    stage_job_skills_bridge
)

from .keywords_normalization import (
    stage_llm_keywords_raw_extraction,
    stage_keywords_standardization_rules,
    stage_keyword_type_mapping,
    stage_keywords_normalized,
    stage_job_keywords_bridge
)

__all__ = [
    # Phase 1: Skills Normalization Assets
    "stage_llm_skills_raw_extraction",
    "stage_skills_standardization_rules",
    "stage_skills_normalized",
    "stage_job_skills_bridge",

    # Phase 2: Keywords Normalization Assets
    "stage_llm_keywords_raw_extraction",
    "stage_keywords_standardization_rules",
    "stage_keyword_type_mapping",
    "stage_keywords_normalized",
    "stage_job_keywords_bridge"
]