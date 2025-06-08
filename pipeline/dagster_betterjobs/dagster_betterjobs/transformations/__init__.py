"""
Stage layer transformation modules for BetterJobs-Snow pipeline.

This package contains reusable transformation functions for cleaning,
standardizing, and enriching job data from the RAW layer to the STAGE layer.
Also includes incremental processing utilities for optimal data discovery.
"""

from .text_cleaning import (
    clean_html_tags,
    normalize_whitespace,
    standardize_job_title,
    standardize_location,
    validate_url,
    clean_job_description,
    clean_company_name
)

from .language_detection import (
    LanguageDetector,
    detect_text_language,
    process_job_dataframe,
    filter_english_jobs,
    get_sql_language_detection_query
)

from .platform_mapping import (
    PlatformMapper,
    map_all_platforms,
    get_platform_field_differences
)

from .dynamic_lookback import (
    DynamicLookbackConfig,
    get_dynamic_lookback_period,
    get_batch_lookback_periods,
)

__all__ = [
    "clean_html_tags",
    "normalize_whitespace",
    "standardize_job_title",
    "standardize_location",
    "validate_url",
    "clean_job_description",
    "clean_company_name",
    "LanguageDetector",
    "detect_text_language",
    "process_job_dataframe",
    "filter_english_jobs",
    "get_sql_language_detection_query",
    "PlatformMapper",
    "map_all_platforms",
    "get_platform_field_differences",
    "DynamicLookbackConfig",
    "get_dynamic_lookback_period",
    "get_batch_lookback_periods",
]