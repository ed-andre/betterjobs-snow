"""
Search Utilities for Enhanced Job Search

ENHANCEMENT-005: Job Search Layer Migration - Stage Data Integration

This module provides utilities for searching and filtering job data from the
STAGE layer with enhanced capabilities for quality scoring, language detection,
and deduplicated results.
"""

from typing import List, Dict, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta


class StageDataSearchConfig:
    """Configuration for stage data search operations."""

    def __init__(self,
                 min_quality_score: float = 0.5,
                 language_filter: str = "english",
                 language_confidence_min: float = 0.8,
                 enable_quality_ranking: bool = True,
                 enable_recency_ranking: bool = True):
        self.min_quality_score = min_quality_score
        self.language_filter = language_filter
        self.language_confidence_min = language_confidence_min
        self.enable_quality_ranking = enable_quality_ranking
        self.enable_recency_ranking = enable_recency_ranking


def build_stage_search_query(
    database_name: str,
    stage_schema: str,
    platforms: List[str],
    keywords: List[str] = None,
    job_titles: List[str] = None,
    locations: List[str] = None,
    excluded_keywords: List[str] = None,
    date_from: str = None,
    date_to: str = None,
    remote_filter: Optional[bool] = None,
    include_descriptions: bool = True,
    include_raw_data: bool = False,
    search_config: StageDataSearchConfig = None,
    max_results: int = 500
) -> str:
    """
    Build optimized search query for STAGE.jobs_unified table.

    Benefits over RAW data queries:
    - Single table query vs multiple table UNION
    - Cleaned and standardized text fields
    - Built-in deduplication (no cross-platform duplicates)
    - Quality scoring and language detection
    - Standardized location data

    Args:
        database_name: Snowflake database name
        stage_schema: Schema name (usually "STAGE")
        platforms: List of platforms to search
        keywords: Keywords to search for in title/description
        job_titles: Specific job titles to match
        locations: Locations to filter by
        excluded_keywords: Keywords to exclude
        date_from, date_to: Date range filters
        remote_filter: Include/exclude remote jobs
        include_descriptions: Whether to include job descriptions
        include_raw_data: Whether to include raw data
        search_config: Stage-specific search configuration
        max_results: Maximum number of results

    Returns:
        Optimized SQL query string
    """

    if search_config is None:
        search_config = StageDataSearchConfig()

    # Base SELECT with unified schema
    select_fields = [
        "job_uid",
        "job_id",
        "platform",
        "company_id",
        "company_name_clean as company_name",
        "job_title_clean as job_title",
        "location_standardized as location",
        "job_url",
        "date_posted as posting_date",
        "date_retrieved",
        "is_active",
        "employment_status",
        "department",
        "detected_language",
        "language_confidence",
        "is_english",
        "data_quality_score",
        "transformation_timestamp"
    ]

    if include_descriptions:
        select_fields.insert(6, "job_description_clean as job_description")

    if include_raw_data:
        select_fields.append("raw_data")

    query = f"""
    SELECT
        {', '.join(select_fields)}
    FROM {database_name}.{stage_schema}.jobs_unified
    WHERE is_active = TRUE
    """

    # Date filters
    if date_from:
        query += f" AND date_posted >= '{date_from}'"
    if date_to:
        query += f" AND date_posted <= '{date_to}'"

    # Platform filters
    if platforms and "all" not in platforms:
        platform_list = "', '".join(platforms)
        query += f" AND platform IN ('{platform_list}')"

    # Enhanced language filters using STAGE data
    if search_config.language_filter == "english":
        query += " AND is_english = TRUE"
        if search_config.language_confidence_min > 0:
            query += f" AND language_confidence >= {search_config.language_confidence_min}"
    elif search_config.language_filter != "all":
        query += f" AND detected_language = '{search_config.language_filter}'"
        if search_config.language_confidence_min > 0:
            query += f" AND language_confidence >= {search_config.language_confidence_min}"

    # Quality score filter
    if search_config.min_quality_score > 0:
        query += f" AND data_quality_score >= {search_config.min_quality_score}"

    # Keyword filters on cleaned fields
    if keywords:
        keyword_conditions = []
        for keyword in keywords:
            keyword_conditions.append(f"LOWER(job_title_clean) LIKE LOWER('%{keyword}%')")
            if include_descriptions:
                keyword_conditions.append(f"LOWER(job_description_clean) LIKE LOWER('%{keyword}%')")

        query += f" AND ({' OR '.join(keyword_conditions)})"

    # Job title filters on cleaned fields
    if job_titles:
        title_conditions = []
        for title in job_titles:
            title_conditions.append(f"LOWER(job_title_clean) LIKE LOWER('%{title}%')")

        query += f" AND ({' OR '.join(title_conditions)})"

    # Location filters using standardized location data
    location_or_remote_conditions = []

    if locations:
        for location in locations:
            safe_location = location.replace("'", "''")
            if location == "":
                location_or_remote_conditions.append("LOWER(location_standardized) = ''")
            else:
                location_or_remote_conditions.append(f"LOWER(location_standardized) LIKE LOWER('%{safe_location}%')")

    # Remote filter
    if remote_filter is not None and remote_filter:
        location_or_remote_conditions.extend([
            "LOWER(location_standardized) LIKE LOWER('%remote%')",
            "LOWER(job_title_clean) LIKE LOWER('%remote%')"
        ])
    elif remote_filter is not None and not remote_filter:
        query += " AND NOT (LOWER(location_standardized) LIKE LOWER('%remote%') OR LOWER(job_title_clean) LIKE LOWER('%remote%'))"

    if location_or_remote_conditions:
        query += f" AND ({' OR '.join(location_or_remote_conditions)})"

    # Exclusion filters on cleaned fields
    if excluded_keywords:
        exclusion_conditions = []
        for keyword in excluded_keywords:
            exclusion_title = f"LOWER(job_title_clean) LIKE LOWER('%{keyword}%')"
            if include_descriptions:
                exclusion_desc = f"LOWER(job_description_clean) LIKE LOWER('%{keyword}%')"
                exclusion_conditions.append(f"({exclusion_title} OR {exclusion_desc})")
            else:
                exclusion_conditions.append(exclusion_title)

        if exclusion_conditions:
            query += f" AND NOT ({' OR '.join(exclusion_conditions)})"

    # Enhanced ordering with quality and recency
    order_clauses = []
    if search_config.enable_quality_ranking:
        order_clauses.append("data_quality_score DESC")
    if search_config.enable_recency_ranking:
        order_clauses.append("date_posted DESC")

    if not order_clauses:
        order_clauses.append("date_posted DESC")  # Default

    query += f" ORDER BY {', '.join(order_clauses)}"
    query += f" LIMIT {max_results}"

    return query


def calculate_enhanced_relevance_score(row: pd.Series,
                                     keywords: List[str],
                                     job_titles: List[str],
                                     locations: List[str],
                                     use_quality_score: bool = True) -> float:
    """
    Calculate enhanced relevance score incorporating stage data quality metrics.

    This enhanced version leverages:
    - Cleaned job titles and descriptions for more accurate matching
    - Data quality scores for result ranking
    - Language confidence for scoring adjustments

    Args:
        row: Job data row
        keywords: Search keywords
        job_titles: Job title filters
        locations: Location filters
        use_quality_score: Whether to incorporate quality score

    Returns:
        Enhanced relevance score (0.0 - 1.0)
    """
    score = 0.0
    max_score = 0.0

    # Get cleaned text fields from stage data
    job_title = str(row.get("job_title", "")).lower()
    job_description = str(row.get("job_description", "")).lower()
    location = str(row.get("location", "")).lower()

    # Keyword matching (40% of score)
    if keywords:
        max_score += 0.4
        keyword_matches = sum(1 for kw in keywords if kw.lower() in job_title or kw.lower() in job_description)
        if keyword_matches > 0:
            score += 0.4 * (keyword_matches / len(keywords))

    # Job title matching (30% of score)
    if job_titles:
        max_score += 0.3
        title_matches = sum(1 for title in job_titles if title.lower() in job_title)
        if title_matches > 0:
            score += 0.3 * (title_matches / len(job_titles))

    # Location matching (20% of score)
    if locations:
        max_score += 0.2
        location_matches = sum(1 for loc in locations if loc.lower() in location)
        if location_matches > 0:
            score += 0.2 * (location_matches / len(locations))

    # Quality score enhancement (10% of score)
    if use_quality_score:
        max_score += 0.1
        quality_score = row.get("data_quality_score", 0.5)
        if quality_score:
            score += 0.1 * quality_score

    # Normalize the score
    final_score = score / max_score if max_score > 0 else 0.0

    # Apply language confidence boost for high-confidence results
    language_confidence = row.get("language_confidence", 1.0)
    if language_confidence and language_confidence > 0.9:
        final_score = min(1.0, final_score * 1.05)  # 5% boost for high confidence

    return final_score


def get_search_performance_metrics(results_df: pd.DataFrame,
                                 execution_time_ms: int) -> Dict[str, any]:
    """
    Calculate performance metrics for stage data search results.

    Args:
        results_df: Search results DataFrame
        execution_time_ms: Query execution time in milliseconds

    Returns:
        Dictionary with performance and quality metrics
    """

    if results_df.empty:
        return {
            "total_results": 0,
            "execution_time_ms": execution_time_ms,
            "avg_quality_score": 0.0,
            "platforms_represented": [],
            "language_distribution": {},
            "quality_distribution": {}
        }

    metrics = {
        "total_results": len(results_df),
        "execution_time_ms": execution_time_ms,
        "platforms_represented": results_df["platform"].unique().tolist() if "platform" in results_df.columns else [],
    }

    # Quality metrics
    if "data_quality_score" in results_df.columns:
        metrics["avg_quality_score"] = results_df["data_quality_score"].mean()
        metrics["quality_distribution"] = {
            "high (>0.8)": len(results_df[results_df["data_quality_score"] > 0.8]),
            "medium (0.5-0.8)": len(results_df[(results_df["data_quality_score"] >= 0.5) & (results_df["data_quality_score"] <= 0.8)]),
            "low (<0.5)": len(results_df[results_df["data_quality_score"] < 0.5])
        }
    else:
        metrics["avg_quality_score"] = 0.0
        metrics["quality_distribution"] = {}

    # Language distribution
    if "detected_language" in results_df.columns:
        language_counts = results_df["detected_language"].value_counts().to_dict()
        metrics["language_distribution"] = language_counts
    else:
        metrics["language_distribution"] = {}

    # Deduplication verification (check for unique job_uids)
    if "job_uid" in results_df.columns:
        metrics["unique_jobs"] = results_df["job_uid"].nunique()
        metrics["duplicate_uids_found"] = len(results_df) - metrics["unique_jobs"]
    else:
        metrics["unique_jobs"] = len(results_df)
        metrics["duplicate_uids_found"] = 0

    return metrics


def validate_stage_data_availability(conn, database_name: str = "BETTERJOBS_DB", stage_schema: str = "STAGE") -> Dict[str, any]:
    """
    Validate that stage data is available and up-to-date for search operations.

    Args:
        conn: Snowflake connection
        database_name: Database name
        stage_schema: Schema name

    Returns:
        Validation results including data freshness and availability
    """

    cursor = conn.cursor()

    try:
        # Check if table exists and has recent data
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_jobs,
            COUNT(DISTINCT platform) as platforms_available,
            MAX(date_posted) as latest_job_date,
            MAX(transformation_timestamp) as latest_processing,
            COUNT(DISTINCT job_uid) as unique_jobs,
            AVG(data_quality_score) as avg_quality
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE is_active = TRUE
        AND date_posted >= CURRENT_DATE - 30
        """)

        result = cursor.fetchone()

        if result and result[0] > 0:
            validation = {
                "table_available": True,
                "total_jobs": result[0],
                "platforms_available": result[1],
                "latest_job_date": result[2],
                "latest_processing": result[3],
                "unique_jobs": result[4],
                "avg_quality": float(result[5]) if result[5] else 0.0,
                "data_freshness_days": (datetime.now().date() - result[2]).days if result[2] else None,
                "status": "ready"
            }
        else:
            validation = {
                "table_available": True,
                "total_jobs": 0,
                "status": "no_recent_data"
            }

    except Exception as e:
        validation = {
            "table_available": False,
            "error": str(e),
            "status": "error"
        }

    finally:
        cursor.close()

    return validation