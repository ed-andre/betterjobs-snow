import os
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Set, Any
from pathlib import Path
import re
import html
import json
from dagster import (
    asset, AssetExecutionContext, Config, MetadataValue,
    get_dagster_logger, Output, Definitions, define_asset_job, RunConfig
)

logger = get_dagster_logger()

def resolve_skills_with_hierarchy(
    conn,
    skill_categories: List[str],
    skill_subcategories: List[str],
    individual_skills: List[str],
    exclude_skill_categories: List[str],
    exclude_skill_subcategories: List[str],
    exclude_individual_skills: List[str],
    database_name: str
) -> List[str]:
    """
    Resolve skills with hierarchical precedence and exclusion logic.

    Precedence (highest to lowest):
    1. skill_categories - gets ALL skills under specified categories
    2. skill_subcategories - gets ALL skills under specified subcategories
    3. individual_skills - specific skills listed

    Exclusions are applied AFTER inclusion resolution.

    Returns:
        List of final skills to include in search query
    """
    final_skills: Set[str] = set()
    excluded_skills: Set[str] = set()

    with conn.cursor() as cursor:
        # First, check if DENORM_SKILLS table has data
        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.SERVE.DENORM_SKILLS")
        skill_count = cursor.fetchone()[0]
        if skill_count == 0:
            logger.warning(f"⚠️ SERVE.DENORM_SKILLS table is empty - skills filtering will be skipped")
            return []

        logger.info(f"📊 SERVE.DENORM_SKILLS contains {skill_count} records")

        # Step 1: Resolve excluded skills first
        # Exclude skills from categories
        if exclude_skill_categories:
            categories_sql = ", ".join(["'" + c.replace("'", "''") + "'" for c in exclude_skill_categories])
            exclude_query = f"""
                SELECT SKILLS_CSV
                FROM {database_name}.SERVE.DENORM_SKILLS
                WHERE SKILL_CATEGORY IN ({categories_sql})
            """
            cursor.execute(exclude_query)
            for row in cursor.fetchall():
                if row and row[0]:
                    excluded_skills.update([s.strip().lower() for s in row[0].split(',') if s.strip()])

        # Exclude skills from subcategories
        if exclude_skill_subcategories:
            subcats_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in exclude_skill_subcategories])
            exclude_query = f"""
                SELECT SKILLS_CSV
                FROM {database_name}.SERVE.DENORM_SKILLS
                WHERE SKILL_SUBCATEGORY IN ({subcats_sql})
            """
            cursor.execute(exclude_query)
            for row in cursor.fetchall():
                if row and row[0]:
                    excluded_skills.update([s.strip().lower() for s in row[0].split(',') if s.strip()])

        # Exclude individual skills
        excluded_skills.update([s.strip().lower() for s in exclude_individual_skills])

        # Step 2: Resolve included skills by precedence

        # Highest precedence: Categories
        if skill_categories:
            categories_sql = ", ".join(["'" + c.replace("'", "''") + "'" for c in skill_categories])
            include_query = f"""
                SELECT SKILLS_CSV
                FROM {database_name}.SERVE.DENORM_SKILLS
                WHERE SKILL_CATEGORY IN ({categories_sql})
            """
            cursor.execute(include_query)
            for row in cursor.fetchall():
                if row and row[0]:
                    category_skills = [s.strip() for s in row[0].split(',') if s.strip()]
                    # Add only skills not in exclusion list
                    final_skills.update([s for s in category_skills if s.lower() not in excluded_skills])

        # Middle precedence: Subcategories (only if no categories specified)
        elif skill_subcategories:
            subcats_sql = ", ".join(["'" + s.replace("'", "''") + "'" for s in skill_subcategories])
            include_query = f"""
                SELECT SKILLS_CSV
                FROM {database_name}.SERVE.DENORM_SKILLS
                WHERE SKILL_SUBCATEGORY IN ({subcats_sql})
            """
            cursor.execute(include_query)
            for row in cursor.fetchall():
                if row and row[0]:
                    subcat_skills = [s.strip() for s in row[0].split(',') if s.strip()]
                    # Add only skills not in exclusion list
                    final_skills.update([s for s in subcat_skills if s.lower() not in excluded_skills])

        # Lowest precedence: Individual skills (only if no categories or subcategories specified)
        elif individual_skills:
            # Add only skills not in exclusion list
            final_skills.update([s for s in individual_skills if s.lower() not in excluded_skills])

    return list(final_skills)

def resolve_keywords_from_denorm_table(
    conn,
    include_keywords: List[str],
    exclude_keywords: List[str],
    database_name: str
) -> List[str]:
    """
    Resolve keywords from SERVE.DENORM_KEYWORDS table with exclusion logic.

    Args:
        conn: Database connection
        include_keywords: Keywords to include from DENORM_KEYWORDS table
        exclude_keywords: Keywords to exclude from DENORM_KEYWORDS table
        database_name: Database name

    Returns:
        List of final keywords to include in search query
    """
    final_keywords: Set[str] = set()
    excluded_keywords: Set[str] = set(k.lower() for k in exclude_keywords)

    if not include_keywords:
        return []

    with conn.cursor() as cursor:
        # First, check if DENORM_KEYWORDS table has data
        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.SERVE.DENORM_KEYWORDS")
        keyword_count = cursor.fetchone()[0]
        if keyword_count == 0:
            logger.warning(f"⚠️ SERVE.DENORM_KEYWORDS table is empty - structured keywords filtering will be skipped")
            return []

        logger.info(f"📊 SERVE.DENORM_KEYWORDS contains {keyword_count} records")

        # Get valid keywords from DENORM_KEYWORDS table
        keywords_sql = ", ".join(["'" + k.replace("'", "''") + "'" for k in include_keywords])
        query = f"""
            SELECT DISTINCT KEYWORD
            FROM {database_name}.SERVE.DENORM_KEYWORDS
            WHERE UPPER(KEYWORD) IN ({', '.join(["UPPER('" + k.replace("'", "''") + "')" for k in include_keywords])})
        """
        cursor.execute(query)

        for row in cursor.fetchall():
            if row and row[0]:
                keyword = row[0].strip()
                # Add only keywords not in exclusion list
                if keyword.lower() not in excluded_keywords:
                    final_keywords.add(keyword)

    return list(final_keywords)

def build_skills_filter_clause(resolved_skills: List[str]) -> str:
    """
    Build SQL WHERE clause for skills filtering using SKILLS_CSV column.

    Args:
        resolved_skills: List of skills to search for

    Returns:
        SQL WHERE clause string or empty string if no skills
    """
    if not resolved_skills:
        return ""

    # Build conditions for each skill using CSV search
    skill_conditions = []
    for skill in resolved_skills:
        escaped_skill = skill.replace("'", "''")
        # Use LIKE with comma boundaries to avoid partial matches
        # Check for: start of string + skill + comma, comma + skill + comma, comma + skill + end
        skill_conditions.append(f"""(
            LOWER(j.SKILLS_CSV) LIKE LOWER('{escaped_skill},%') OR
            LOWER(j.SKILLS_CSV) LIKE LOWER('%,{escaped_skill},%') OR
            LOWER(j.SKILLS_CSV) LIKE LOWER('%,{escaped_skill}') OR
            LOWER(j.SKILLS_CSV) = LOWER('{escaped_skill}')
        )""")

    return f"({' OR '.join(skill_conditions)})"

def build_keywords_filter_clause(resolved_keywords: List[str]) -> str:
    """
    Build SQL WHERE clause for keywords filtering using KEYWORDS_CSV column.

    Args:
        resolved_keywords: List of keywords to search for

    Returns:
        SQL WHERE clause string or empty string if no keywords
    """
    if not resolved_keywords:
        return ""

    # Build conditions for each keyword using CSV search
    keyword_conditions = []
    for keyword in resolved_keywords:
        escaped_keyword = keyword.replace("'", "''")
        # Use LIKE with comma boundaries to avoid partial matches
        keyword_conditions.append(f"""(
            LOWER(j.KEYWORDS_CSV) LIKE LOWER('{escaped_keyword},%') OR
            LOWER(j.KEYWORDS_CSV) LIKE LOWER('%,{escaped_keyword},%') OR
            LOWER(j.KEYWORDS_CSV) LIKE LOWER('%,{escaped_keyword}') OR
            LOWER(j.KEYWORDS_CSV) = LOWER('{escaped_keyword}')
        )""")

    return f"({' OR '.join(keyword_conditions)})"

def build_work_type_filter_clause(config: 'AdvancedJobSearchConfig') -> str:
    """
    Build SQL WHERE clause for work type filtering using ENRICHED_WORK_TYPE field.

    Handles boolean flags (remote, hybrid, on_site, etc.) and custom work_condition list.
    Multiple selections within the same category use OR logic.

    Args:
        config: AdvancedJobSearchConfig with work type settings

    Returns:
        SQL WHERE clause string or empty string if no work type filters
    """
    work_type_conditions = []

    # Handle boolean flags with case-insensitive matching for variations
    if config.remote:
        work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'REMOTE'"
        ])

    if config.hybrid:
        work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'HYBRID'"
        ])

    if config.on_site:
        work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'ON-SITE'"
        ])

        if config.full_time:
            work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'FULL-TIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) = 'FULL TIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) = 'FULL-TIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) = 'FULLTIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) LIKE 'FULL-TIME%'",  # Handles "Full-Time or Part-time available"
            "UPPER(j.ENRICHED_WORK_TYPE) LIKE 'FULL TIME%'"   # Handles variations
        ])

    if config.part_time:
        work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'PART-TIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) = 'PART TIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) = 'PARTTIME'",
            "UPPER(j.ENRICHED_WORK_TYPE) LIKE '%PART-TIME%'",  # Handles "Full-Time or Part-time"
            "UPPER(j.ENRICHED_WORK_TYPE) LIKE '%PART TIME%'"   # Handles variations
        ])

    if config.contract:
        work_type_conditions.extend([
            "UPPER(j.ENRICHED_WORK_TYPE) = 'CONTRACT'"
        ])

    # Handle custom work conditions (exact matching, case-insensitive)
    if config.work_condition:
        for condition in config.work_condition:
            escaped_condition = condition.replace("'", "''")
            work_type_conditions.append(f"UPPER(j.ENRICHED_WORK_TYPE) = UPPER('{escaped_condition}')")

    # Return combined conditions with OR logic (any matching work type)
    if work_type_conditions:
        return f"({' OR '.join(work_type_conditions)})"

    return ""

class AdvancedJobSearchConfig(Config):
    """Configuration parameters for advanced job search using SERVE denormalized data."""
    # Search parameters - REDESIGNED
    description_terms: List[str] = []  # Free-text search in job titles/descriptions (renamed from keywords)
    job_titles: List[str] = []  # Specific job titles to search for
    excluded_keywords: List[str] = []  # Keywords to exclude (legacy, use exclude_keywords instead)
    locations: List[str] = []  # Locations to include
    # Work Type Filtering (using ENRICHED_WORK_TYPE field)
    remote: bool = None  # Include remote jobs (None = don't filter)
    hybrid: bool = None  # Include hybrid jobs
    on_site: bool = None  # Include on-site jobs
    full_time: bool = None  # Include full-time jobs
    part_time: bool = None  # Include part-time jobs
    contract: bool = None  # Include contract jobs
    work_condition: List[str] = []  # Custom work type conditions (for the 20+ other values)

    # ATS platforms to include
    platforms: List[str] = ["all"]  # Options: "all", "greenhouse", "bamboohr", "workday", "smartrecruiters"

    # Date range
    days_back: int = 3  # Default to last 3 days
    date_from: Optional[str] = None  # Format: "YYYY-MM-DD"
    date_to: Optional[str] = None  # Format: "YYYY-MM-DD"

    # Enhanced stage-data specific filters
    min_quality_score: float = 0.5  # Filter by data quality
    language_filter: str = "english"  # Filter by detected language ("english", "all")
    language_confidence_min: float = 0.8  # Minimum language detection confidence

    # Enhanced ranking options
    rank_by_quality: bool = True  # Use quality score in ranking
    rank_by_recency: bool = True  # Prioritize recent postings
    deduplicate_cross_platform: bool = True  # Remove cross-platform duplicates (already done in STAGE)

    # Results control
    max_results: int = 500  # Limit number of results
    min_relevance_score: float = 0.4  # Minimum relevance score (0.0 - 1.0)
    output_format: str = "dataframe"  # Output format: "dataframe", "dict", "csv", "html"
    output_file: Optional[str] = None  # Path to save results to file, with optional {date} placeholder

    # Job identification
    job_name: str = "Job Search"  # Name of the job for display in reports

    # Advanced options
    include_descriptions: bool = True  # Include full job descriptions in output
    include_raw_data: bool = False  # Include raw API response data in output

    # NEW: Enriched data display options (ENHANCEMENT-032)
    show_enriched_data: bool = True  # Display LLM-enriched job insights
    min_enrichment_confidence: float = 0.6  # Minimum overall confidence for enriched data
    min_salary_confidence: float = 0.7  # Minimum confidence for salary data
    min_experience_confidence: float = 0.6  # Minimum confidence for experience data
    min_skills_confidence: float = 0.6  # Minimum confidence for skills data
    min_work_arrangement_confidence: float = 0.6  # Minimum confidence for work arrangement data
    min_classification_confidence: float = 0.6  # Minimum confidence for classification data

    # Display limits for enriched data
    max_skills_display: int = 8  # Maximum number of skills to display
    max_keywords_display: int = 8  # Maximum number of keywords to display
    max_office_locations_display: int = 5  # Maximum number of office locations to display

    # UI preferences for enriched data
    show_confidence_scores: bool = True  # Show confidence scores in UI
    highlight_high_confidence: bool = True  # Highlight high-confidence data

    # NEW: HIERARCHICAL SKILLS FILTERING SYSTEM (with precedence and exclusions)
    skill_categories: List[str] = []  # Highest precedence - gets ALL skills under categories
    skill_subcategories: List[str] = []  # Middle precedence - gets ALL skills under subcategories
    skills: List[str] = []  # Individual skills (lowest precedence)
    exclude_skill_categories: List[str] = []  # Exclude entire categories
    exclude_skill_subcategories: List[str] = []  # Exclude specific subcategories
    exclude_skills: List[str] = []  # Exclude individual skills

    # NEW: STRUCTURED KEYWORDS FILTERING SYSTEM (using DENORM_KEYWORDS)
    keywords: List[str] = []  # Structured keywords from SERVE.DENORM_KEYWORDS
    exclude_keywords: List[str] = []  # Exclude specific keywords from DENORM_KEYWORDS

    # Work arrangement filters (DEPRECATED - use individual boolean flags and work_condition instead)
    work_types: List[str] = []  # DEPRECATED: Use remote, hybrid, on_site, full_time, part_time, contract, work_condition instead
    office_locations: List[str] = []  # Match ENRICHED_OFFICE_LOCATIONS
    role_types: List[str] = []  # Match ENRICHED_ROLE_TYPE

    # DEPRECATED: Legacy fields (kept for backward compatibility)
    technical_skills: List[str] = []  # Use skills instead
    soft_skills: List[str] = []  # Use skills instead

    # Free-text description/title search (legacy)
    # description_terms: List[str] = []  # Already defined above

    # Skill & keyword exclusion (legacy - use exclude_* fields instead)
    # exclude_skills: List[str] = []  # Already defined above
    # exclude_skill_categories: List[str] = []  # Already defined above
    # exclude_skill_subcategories: List[str] = []  # Already defined above

@asset(
    group_name="job_search",
    kinds={"snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=["serve_denorm_job_postings", "serve_denorm_skills", "serve_denorm_keywords"]
)
def advanced_jobs_search(context: AssetExecutionContext, config: AdvancedJobSearchConfig) -> pd.DataFrame:
    """
    Search for jobs using cleaned, enriched, and deduplicated data from the SERVE denormalized layer.

    This enhanced version provides:
    - Unified schema across all platforms
    - Cleaned job titles and descriptions
    - Deduplicated results (no cross-platform duplicates)
    - Quality scores for filtering and ranking
    - Pre-aggregated skills and keywords for fast search
    - Standardized location data
    - Enhanced search capabilities

    ENHANCEMENT-041: Job Search Layer Migration - SERVE Data Integration
    """
    # Initialize Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
    stage_schema = "SERVE"  # Using Serve layer instead of STAGE

    # Initialize basic stats (will be completed after date variables are set)
    stats = {
        "query_platforms": list(config.platforms),
        "total_results": 0,
        "filtered_results": 0,
        "execution_time_ms": 0
    }

    # Log basic search parameters (detailed params logged after date calculation)
    context.log.info(f"🔍 Searching SERVE layer data with platforms: {config.platforms}, days_back: {config.days_back}")

    cursor = conn.cursor()
    start_time = datetime.now()

    try:
        # Get the latest SERVE data materialization date for accurate date range reporting
        cursor.execute(f"""
        SELECT MAX(updated_timestamp)::DATE as latest_serve_date
        FROM {database_name}.{stage_schema}.denorm_job_postings
        """)

        latest_serve_result = cursor.fetchone()
        latest_serve_date = latest_serve_result[0] if latest_serve_result and latest_serve_result[0] else datetime.now().date()

        # Prepare date filters
        date_from = None
        date_to = datetime.now().strftime("%Y-%m-%d")

        if config.date_from:
            date_from = config.date_from
        elif config.days_back > 0:
            date_from = (datetime.now() - timedelta(days=config.days_back)).strftime("%Y-%m-%d")

        if config.date_to:
            date_to = config.date_to

        # Complete stats with date information
        stats["search_params"] = {
            "description_terms": config.description_terms,  # Updated from keywords
            "structured_keywords": config.keywords,         # New structured keywords
            "job_titles": config.job_titles,
            "locations": config.locations,
            "remote": config.remote,
            "language_filter": config.language_filter,
            "min_quality_score": config.min_quality_score,
            "date_range": f"{config.days_back} days" if not config.date_from else f"{config.date_from} to {config.date_to or 'now'}",
            "actual_date_range": {
                "date_from": date_from,
                "date_to": date_to,
                "latest_serve_date": latest_serve_date.strftime("%Y-%m-%d") if isinstance(latest_serve_date, date) else str(latest_serve_date),
                "days_back_requested": config.days_back
            },
            # NEW: Hierarchical filtering stats
            "skills_filtering": {
                "skill_categories": config.skill_categories,
                "skill_subcategories": config.skill_subcategories,
                "individual_skills": config.skills,
                "exclude_skill_categories": config.exclude_skill_categories,
                "exclude_skill_subcategories": config.exclude_skill_subcategories,
                "exclude_skills": config.exclude_skills,
            },
            "keywords_filtering": {
                "include_keywords": config.keywords,
                "exclude_keywords": config.exclude_keywords
            },
            # NEW: Work type filtering stats
            "work_type_filtering": {
                "remote": config.remote,
                "hybrid": config.hybrid,
                "on_site": config.on_site,
                "full_time": config.full_time,
                "part_time": config.part_time,
                "contract": config.contract,
                "custom_conditions": config.work_condition
            }
        }

        # Log complete search parameters
        context.log.info(f"📊 Search parameters: {stats['search_params']['date_range']}, platforms: {config.platforms}")

        # ------------------------------------------------------------------
        # NEW: HIERARCHICAL SKILLS RESOLUTION with precedence and exclusions
        # ------------------------------------------------------------------
        resolved_skills = resolve_skills_with_hierarchy(
            conn=conn,
            skill_categories=config.skill_categories,
            skill_subcategories=config.skill_subcategories,
            individual_skills=config.skills,
            exclude_skill_categories=config.exclude_skill_categories,
            exclude_skill_subcategories=config.exclude_skill_subcategories,
            exclude_individual_skills=config.exclude_skills,
            database_name=database_name
        )

        context.log.info(f"🔧 Resolved {len(resolved_skills)} skills from hierarchical filtering")
        if resolved_skills:
            context.log.debug(f"Skills to include: {resolved_skills[:10]}{'...' if len(resolved_skills) > 10 else ''}")
        elif config.skill_categories or config.skill_subcategories or config.skills:
            context.log.warning(f"⚠️ No skills resolved despite config specifying categories: {config.skill_categories}, subcategories: {config.skill_subcategories}, individual: {config.skills}")
            context.log.warning("This may indicate SERVE.DENORM_SKILLS table is empty or skill names don't match")

        # ------------------------------------------------------------------
        # NEW: STRUCTURED KEYWORDS RESOLUTION from DENORM_KEYWORDS
        # ------------------------------------------------------------------
        resolved_keywords = resolve_keywords_from_denorm_table(
            conn=conn,
            include_keywords=config.keywords,
            exclude_keywords=config.exclude_keywords,
            database_name=database_name
        )

        context.log.info(f"🏷️ Resolved {len(resolved_keywords)} keywords from DENORM_KEYWORDS table")
        if resolved_keywords:
            context.log.debug(f"Keywords to include: {resolved_keywords[:10]}{'...' if len(resolved_keywords) > 10 else ''}")

        # Update stats with resolved filtering results
        stats["search_params"]["resolved_filtering"] = {
            "resolved_skills_count": len(resolved_skills),
            "resolved_keywords_count": len(resolved_keywords),
            "resolved_skills": resolved_skills[:20],  # First 20 for logging
            "resolved_keywords": resolved_keywords[:20]  # First 20 for logging
        }

        # Build the advanced search query using SERVE.DENORM_JOB_POSTINGS (denormalised)
        query = f"""
        SELECT
            j.JOB_UID                               AS job_uid,
            j.JOB_ID                                AS job_id,
            j.PLATFORM                              AS platform,
            j.COMPANY_ID                            AS company_id,
            j.COMPANY_NAME                          AS company_name,
            j.JOB_TITLE                             AS job_title,
            {"j.JOB_DESCRIPTION AS job_description," if config.include_descriptions else ""}
            j.ENRICHED_OFFICE_LOCATIONS             AS location,
            j.JOB_URL                               AS job_url,
            j.DATE_POSTED                           AS posting_date,
            j.DATE_RETRIEVED,
            j.IS_ACTIVE,
            NULL                                    AS employment_status,
            NULL                                    AS department,
            NULL                                    AS detected_language,
            NULL                                    AS language_confidence,
            NULL                                    AS is_english,
            j.DATA_QUALITY_SCORE,
            NULL                                    AS raw_data,
            j.TRANSFORMATION_TIMESTAMP,

            -- Salary (confidence filtered)
            CASE WHEN j.ENRICHED_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN j.ENRICHED_SALARY_MIN END  AS enriched_salary_min,
            CASE WHEN j.ENRICHED_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN j.ENRICHED_SALARY_MAX END  AS enriched_salary_max,
            CASE WHEN j.ENRICHED_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN j.ENRICHED_SALARY_CURRENCY END AS enriched_salary_currency,
            CASE WHEN j.ENRICHED_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN j.ENRICHED_SALARY_PERIOD END    AS enriched_salary_period,
            CASE WHEN j.ENRICHED_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN j.ENRICHED_SALARY_TYPE END      AS enriched_salary_type,

            -- Experience (confidence filtered)
            CASE WHEN j.ENRICHED_EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN j.ENRICHED_MIN_YEARS_EXPERIENCE END AS enriched_min_years_experience,
            CASE WHEN j.ENRICHED_EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN j.ENRICHED_MAX_YEARS_EXPERIENCE END AS enriched_max_years_experience,
            CASE WHEN j.ENRICHED_EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN j.ENRICHED_EXPERIENCE_LEVEL END    AS enriched_experience_level,

            -- Skills CSV fields (already aggregated)
            j.TECHNICAL_SKILLS_CSV                 AS enriched_technical_skills,
            j.SOFT_SKILLS_CSV                      AS enriched_soft_skills,

            -- Keywords and classification
            j.ENRICHED_PRIMARY_KEYWORDS,
            j.ENRICHED_INDUSTRY_KEYWORDS,
            j.ENRICHED_WORK_TYPE,
            j.ENRICHED_OFFICE_LOCATIONS            AS enriched_office_locations,
            j.ENRICHED_ROLE_TYPE,
            j.ENRICHED_TEAM_SIZE,

            -- Confidence indicators
            j.ENRICHED_OVERALL_CONFIDENCE,
            j.ENRICHED_SALARY_CONFIDENCE,
            j.ENRICHED_EXPERIENCE_CONFIDENCE,
            j.ENRICHED_SKILLS_CONFIDENCE,
            j.ENRICHED_WORK_ARRANGEMENT_CONFIDENCE,
            j.ENRICHED_CLASSIFICATION_CONFIDENCE,

            -- Aggregated lists for search optimisation
            j.SKILLS_CSV,
            j.KEYWORDS_CSV

        FROM {database_name}.{stage_schema}.DENORM_JOB_POSTINGS j
        WHERE j.IS_ACTIVE = TRUE
        """

        # Add date filters
        if date_from:
            query += f" AND DATE_POSTED >= '{date_from}'"
        if date_to:
            query += f" AND DATE_POSTED <= '{date_to}'"

        # Add platform filters
        if config.platforms and "all" not in config.platforms:
            platform_list = "', '".join(config.platforms)
            query += f" AND PLATFORM IN ('{platform_list}')"

        # Add language filters using enhanced data
        # Language filters not applicable on SERVE layer – skip

        # Add quality score filter
        if config.min_quality_score > 0:
            query += f" AND data_quality_score >= {config.min_quality_score}"

        # ------------------------------------------------------------------
        # NEW: Apply resolved skills filtering using SKILLS_CSV column
        # ------------------------------------------------------------------
        skills_filter_clause = build_skills_filter_clause(resolved_skills)
        if skills_filter_clause:
            query += f" AND {skills_filter_clause}"
            context.log.info(f"Applied skills filter for {len(resolved_skills)} skills")

        # ------------------------------------------------------------------
        # NEW: Apply resolved keywords filtering using KEYWORDS_CSV column
        # ------------------------------------------------------------------
        keywords_filter_clause = build_keywords_filter_clause(resolved_keywords)
        if keywords_filter_clause:
            query += f" AND {keywords_filter_clause}"
            context.log.info(f"Applied keywords filter for {len(resolved_keywords)} keywords")

        # Add description terms filters (free-text search) with word boundary checking
        if config.description_terms:
            keyword_conditions = []
            for keyword in config.description_terms:
                kw_lower = keyword.lower()

                # Handle single words vs multi-word phrases differently
                if ' ' in kw_lower:
                    # Multi-word phrase: use simple substring matching
                    keyword_conditions.append(f"LOWER(job_title) LIKE '%{kw_lower}%'")
                    if config.include_descriptions:
                        keyword_conditions.append(f"LOWER(job_description) LIKE '%{kw_lower}%'")
                else:
                    # Single word: use word boundary checking
                    title_condition = f"(LOWER(job_title) LIKE '% {kw_lower} %' OR LOWER(job_title) LIKE '{kw_lower} %' OR LOWER(job_title) LIKE '% {kw_lower}' OR LOWER(job_title) = '{kw_lower}')"
                    keyword_conditions.append(title_condition)

                    if config.include_descriptions:
                        desc_condition = f"(LOWER(job_description) LIKE '% {kw_lower} %' OR LOWER(job_description) LIKE '{kw_lower} %' OR LOWER(job_description) LIKE '% {kw_lower}' OR LOWER(job_description) = '{kw_lower}')"
                        keyword_conditions.append(desc_condition)

            # Combine with OR (any keyword match)
            query += f" AND ({' OR '.join(keyword_conditions)})"
            context.log.info(f"Applied keyword conditions: {len(keyword_conditions)} total")

        # Add job title filters
        if config.job_titles:
            title_conditions = []
            for title in config.job_titles:
                title_conditions.append(f"LOWER(job_title) LIKE LOWER('%{title}%')")

            query += f" AND ({' OR '.join(title_conditions)})"
            context.log.info(f"Applied job title conditions: {len(title_conditions)} total")

        # Add location filters using standardized location data (separate from work type)
        location_conditions = []

        if config.locations:
            for location in config.locations:
                # Use standardized location field for better matching
                safe_location = location.replace("'", "''")
                if location == "":
                    # Empty location means "No Location"
                    location_conditions.append("(ENRICHED_OFFICE_LOCATIONS IS NULL OR TRIM(ENRICHED_OFFICE_LOCATIONS) = '')")
                elif location == "Location":
                    # "Location" config value means "Various Locations" (e.g., "2 Locations", "3 Locations")
                    location_conditions.append("REGEXP_LIKE(ENRICHED_OFFICE_LOCATIONS, '[0-9]+\\\\s+[Ll]ocations')")
                else:
                    # Handle state abbreviations (2-3 chars) with word boundaries to avoid false positives
                    if len(safe_location) <= 3 and safe_location.isupper():
                        # State abbreviation: use word boundary with space
                        loc_condition = f"(LOWER(ENRICHED_OFFICE_LOCATIONS) LIKE LOWER('% {safe_location} %') OR LOWER(ENRICHED_OFFICE_LOCATIONS) LIKE LOWER('{safe_location} %') OR LOWER(ENRICHED_OFFICE_LOCATIONS) LIKE LOWER('% {safe_location}') OR LOWER(ENRICHED_OFFICE_LOCATIONS) = LOWER('{safe_location}'))"
                        location_conditions.append(loc_condition)
                    else:
                        # Regular location: use substring matching
                        location_conditions.append(f"LOWER(ENRICHED_OFFICE_LOCATIONS) LIKE LOWER('%{safe_location}%')")

        # Apply location conditions
        if location_conditions:
            query += f" AND ({' OR '.join(location_conditions)})"
            context.log.info(f"Applied location conditions: {len(location_conditions)} total")

        # ------------------------------------------------------------------
        # NEW: Apply work type filtering using ENRICHED_WORK_TYPE field
        # ------------------------------------------------------------------
        work_type_filter_clause = build_work_type_filter_clause(config)
        if work_type_filter_clause:
            query += f" AND {work_type_filter_clause}"

            # Log which work types are being filtered
            active_work_types = []
            if config.remote: active_work_types.append("Remote")
            if config.hybrid: active_work_types.append("Hybrid")
            if config.on_site: active_work_types.append("On-Site")
            if config.full_time: active_work_types.append("Full-Time")
            if config.part_time: active_work_types.append("Part-Time")
            if config.contract: active_work_types.append("Contract")
            if config.work_condition: active_work_types.extend(config.work_condition)

            context.log.info(f"Applied work type filter for: {', '.join(active_work_types)}")

        # Add exclusion filters
        if config.excluded_keywords:
            exclusion_conditions = []
            for keyword in config.excluded_keywords:
                exclusion_title = f"LOWER(job_title) LIKE LOWER('%{keyword}%')"
                if config.include_descriptions:
                    exclusion_desc = f"LOWER(job_description) LIKE LOWER('%{keyword}%')"
                    exclusion_conditions.append(f"({exclusion_title} OR {exclusion_desc})")
                else:
                    exclusion_conditions.append(exclusion_title)

            if exclusion_conditions:
                query += f" AND NOT ({' OR '.join(exclusion_conditions)})"
                context.log.info(f"Applied exclusion conditions: {len(exclusion_conditions)} total")

        # Add ordering based on configuration
        order_clauses = []
        if config.rank_by_quality:
            order_clauses.append("data_quality_score DESC")
        if config.rank_by_recency:
            order_clauses.append("date_posted DESC")

        if not order_clauses:
            order_clauses.append("date_posted DESC")  # Default ordering

        query += f" ORDER BY {', '.join(order_clauses)}"

        # Add result limit
        # Apply server-side limit strictly across all formats
        sql_limit = config.max_results
        query += f" LIMIT {sql_limit}"

        # Execute the query
        context.log.info("📊 Executing SERVE layer denormalized query...")
        context.log.debug(f"Query: {query}")

        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0].lower() for desc in cursor.description]

        # Convert to DataFrame
        combined_results = pd.DataFrame(results, columns=column_names)

        context.log.info(f"✅ Found {len(combined_results)} results from SERVE denormalized table")

        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        stats["execution_time_ms"] = int(execution_time)

        # Calculate relevance scores if description terms are provided
        if config.description_terms and not combined_results.empty:
            combined_results["relevance_score"] = combined_results.apply(
                lambda row: calculate_relevance_score(row, config.description_terms, config.job_titles, config.locations),
                axis=1
            )

            # Filter by minimum relevance score
            if config.min_relevance_score > 0:
                prev_count = len(combined_results)
                combined_results = combined_results[combined_results["relevance_score"] >= config.min_relevance_score]
                context.log.info(f"Filtered {prev_count - len(combined_results)} results below minimum relevance score")

        # Update stats with actual platforms found when "all" was specified
        if "all" in config.platforms and not combined_results.empty:
            actual_platforms = sorted(combined_results['platform'].unique().tolist())
            stats["query_platforms"] = actual_platforms
            context.log.info(f"Resolved 'all' platforms to: {actual_platforms}")

        stats["total_results"] = len(combined_results)

        # Limit results if needed (additional safeguard)
        result_limit = config.max_results
        if len(combined_results) > result_limit:
            combined_results = combined_results.head(result_limit)
            context.log.info(f"Limited results to {result_limit} for {config.output_format} output")

        stats["filtered_results"] = len(combined_results)

        # Save to file if requested
        if config.output_file:
            try:
                # Create the output directory if it doesn't exist
                output_file = config.output_file

                # Replace {date} placeholder with current date if present
                if "{date}" in output_file:
                    current_date = datetime.now().strftime("%Y%m%d")
                    output_file = output_file.replace("{date}", current_date)

                # Ensure directory exists
                output_dir = os.path.dirname(output_file)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    context.log.info(f"Created directory: {output_dir}")

                # Save in the appropriate format
                if config.output_format.lower() == "csv":
                    combined_results.to_csv(output_file, index=False)
                    context.log.info(f"Saved results to CSV: {output_file}")
                elif config.output_format.lower() == "html":
                    context.log.info(f"Generating enhanced HTML report for {len(combined_results)} results")
                    # Create a modern, responsive HTML report (ENHANCEMENT-006 Phase 1)
                    html_content = generate_enhanced_html_report(combined_results, stats, config)

                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    context.log.info(f"Saved results to HTML: {output_file}")
                elif config.output_format.lower() == "json":
                    combined_results.to_json(output_file, orient="records", indent=2)
                    context.log.info(f"Saved results to JSON: {output_file}")
                else:
                    # Default to CSV
                    combined_results.to_csv(output_file, index=False)
                    context.log.info(f"Saved results to {output_file}")

                # Add file path to metadata
                stats["output_file"] = output_file

            except Exception as e:
                context.log.error(f"Error saving results to file: {str(e)}")
                import traceback
                context.log.error(f"Traceback: {traceback.format_exc()}")

        # Enhanced metadata with SERVE data benefits and new filtering system
        metadata = {
            "total_results": MetadataValue.int(stats["total_results"]),
            "filtered_results": MetadataValue.int(stats["filtered_results"]),
            "platforms_searched": MetadataValue.json(stats["query_platforms"]),
            "execution_time_ms": MetadataValue.int(stats["execution_time_ms"]),
            "data_source": MetadataValue.text("SERVE.DENORM_JOB_POSTINGS (enhanced with skills & keywords filtering)"),
            "language_filter": MetadataValue.text(config.language_filter),
            "min_quality_score": MetadataValue.float(config.min_quality_score),
            "resolved_skills_count": MetadataValue.int(len(resolved_skills)),
            "resolved_keywords_count": MetadataValue.int(len(resolved_keywords)),
            "skills_hierarchy": MetadataValue.json({
                "categories": config.skill_categories,
                "subcategories": config.skill_subcategories,
                "individual": config.skills[:10],  # Limit for display
                "excluded_categories": config.exclude_skill_categories,
                "excluded_subcategories": config.exclude_skill_subcategories,
                "excluded_skills": config.exclude_skills[:10]
            }),
            "keywords_filtering": MetadataValue.json({
                "included": config.keywords[:10],  # Limit for display
                "excluded": config.exclude_keywords[:10]
            }),
            "work_type_filtering": MetadataValue.json({
                "remote": config.remote,
                "hybrid": config.hybrid,
                "on_site": config.on_site,
                "full_time": config.full_time,
                "part_time": config.part_time,
                "contract": config.contract,
                "custom_conditions": config.work_condition[:5]  # Limit for display
            }),
            "preview": MetadataValue.md(generate_results_preview(combined_results))
        }

        # Add quality metrics if available
        if not combined_results.empty and 'data_quality_score' in combined_results.columns:
            avg_quality = combined_results['data_quality_score'].mean()
            metadata["avg_quality_score"] = MetadataValue.float(float(avg_quality))

        context.add_output_metadata(metadata)

        # Return in requested format
        if config.output_format == "dict":
            return combined_results.to_dict(orient="records")
        else:  # Default to dataframe
            return combined_results

    except Exception as e:
        context.log.error(f"Error executing advanced job search: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_relevance_score(row: pd.Series, keywords: List[str], job_titles: List[str], locations: List[str]) -> float:
    """
    Calculate a relevance score between 0.0 and 1.0 for a job based on how well it matches search criteria.
    """
    score = 0.0
    max_score = 0.0

    # Get the text fields to search in
    job_title = str(row.get("job_title", "")).lower()
    job_description = str(row.get("job_description", "")).lower()
    location = str(row.get("location", "")).lower()

    # Score matching keywords (50% of total score)
    if keywords:
        max_score += 0.5
        keyword_matches = sum(1 for kw in keywords if kw.lower() in job_title or kw.lower() in job_description)
        if keyword_matches > 0:
            score += 0.5 * (keyword_matches / len(keywords))

    # Score matching job titles (30% of total score)
    if job_titles:
        max_score += 0.3
        title_matches = sum(1 for title in job_titles if title.lower() in job_title)
        if title_matches > 0:
            score += 0.3 * (title_matches / len(job_titles))

    # Score matching locations (20% of total score)
    if locations:
        max_score += 0.2
        location_matches = sum(1 for loc in locations if loc.lower() in location)
        if location_matches > 0:
            score += 0.2 * (location_matches / len(locations))

    # Normalize the score if we had any scoring criteria
    return score / max_score if max_score > 0 else 0.0

def generate_results_preview(results: pd.DataFrame) -> str:
    """Generate a markdown preview of the results for the Dagster UI."""
    if results.empty:
        return "No results found matching the search criteria."

    preview = "## Job Search Results\n\n"
    preview += f"Found {len(results)} matching jobs.\n\n"

    # Add a table of the first 5 results
    preview += "| Platform | Job Title | Location | Date Posted |\n"
    preview += "|----------|-----------|----------|-------------|\n"

    for _, row in results.head(5).iterrows():
        platform = row.get("platform", "")
        title = row.get("job_title", "")
        location = row.get("location", "")
        date = row.get("posting_date", "")

        if isinstance(date, pd.Timestamp):
            date = date.strftime("%Y-%m-%d")

        preview += f"| {platform} | {title[:50] + '...' if len(title) > 50 else title} "
        preview += f"| {location[:30] + '...' if len(location) > 30 else location} | {date} |\n"

    if len(results) > 5:
        preview += f"\n... and {len(results) - 5} more results."

    return preview

# NEW: Enriched data processing functions (ENHANCEMENT-032)
def process_enriched_data(job_row: pd.Series, config: AdvancedJobSearchConfig) -> Dict[str, Any]:
    """
    Process enriched LLM data for display with confidence-based filtering.

    Args:
        job_row: Pandas Series containing job data with enriched fields
        config: AdvancedJobSearchConfig with confidence thresholds

    Returns:
        Dictionary containing processed enriched data sections
    """
    import json

    enriched = {}

    # Salary information (confidence-based filtering)
    if (pd.notna(job_row.get('enriched_salary_min')) and
        pd.notna(job_row.get('enriched_salary_confidence')) and
        job_row.get('enriched_salary_confidence', 0) >= config.min_salary_confidence):
        enriched['salary'] = {
            'min': int(job_row['enriched_salary_min']),
            'max': int(job_row['enriched_salary_max']) if pd.notna(job_row.get('enriched_salary_max')) else None,
            'currency': job_row.get('enriched_salary_currency', 'USD'),
            'period': job_row.get('enriched_salary_period', 'year'),
            'type': job_row.get('enriched_salary_type', 'base'),
            'confidence': float(job_row.get('enriched_salary_confidence', 0)) if pd.notna(job_row.get('enriched_salary_confidence')) else 0.0
        }

    # Experience requirements (confidence-based filtering)
    if (pd.notna(job_row.get('enriched_min_years_experience')) and
        pd.notna(job_row.get('enriched_experience_confidence')) and
        job_row.get('enriched_experience_confidence', 0) >= config.min_experience_confidence):
        enriched['experience'] = {
            'min_years': int(job_row['enriched_min_years_experience']),
            'max_years': int(job_row['enriched_max_years_experience']) if pd.notna(job_row.get('enriched_max_years_experience')) else None,
            'level': job_row.get('enriched_experience_level'),
            'confidence': float(job_row.get('enriched_experience_confidence', 0)) if pd.notna(job_row.get('enriched_experience_confidence')) else 0.0
        }

    # Technical skills (confidence-based filtering)
    if (job_row.get('enriched_technical_skills') and
        pd.notna(job_row.get('enriched_skills_confidence')) and
        job_row.get('enriched_skills_confidence', 0) >= config.min_skills_confidence):
        try:
            raw_val = job_row['enriched_technical_skills']
            if isinstance(raw_val, str):
                try:
                    skills_data = json.loads(raw_val)
                except json.JSONDecodeError:
                    skills_data = [s.strip() for s in raw_val.split(',') if s.strip()]
            else:
                skills_data = raw_val
            if skills_data and len(skills_data) > 0:
                # Limit skills to max display and ensure they're strings
                skills_list = [str(skill) for skill in skills_data[:config.max_skills_display]]
                enriched['technical_skills'] = {
                    'skills': skills_list,
                    'confidence': float(job_row.get('enriched_skills_confidence', 0)) if pd.notna(job_row.get('enriched_skills_confidence')) else 0.0
                }
        except (TypeError, AttributeError):
            pass

    # Soft skills (confidence-based filtering)
    if (job_row.get('enriched_soft_skills') and
        pd.notna(job_row.get('enriched_skills_confidence')) and
        job_row.get('enriched_skills_confidence', 0) >= config.min_skills_confidence):
        try:
            raw_val = job_row['enriched_soft_skills']
            if isinstance(raw_val, str):
                try:
                    soft_skills_data = json.loads(raw_val)
                except json.JSONDecodeError:
                    soft_skills_data = [s.strip() for s in raw_val.split(',') if s.strip()]
            else:
                soft_skills_data = raw_val
            if soft_skills_data and len(soft_skills_data) > 0:
                skills_list = [str(skill) for skill in soft_skills_data[:config.max_skills_display]]
                enriched['soft_skills'] = {
                    'skills': skills_list,
                    'confidence': float(job_row.get('enriched_skills_confidence', 0)) if pd.notna(job_row.get('enriched_skills_confidence')) else 0.0
                }
        except (TypeError, AttributeError):
            pass

    # Work arrangements (confidence-based filtering)
    if (pd.notna(job_row.get('enriched_work_arrangement_confidence')) and
        job_row.get('enriched_work_arrangement_confidence', 0) >= config.min_work_arrangement_confidence):
        work_arrangement = {}
        if job_row.get('enriched_work_type'):
            work_arrangement['work_type'] = str(job_row['enriched_work_type'])
        if job_row.get('enriched_office_locations'):
            try:
                locations_data = json.loads(job_row['enriched_office_locations']) if isinstance(job_row['enriched_office_locations'], str) else job_row['enriched_office_locations']
                if locations_data:
                    # Limit office locations and ensure they're strings
                    locations_list = [str(loc) for loc in locations_data[:config.max_office_locations_display]]
                    work_arrangement['office_locations'] = locations_list
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        if work_arrangement:
            work_arrangement['confidence'] = float(job_row.get('enriched_work_arrangement_confidence', 0)) if pd.notna(job_row.get('enriched_work_arrangement_confidence')) else 0.0
            enriched['work_arrangement'] = work_arrangement

    # Job classification (confidence-based filtering)
    if (pd.notna(job_row.get('enriched_classification_confidence')) and
        job_row.get('enriched_classification_confidence', 0) >= config.min_classification_confidence):
        classification = {}

        # Keywords (primary and industry)
        keywords = []
        if job_row.get('enriched_primary_keywords'):
            try:
                primary_kw = json.loads(job_row['enriched_primary_keywords']) if isinstance(job_row['enriched_primary_keywords'], str) else job_row['enriched_primary_keywords']
                if primary_kw:
                    keywords.extend([str(kw) for kw in primary_kw[:5]])  # Top 5 primary keywords
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        if job_row.get('enriched_industry_keywords'):
            try:
                industry_kw = json.loads(job_row['enriched_industry_keywords']) if isinstance(job_row['enriched_industry_keywords'], str) else job_row['enriched_industry_keywords']
                if industry_kw:
                    keywords.extend([str(kw) for kw in industry_kw[:3]])  # Top 3 industry keywords
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        # Limit total keywords to max display
        if keywords:
            classification['keywords'] = keywords[:config.max_keywords_display]

        if job_row.get('enriched_role_type'):
            classification['role_type'] = str(job_row['enriched_role_type'])

        if job_row.get('enriched_team_size'):
            classification['team_size'] = str(job_row['enriched_team_size'])

        if classification:
            classification['confidence'] = float(job_row.get('enriched_classification_confidence', 0)) if pd.notna(job_row.get('enriched_classification_confidence')) else 0.0
            enriched['classification'] = classification

    # Overall metadata / presence detection – SERVE layer has no LLM_PROCESSED flag.
    overall_conf = job_row.get('enriched_overall_confidence')
    enriched['overall_confidence'] = float(overall_conf) if pd.notna(overall_conf) else 0.0

    # Mark enrichment present if we have any confidence value or at least one enriched field populated
    enriched['has_enrichment'] = (
        pd.notna(overall_conf) and overall_conf is not None
    ) or (
        bool(enriched.get('technical_skills')) or bool(enriched.get('soft_skills')) or bool(enriched.get('salary')) or bool(enriched.get('experience')) or bool(enriched.get('work_arrangement')) or bool(enriched.get('classification'))
    )

    # Processing date – use transformation_timestamp which is always present
    enriched['processing_date'] = job_row.get('transformation_timestamp')

    return enriched

def format_salary_display(salary_data: Dict) -> str:
    """Format salary information for display."""
    min_salary = salary_data['min']
    max_salary = salary_data.get('max')
    currency = salary_data.get('currency', 'USD')
    period = salary_data.get('period', 'year')

    # Format currency symbol
    currency_symbol = '$' if currency.upper() == 'USD' else f'{currency} '

    if max_salary and max_salary != min_salary:
        return f"{currency_symbol}{min_salary:,} - {currency_symbol}{max_salary:,} per {period}"
    else:
        return f"{currency_symbol}{min_salary:,}+ per {period}"

def format_experience_display(exp_data: Dict) -> str:
    """Format experience requirements for display."""
    min_years = exp_data['min_years']
    max_years = exp_data.get('max_years')
    level = exp_data.get('level')

    if min_years == 0:
        years_text = "Entry level"
    elif max_years and max_years != min_years:
        years_text = f"{min_years}-{max_years} years"
    else:
        years_text = f"{min_years}+ years"

    return f"{years_text}" + (f" ({level})" if level else "")

def generate_salary_section_html(salary_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for salary information."""
    if not salary_data:
        return ""

    salary_display = format_salary_display(salary_data)

    return f"💰 <strong>Salary Range:</strong> {salary_display}"

def generate_experience_section_html(exp_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for experience requirements."""
    if not exp_data:
        return ""

    experience_display = format_experience_display(exp_data)

    return f"🎯 <strong>Experience Required:</strong> {experience_display}"

def generate_skills_section_html(skills_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for technical skills with badges."""
    if not skills_data or not skills_data.get('skills'):
        return ""

    skills = skills_data['skills']

    # Create skill badges
    skill_badges = ''.join([f'<span class="skill-badge">{html.escape(str(skill))}</span>' for skill in skills[:config.max_skills_display]])
    if len(skills) > config.max_skills_display:
        skill_badges += f'<span class="more-badge">+{len(skills) - config.max_skills_display} more</span>'

    return f"🛠️ <strong>Technical Skills:</strong> {skill_badges}"

def generate_soft_skills_section_html(soft_skills_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for soft skills with badges."""
    if not soft_skills_data or not soft_skills_data.get('skills'):
        return ""

    skills = soft_skills_data['skills']

    skill_badges = ''.join([f'<span class="skill-badge">{html.escape(str(skill))}</span>' for skill in skills[:config.max_skills_display]])
    if len(skills) > config.max_skills_display:
        skill_badges += f'<span class="more-badge">+{len(skills) - config.max_skills_display} more</span>'

    return f"🤝 <strong>Soft Skills:</strong> {skill_badges}"

def generate_work_arrangement_section_html(work_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for work arrangement."""
    if not work_data:
        return ""

    content_parts = []
    if work_data.get('work_type'):
        content_parts.append(f"Type: {html.escape(work_data['work_type'])}")

    if work_data.get('office_locations'):
        locations_text = ', '.join([html.escape(str(loc)) for loc in work_data['office_locations'][:config.max_office_locations_display]])
        if len(work_data['office_locations']) > config.max_office_locations_display:
            locations_text += f" (+{len(work_data['office_locations']) - config.max_office_locations_display} more)"
        content_parts.append(f"Locations: {locations_text}")

    content_text = ', '.join(content_parts) if content_parts else "Not specified"

    return f"🏢 <strong>Work Arrangement:</strong> {content_text}"

def generate_classification_section_html(classification_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate inline text for job classification (role type and team size only)."""
    if not classification_data:
        return ""

    content_parts = []

    # Role type
    if classification_data.get('role_type'):
        content_parts.append(f"👤 <strong>Role Type:</strong> {html.escape(classification_data['role_type'])}")

    # Team size
    if classification_data.get('team_size'):
        content_parts.append(f"👥 <strong>Team Size:</strong> {html.escape(classification_data['team_size'])}")

    if not content_parts:
        return ""

    # Join the parts
    return ' '.join(content_parts)

def generate_keywords_section_html(classification_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate keywords section for display on separate line."""
    if not classification_data or not classification_data.get('keywords'):
        return ""

    keywords = classification_data['keywords']
    keyword_badges = ''.join([f'<span class="keyword-badge">{html.escape(str(kw))}</span>' for kw in keywords[:config.max_keywords_display]])
    if len(keywords) > config.max_keywords_display:
        keyword_badges += f'<span class="more-badge">+{len(keywords) - config.max_keywords_display} more</span>'

    return f"🏷️ <strong>Keywords:</strong> {keyword_badges}"

def generate_enriched_insights_section_html(enriched_data: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate complete enriched job insights section HTML with keywords on separate line."""
    if not enriched_data.get('has_enrichment') or not config.show_enriched_data:
        return ""

    # ----------------- Build top and bottom sections -----------------
    top_sections: List[str] = []  # single horizontal line
    bottom_sections: List[str] = []  # each element will become its own line

    # Top sections (keep inline)
    if enriched_data.get('salary'):
        top_sections.append(generate_salary_section_html(enriched_data['salary'], config))

    if enriched_data.get('experience'):
        top_sections.append(generate_experience_section_html(enriched_data['experience'], config))

    if enriched_data.get('work_arrangement'):
        top_sections.append(generate_work_arrangement_section_html(enriched_data['work_arrangement'], config))

    if enriched_data.get('classification'):
        # Role Type + Team Size stay on top line
        top_sections.append(generate_classification_section_html(enriched_data['classification'], config))

    # Bottom sections (each on its own line) – Technical, Soft, Keywords
    if enriched_data.get('technical_skills'):
        bottom_sections.append(generate_skills_section_html(enriched_data['technical_skills'], config))

    if enriched_data.get('soft_skills'):
        bottom_sections.append(generate_soft_skills_section_html(enriched_data['soft_skills'], config))

    if enriched_data.get('classification'):
        kw_line = generate_keywords_section_html(enriched_data['classification'], config)
        if kw_line:
            bottom_sections.append(kw_line)

    # Nothing to show
    if not top_sections and not bottom_sections:
        return ""

    # Assemble HTML parts
    content_lines: List[str] = []

    if top_sections:
        top_line = '&nbsp;&nbsp;&nbsp;&nbsp;'.join(top_sections)
        content_lines.append(top_line)

    # Each bottom section on its own line
    content_lines.extend(bottom_sections)

    sections_text = '<br>'.join(content_lines)

    return f"""
    <div class="job-insights" data-has-enrichment="true">
        <div class="insights-header">
            <h4 class="insights-title">💡 Job Insights</h4>
        </div>
        <div class="insights-content">
            {sections_text}
        </div>
    </div>
    """

def sanitize_html_description(description: str) -> str:
    """
    Sanitize and prepare job description HTML for safe display.
    This handles common issues with HTML content in job descriptions.
    """
    if not description:
        return "<p><em>No description available</em></p>"

    # Unescape HTML entities
    sanitized = html.unescape(description)

    # Replace any double-escaped entities that might remain
    sanitized = sanitized.replace("&amp;", "&")

    # Fix common issues with HTML formatting
    sanitized = sanitized.replace("\n", " ")
    sanitized = sanitized.replace("\r", " ")

    # Clean up excessive whitespace
    sanitized = re.sub(r'\s+', ' ', sanitized)

    return sanitized

def generate_enhanced_html_report(results: pd.DataFrame, stats: Dict, config: AdvancedJobSearchConfig) -> str:
    """Generate a modern, responsive HTML report with client-side filtering (ENHANCEMENT-006 Phase 2)."""

    # Modern CSS with improved design, typography, and responsiveness
    css = """
    <style>
        /* Modern CSS Reset and Typography */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --primary-color: #2563eb;
            --primary-dark: #1d4ed8;
            --secondary-color: #64748b;
            --success-color: #059669;
            --warning-color: #d97706;
            --error-color: #dc2626;
            --background: #f8fafc;
            --surface: #ffffff;
            --surface-elevated: #f1f5f9;
            --text-primary: #0f172a;
            --text-secondary: #475569;
            --text-muted: #64748b;
            --border-color: #e2e8f0;
            --border-light: #f1f5f9;
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
            --radius-sm: 0.375rem;
            --radius-md: 0.5rem;
            --radius-lg: 0.75rem;
            --radius-xl: 1rem;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: var(--text-primary);
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            margin: 0;
            padding: 20px;
        }

        .app-container {
            max-width: 1400px;
            margin: 0 auto;
            background: var(--surface);
            border-radius: var(--radius-xl);
            box-shadow: var(--shadow-lg);
            overflow: hidden;
            min-height: calc(100vh - 40px);
        }

        /* Header Section */
        .header {
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
            color: white;
            padding: 2rem;
            position: relative;
            overflow: hidden;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grain" width="100" height="100" patternUnits="userSpaceOnUse"><circle cx="50" cy="50" r="1" fill="white" opacity="0.1"/></pattern></defs><rect width="100" height="100" fill="url(%23grain)"/></svg>');
            opacity: 0.1;
        }

        .header-content {
            position: relative;
            z-index: 1;
        }

        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            letter-spacing: -0.025em;
        }

        .header-subtitle {
            font-size: 1.125rem;
            opacity: 0.9;
            margin-bottom: 1rem;
        }

        .header-stats {
            display: flex;
            gap: 2rem;
            flex-wrap: wrap;
        }

        .header-stat {
            background: rgba(255, 255, 255, 0.1);
            padding: 0.75rem 1.25rem;
            border-radius: var(--radius-lg);
            backdrop-filter: blur(10px);
        }

        .header-stat-label {
            font-size: 0.875rem;
            opacity: 0.8;
            margin-bottom: 0.25rem;
        }

        .header-stat-value {
            font-size: 1.5rem;
            font-weight: 600;
        }

        /* Main Content Layout */
        .main-content {
            display: grid;
            grid-template-columns: 320px 1fr;
            gap: 0;
            min-height: 600px;
        }

        /* Filter Panel */
        .filter-panel {
            background: var(--surface-elevated);
            border-right: 1px solid var(--border-color);
            padding: 1.5rem;
            overflow-y: auto;
            max-height: calc(100vh - 200px);
        }

        .filter-section {
            margin-bottom: 1.5rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid var(--border-light);
        }

        .filter-section:last-child {
            border-bottom: none;
        }

        .filter-title {
            font-size: 0.875rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .filter-content {
            font-size: 0.875rem;
            color: var(--text-secondary);
            line-height: 1.5;
        }

        .filter-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-top: 0.5rem;
        }

        .filter-tag {
            background: var(--primary-color);
            color: white;
            padding: 0.25rem 0.75rem;
            border-radius: var(--radius-md);
            font-size: 0.75rem;
            font-weight: 500;
            display: inline-flex;
            align-items: center;
            gap: 0.375rem;
        }

        .tag-text {
            display: inline;
        }

        .filter-option {
            cursor: pointer;
            transition: all 0.2s ease;
            user-select: none;
        }

        .filter-option:hover {
            transform: translateY(-1px);
            box-shadow: var(--shadow-sm);
        }

        .filter-option.active {
            background: var(--success-color);
            color: white;
        }

        .filter-option.inactive {
            background: var(--text-muted);
            opacity: 0.6;
        }

        .clear-filters-btn {
            background: var(--primary-color);
            color: white;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: var(--radius-md);
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            width: 100%;
        }

        .clear-filters-btn:hover {
            background: var(--primary-dark);
            transform: translateY(-1px);
            box-shadow: var(--shadow-sm);
        }



        /* Job Results Section */
        .job-results {
            padding: 1.5rem;
            background: var(--background);
            overflow-y: auto;
            max-height: calc(100vh - 200px);
        }

        .results-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border-color);
        }

        .results-title {
            font-size: 1.5rem;
            font-weight: 600;
            color: var(--text-primary);
        }

        .results-count {
            font-size: 0.875rem;
            color: var(--text-muted);
            background: var(--surface);
            padding: 0.5rem 1rem;
            border-radius: var(--radius-md);
            border: 1px solid var(--border-color);
        }

        /* Job Cards */
        .job-grid {
            display: grid;
            gap: 1.5rem;
        }

        .job-card {
            background: var(--surface);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--border-color);
            overflow: hidden;
            transition: all 0.2s ease-in-out;
            position: relative;
        }

        .job-card:hover {
            box-shadow: var(--shadow-md);
            transform: translateY(-1px);
            border-color: var(--primary-color);
        }

        .job-card-header {
            padding: 1.5rem 1.5rem 1rem 1.5rem;
            position: relative;
        }

        .job-badges {
            position: absolute;
            top: 1rem;
            right: 1rem;
            display: flex;
            gap: 0.5rem;
        }

        .job-badge {
            padding: 0.25rem 0.75rem;
            border-radius: var(--radius-md);
            font-size: 0.75rem;
            font-weight: 600;
        }

        .badge-relevance {
            background: var(--primary-color);
            color: white;
        }

        .badge-quality {
            background: var(--success-color);
            color: white;
        }

        .job-title {
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
            margin-right: 120px;
            line-height: 1.4;
        }

        .job-company {
            font-size: 1rem;
            color: var(--primary-color);
            font-weight: 500;
            margin-bottom: 0.5rem;
        }

        .job-meta {
            display: none;
        }

        /* Compact meta (date + platform) positioned under badges */
        .job-meta-right {
            position: absolute;
            top: 3.2rem;
            right: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.35rem;
            font-size: 0.825rem;
            color: var(--text-secondary);
        }

        .job-meta-right .job-meta-icon {
            width: 14px;
            height: 14px;
            opacity: 0.7;
        }

        .job-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;

        }

        .job-tag {
            padding: 0.25rem 0.75rem;
            border-radius: var(--radius-md);
            font-size: 0.75rem;
            font-weight: 500;
            border: 1px solid var(--border-color);
        }

        .tag-remote {
            background: #dcfce7;
            color: var(--success-color);
            border-color: #bbf7d0;
        }

        .tag-department {
            background: #dbeafe;
            color: var(--primary-color);
            border-color: #bfdbfe;
        }

        .tag-language {
            background: #fef3c7;
            color: var(--warning-color);
            border-color: #fed7aa;
        }

        .tag-uid {
            background: var(--surface-elevated);
            color: var(--text-muted);
            border-color: var(--border-light);
        }

        .job-actions {
            padding: 1rem 1.5rem;
            background: var(--surface-elevated);
            border-top: 1px solid var(--border-light);
        }

        .job-link {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            color: var(--primary-color);
            text-decoration: none;
            font-weight: 500;
            font-size: 0.875rem;
            padding: 0.5rem 1rem;
            border: 1px solid var(--primary-color);
            border-radius: var(--radius-md);
            transition: all 0.2s ease-in-out;
        }

        .job-link:hover {
            background: var(--primary-color);
            color: white;
            transform: translateY(-1px);
            box-shadow: var(--shadow-sm);
        }

        .job-description {
            padding: 1.5rem;
            border-top: 1px solid var(--border-light);
            max-height: 300px;
            overflow-y: auto;
            font-size: 0.875rem;
            line-height: 1.6;
            color: var(--text-secondary);
        }

        .job-description::-webkit-scrollbar {
            width: 6px;
        }

        .job-description::-webkit-scrollbar-track {
            background: var(--surface-elevated);
        }

        .job-description::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 3px;
        }

        .job-description p {
            margin-bottom: 1em;
        }

        .job-description ul, .job-description ol {
            margin-left: 1.5em;
            margin-bottom: 1em;
        }

        .job-description li {
            margin-bottom: 0.5em;
        }

        .job-description h1, .job-description h2, .job-description h3,
        .job-description h4, .job-description h5 {
            margin-top: 1em;
            margin-bottom: 0.5em;
            color: var(--text-primary);
            font-weight: 600;
        }

        .job-description a {
            color: var(--primary-color);
            text-decoration: none;
        }

        .job-description a:hover {
            text-decoration: underline;
        }

        .job-description strong, .job-description b {
            font-weight: 600;
            color: var(--text-primary);
        }

        /* No Results State */
        .no-results {
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-muted);
        }

        .no-results h2 {
            font-size: 1.5rem;
            color: var(--text-secondary);
            margin-bottom: 1rem;
        }

        /* Responsive Design */
        @media (max-width: 1024px) {
            .main-content {
                grid-template-columns: 280px 1fr;
            }

            .header h1 {
                font-size: 2rem;
            }

            .header-stats {
                gap: 1rem;
            }
        }

        @media (max-width: 768px) {
            body {
                padding: 10px;
            }

            .app-container {
                border-radius: var(--radius-lg);
                min-height: calc(100vh - 20px);
            }

            .main-content {
                grid-template-columns: 1fr;
            }

            .filter-panel {
                border-right: none;
                border-bottom: 1px solid var(--border-color);
                max-height: none;
                padding: 1rem;
            }

            .job-results {
                max-height: none;
            }

            .header {
                padding: 1.5rem;
            }

            .header h1 {
                font-size: 1.75rem;
            }

            .header-stats {
                flex-direction: column;
                gap: 0.75rem;
            }

            .job-meta {
                grid-template-columns: 1fr;
            }

            .job-badges {
                position: static;
                margin-bottom: 1rem;
                justify-content: flex-start;
            }

            .job-title {
                margin-right: 0;
            }

            .insights-horizontal {
                flex-direction: column;
            }

            .insight-card {
                min-width: unset;
                max-width: unset;
            }

            .insights-content {
                font-size: 0.85rem;
                line-height: 1.4;
            }

            .skill-badge, .keyword-badge, .more-badge {
                font-size: 0.7rem;
                padding: 0.15rem 0.4rem;
                margin: 0 0.2rem 0.3rem 0;
            }
        }

        @media (max-width: 480px) {
            .header {
                padding: 1rem;
            }

            .job-results {
                padding: 1rem;
            }

            .job-card-header {
                padding: 1rem;
            }

            .job-description {
                padding: 1rem;
            }
        }

        /* Print Styles */
        @media print {
            body {
                background: white;
                padding: 0;
            }

            .app-container {
                box-shadow: none;
                border-radius: 0;
            }

            .job-card {
                break-inside: avoid;
                box-shadow: none;
                border: 1px solid #ccc;
                margin-bottom: 1rem;
            }

            .job-description {
                max-height: none;
                overflow: visible;
            }
        }

        /* NEW: Enriched Job Insights Styles (ENHANCEMENT-032) */
        .job-insights {
            padding: 1rem;
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            border-top: 1px solid var(--border-light);
            border-bottom: 1px solid var(--border-light);
        }

        .job-insights[data-has-enrichment="false"] {
            display: none;
        }

        .insights-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
        }

        .insights-title {
            font-size: 1rem;
            font-weight: 600;
            color: var(--text-primary);
            margin: 0;
        }

        .insights-content {
            font-size: 0.9rem;
            line-height: 1.8;
            color: var(--text-secondary);
        }

        .insights-content strong {
            color: var(--text-primary);
            font-weight: 600;
        }

        .skill-badge {
            display: inline-block;
            background: var(--primary-color);
            color: white;
            padding: 0.2rem 0.5rem;
            border-radius: var(--radius-sm);
            font-size: 0.75rem;
            font-weight: 500;
            margin: 0 0.25rem 0.4rem 0;
        }

        .keyword-badge {
            display: inline-block;
            background: var(--warning-color);
            color: white;
            padding: 0.2rem 0.5rem;
            border-radius: var(--radius-sm);
            font-size: 0.75rem;
            font-weight: 500;
            margin: 0 0.25rem 0.4rem 0;
        }

        .more-badge {
            display: inline-block;
            background: var(--text-muted);
            color: white;
            padding: 0.2rem 0.5rem;
            border-radius: var(--radius-sm);
            font-size: 0.75rem;
            font-weight: 500;
            margin: 0 0.25rem 0.4rem 0;
        }

        .insights-horizontal {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            align-items: flex-start;
        }

        .insight-card {
            background: var(--surface);
            border-radius: var(--radius-md);
            padding: 0.75rem;
            border: 1px solid var(--border-color);
            box-shadow: var(--shadow-sm);
            flex: 0 0 auto;
            min-width: 180px;
            max-width: 250px;
        }
    </style>
    """

    # Generate the HTML with modern structure and client-side filtering
    today = datetime.now().strftime("%B %d, %Y")
    search_date_range = f"{stats['search_params']['date_range']}"
    platforms = ", ".join(stats["query_platforms"])
    job_name = config.job_name

    # Format actual date range for display
    actual_range = stats['search_params']['actual_date_range']
    date_from_display = datetime.strptime(actual_range['date_from'], "%Y-%m-%d").strftime("%B %d, %Y") if actual_range['date_from'] else "N/A"
    date_to_display = datetime.strptime(actual_range['date_to'], "%Y-%m-%d").strftime("%B %d, %Y") if actual_range['date_to'] else "N/A"
    latest_serve_display = datetime.strptime(actual_range['latest_serve_date'], "%Y-%m-%d").strftime("%B %d, %Y")
    days_back = actual_range['days_back_requested']

    # Extract filter data for display
    keywords = stats['search_params']['structured_keywords']
    job_titles = stats['search_params']['job_titles']
    locations = stats['search_params']['locations']
    remote = stats['search_params'].get('remote', None)
    language_filter = stats['search_params'].get('language_filter', 'english')
    min_quality_score = stats['search_params'].get('min_quality_score', 0.5)

    # Process locations for display
    display_locations = []
    for location in locations:
        if location == "":
            display_locations.append("No Location")
        elif location == "Location":
            display_locations.append("Various Locations")
        else:
            display_locations.append(location)

    # Add Remote if remote search is enabled
    if remote is True:
        display_locations.append("Remote")

    # Prepare job data with pre-computed matches for URL parameter filtering
    job_data_for_filtering = []
    if not results.empty:
        for _, job in results.iterrows():
            # Get searchable text
            job_title = str(job.get('job_title', '')).lower()
            job_description = str(job.get('job_description', '')).lower()
            search_text = f"{job_title} {job_description}"

            # Pre-compute keyword matches (handle single words vs phrases)
            search_text_lower = search_text.lower()
            search_words = set(search_text_lower.split())
            matched_keywords = []

            for kw in config.keywords:
                kw_lower = kw.lower()
                if ' ' in kw_lower:
                    # Multi-word phrase: check if phrase exists in text
                    if kw_lower in search_text_lower:
                        matched_keywords.append(kw)
                else:
                    # Single word: check for exact word match
                    if kw_lower in search_words:
                        matched_keywords.append(kw)

                        # Pre-compute job title matches (only check actual job title, not description)
            matched_job_titles = [jt for jt in config.job_titles if jt.lower() in job_title]

            # Check if remote
            location = str(job.get('location', ''))
            is_remote = 'remote' in location.lower() or 'remote' in job_title

            job_data_for_filtering.append({
                'job_uid': job.get('job_uid', ''),
                'platform': job.get('platform', ''),
                'location': location,
                'is_remote': is_remote,
                'matched_keywords': matched_keywords,
                'matched_job_titles': matched_job_titles
            })

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{job_name} Results - {today}</title>
        <meta name="description" content="Professional advanced job search results from SERVE denormalized data with quality scoring and deduplication">
        {css}
    </head>
    <body>
        <div class="app-container">
            <header class="header">
                <div class="header-content">
                    <h1>📊 {job_name} Results</h1>
                    <div class="header-subtitle">
                        Using processed data • Generated on {today}
                    </div>
                    <div class="header-stats">
                        <div class="header-stat">
                            <div class="header-stat-label">Total Results</div>
                            <div class="header-stat-value">{stats['total_results']}</div>
                        </div>
                        <div class="header-stat">
                            <div class="header-stat-label">Platforms</div>
                            <div class="header-stat-value">{len(stats['query_platforms'])}</div>
                        </div>
                        <div class="header-stat">
                            <div class="header-stat-label">Date Range</div>
                            <div class="header-stat-value">{days_back} Days</div>
                        </div>
                        <div class="header-stat">
                            <div class="header-stat-label">Latest Data</div>
                            <div class="header-stat-value">{latest_serve_display}</div>
                        </div>
                    </div>
                </div>
            </header>

                        <div class="main-content">
                <aside class="filter-panel">
                    {f'''<div class="filter-section">
                        <div class="filter-title">🔍 Keywords</div>
                        <div class="filter-tags">
                            {' '.join([f'<span class="filter-tag filter-option" data-type="kw" data-value="{kw}"><span class="tag-text">{kw}</span></span>' for kw in keywords])}
                        </div>
                    </div>''' if keywords else ''}

                    {f'''<div class="filter-section">
                        <div class="filter-title">💼 Job Titles</div>
                        <div class="filter-tags">
                            {' '.join([f'<span class="filter-tag filter-option" data-type="jt" data-value="{title}"><span class="tag-text">{title}</span></span>' for title in job_titles])}
                        </div>
                    </div>''' if job_titles else ''}

                    {f'''<div class="filter-section">
                        <div class="filter-title">📍 Locations</div>
                        <div class="filter-tags">
                            {' '.join([f'<span class="filter-tag filter-option" data-type="location" data-value="{loc}"><span class="tag-text">{loc}</span></span>' for loc in display_locations])}
                        </div>
                    </div>''' if display_locations else ''}

                    <div class="filter-section">
                        <div class="filter-title">🏢 Platforms</div>
                        <div class="filter-tags">
                            {' '.join([f'<span class="filter-tag filter-option" data-type="platform" data-value="{platform}"><span class="tag-text">{platform.title()}</span></span>' for platform in stats['query_platforms']])}
                        </div>
                    </div>

                    <div class="filter-section">
                        <div class="filter-title">🌐 Language</div>
                        <div class="filter-content">{language_filter.title()}</div>
                    </div>

                    <div class="filter-section">
                        <div class="filter-title">⭐ Quality Score</div>
                        <div class="filter-content">Minimum: {int(min_quality_score * 100)}%</div>
                    </div>

                    <div class="filter-section">
                        <div class="filter-title">📅 Search Period</div>
                        <div class="filter-content">
                            <strong>From:</strong> {date_from_display}<br>
                            <strong>To:</strong> {date_to_display}<br>
                            <small style="opacity: 0.7;">Latest SERVE data: {latest_serve_display}</small>
                        </div>
                    </div>

                    <div class="filter-section">
                        <button class="clear-filters-btn" onclick="clearAllFilters()">Clear All Filters</button>
                    </div>
                </aside>

                <main class="job-results">
                    <div class="results-header">
                        <h2 class="results-title">Job Listings</h2>
                        <div class="results-count" id="results-count">
                            Showing {stats['filtered_results']} of {stats['total_results']} jobs
                        </div>
                    </div>


    """

    # Job cards container that will be updated by JavaScript
    html += '<div class="job-grid" id="job-grid">'

    if results.empty:
        html += """
                    <div class="no-results-message">
                        <h2>🔍 No Results Found</h2>
                        <p>No job postings were found matching your search criteria.</p>
                        <p>Try adjusting your filters or expanding your search parameters.</p>
                    </div>
        """
    else:
        # Add each job card with enhanced design - these will be managed by JavaScript
        for _, job in results.iterrows():
            # Format date
            if isinstance(job.get('posting_date'), pd.Timestamp):
                posting_date = job['posting_date'].strftime("%B %d, %Y")
            else:
                posting_date = str(job.get('posting_date', 'Unknown date'))

            # Format location and look for remote
            location = job.get('location', 'Unknown location')
            is_remote = False
            if isinstance(location, str) and re.search(r'remote|virtual|work from home|wfh', location.lower()):
                is_remote = True

            # Format job description
            job_description = job.get('job_description', '')
            job_description = sanitize_html_description(job_description)

            # Format platform name for display
            platform_name = job.get('platform', '').capitalize()

            # Score formatting
            relevance_score = job.get('relevance_score', 0)
            relevance_pct = int(relevance_score * 100)

            quality_score = job.get('data_quality_score', 0)
            quality_pct = int(quality_score * 100) if quality_score else 0

            # Company and title
            job_title = job.get('job_title', 'Unknown title')
            company_name = job.get('company_name', '')

            # NEW: Process enriched data for this job (ENHANCEMENT-032)
            enriched_data = process_enriched_data(job, config)
            enriched_insights_html = generate_enriched_insights_section_html(enriched_data, config)

            html += f"""
                    <div class="job-card" data-job-uid="{job.get('job_uid', '')}" data-platform="{job.get('platform', '')}" data-company="{company_name}" data-location="{location}" data-quality="{quality_pct}" data-relevance="{relevance_pct}" data-remote="{str(is_remote).lower()}">
                        <div class="job-card-header">
                            <div class="job-badges">
                                <span class="job-badge badge-relevance">{relevance_pct}% Match</span>
                                <span class="job-badge badge-quality">{quality_pct}% Quality</span>
                            </div>

                            <h3 class="job-title">{job_title}</h3>
                            {f'<div class="job-company">{company_name}</div>' if company_name else ''}

                            <div class="job-meta-right">
                                <div class="job-meta-item" style="gap:0.3rem;">
                                    <svg class="job-meta-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2ZM12 20C7.59 20 4 16.41 4 12C4 7.59 7.59 4 12 4C16.41 4 20 7.59 20 12C20 16.41 16.41 20 12 20Z" fill="currentColor"/>
                                        <path d="M12.5 7H11V13L16.2 16.2L17 14.9L12.5 12.2V7Z" fill="currentColor"/>
                                    </svg>
                                    {posting_date}
                                    &bull;
                                    <svg class="job-meta-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M20 6H16V4C16 2.89 15.11 2 14 2H10C8.89 2 8 2.89 8 4V6H4C2.89 6 2 6.89 2 8V19C2 20.11 2.89 21 4 21H20C21.11 21 22 20.11 22 19V8C22 6.89 21.11 6 20 6ZM10 4H14V6H10V4ZM20 19H4V8H20V19Z" fill="currentColor"/>
                                    </svg>
                                    {platform_name}
                                </div>
                            </div>

                            <div class="job-tags">
            """

            # Add various tags
            if is_remote:
                html += '<span class="job-tag tag-remote">🏠 Remote</span>'

            if job.get('department'):
                html += f'<span class="job-tag tag-department">🏢 {job.get("department")}</span>'

            if job.get('detected_language'):
                html += f'<span class="job-tag tag-language">🌐 {job.get("detected_language", "").title()}</span>'

            html += f"""
                             </div>
                         </div>

                         {enriched_insights_html}

                         <div class="job-actions">
                             <a href="{job.get('job_url', '#')}" target="_blank" class="job-link">
                                 <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                     <path d="M14 2H6C4.9 2 4 2.9 4 4V20C4 21.1 4.89 22 5.99 22H18C19.1 22 20 21.1 20 20V8L14 2ZM18 20H6V4H13V9H18V20Z" fill="currentColor"/>
                                 </svg>
                                 Apply on Company Website
                             </a>
                         </div>

                         <div class="job-description">
                             {job_description}
                         </div>
                     </div>
             """

    # Close job-grid container (outside the job loop)
    html += '</div>'  # Close job-grid

    # Prepare JSON data for filtering
    import json

    html += f"""
                </main>
            </div>
        </div>

                <!-- URL Parameter-Based Filtering System -->
        <script>
        // Job filtering data with pre-computed matches
        const jobFilterData = {json.dumps(job_data_for_filtering)};

        // Parse URL parameters for current filter state
        function getActiveFilters() {{
            const params = new URLSearchParams(window.location.search);
            return {{
                keywords: params.getAll('kw'),
                jobTitles: params.getAll('jt'),
                platforms: params.getAll('platform'),
                locations: params.getAll('location')
            }};
        }}

        // Update URL with new filter parameters
        function updateURL(filters) {{
            const params = new URLSearchParams();

            filters.keywords.forEach(kw => params.append('kw', kw));
            filters.jobTitles.forEach(jt => params.append('jt', jt));
            filters.platforms.forEach(p => params.append('platform', p));
            filters.locations.forEach(l => params.append('location', l));

            const newURL = window.location.pathname + (params.toString() ? '?' + params.toString() : '');
            history.pushState(null, '', newURL);

            applyFilters();
        }}

        // Toggle filter option (add/remove from URL)
        function toggleFilter(element) {{
            const type = element.getAttribute('data-type');
            const value = element.getAttribute('data-value');
            const filters = getActiveFilters();

            let targetArray;
            switch(type) {{
                case 'kw': targetArray = filters.keywords; break;
                case 'jt': targetArray = filters.jobTitles; break;
                case 'platform': targetArray = filters.platforms; break;
                case 'location': targetArray = filters.locations; break;
                default: return;
            }}

            const index = targetArray.indexOf(value);
            if (index > -1) {{
                targetArray.splice(index, 1); // Remove filter
            }} else {{
                targetArray.push(value); // Add filter
            }}

            updateURL(filters);
        }}

        // Apply filters based on URL parameters
        function applyFilters() {{
            const filters = getActiveFilters();
            const hasAnyFilters = filters.keywords.length > 0 || filters.jobTitles.length > 0 ||
                                filters.platforms.length > 0 || filters.locations.length > 0;

            // Show/hide job cards based on filters
            const jobCards = document.querySelectorAll('.job-card');
            let visibleCount = 0;

            jobCards.forEach(card => {{
                const jobUid = card.getAttribute('data-job-uid');
                const jobData = jobFilterData.find(job => job.job_uid === jobUid);

                if (!jobData) {{
                    card.style.display = 'none';
                    return;
                }}

                let shouldShow = true;

                // If no filters are active, show all jobs
                if (!hasAnyFilters) {{
                    shouldShow = true;
                }} else {{
                    // Apply keyword filters
                    if (filters.keywords.length > 0) {{
                        const hasMatchingKeyword = filters.keywords.some(kw =>
                            jobData.matched_keywords.includes(kw)
                        );
                        if (!hasMatchingKeyword) shouldShow = false;
                    }}

                    // Apply job title filters
                    if (filters.jobTitles.length > 0 && shouldShow) {{
                        const hasMatchingTitle = filters.jobTitles.some(jt =>
                            jobData.matched_job_titles.includes(jt)
                        );
                        if (!hasMatchingTitle) shouldShow = false;
                    }}

                    // Apply platform filters
                    if (filters.platforms.length > 0 && shouldShow) {{
                        if (!filters.platforms.includes(jobData.platform)) shouldShow = false;
                    }}

                    // Apply location filters
                    if (filters.locations.length > 0 && shouldShow) {{
                        const hasMatchingLocation = filters.locations.some(loc => {{
                            if (loc === 'Remote') return jobData.is_remote;
                            if (loc === 'No Location') return !jobData.location || jobData.location.trim() === '';
                            if (loc === 'Various Locations') return /\d+\s+locations/i.test(jobData.location);

                            // Handle state abbreviations (2-3 chars, all caps) with word boundaries
                            if (loc.length <= 3 && loc === loc.toUpperCase() && /^[A-Z]+$/.test(loc)) {{
                                const regex = new RegExp('\\\\b' + loc + '\\\\b', 'i');
                                return regex.test(jobData.location);
                            }}

                            // Regular location: substring matching
                            return jobData.location.toLowerCase().includes(loc.toLowerCase());
                        }});
                        if (!hasMatchingLocation) shouldShow = false;
                    }}
                }}

                card.style.display = shouldShow ? 'block' : 'none';
                if (shouldShow) visibleCount++;
            }});

            // Update results count
            const totalCount = jobCards.length;
            const resultCount = document.getElementById('results-count');
            if (resultCount) {{
                resultCount.textContent = `Showing ${{visibleCount}} of ${{totalCount}} jobs`;
            }}

            updateFilterUI();
        }}

        // Clear all filters
        function clearAllFilters() {{
            const newURL = window.location.pathname;
            history.pushState(null, '', newURL);
            applyFilters();
        }}

        // Update filter UI to show active/inactive states
        function updateFilterUI() {{
            const filters = getActiveFilters();

            document.querySelectorAll('.filter-option').forEach(option => {{
                const type = option.getAttribute('data-type');
                const value = option.getAttribute('data-value');

                let isActive = false;
                switch(type) {{
                    case 'kw': isActive = filters.keywords.includes(value); break;
                    case 'jt': isActive = filters.jobTitles.includes(value); break;
                    case 'platform': isActive = filters.platforms.includes(value); break;
                    case 'location': isActive = filters.locations.includes(value); break;
                }}

                option.classList.toggle('active', isActive);
                option.classList.toggle('inactive', !isActive);
            }});
        }}

        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {{
            // Add click handlers to filter options
            document.querySelectorAll('.filter-option').forEach(option => {{
                option.addEventListener('click', () => toggleFilter(option));
            }});

            // Apply initial filters from URL
            applyFilters();

            // Handle browser back/forward
            window.addEventListener('popstate', () => {{
                applyFilters();
            }});
        }});
        </script>
    </body>
    </html>
    """

    return html