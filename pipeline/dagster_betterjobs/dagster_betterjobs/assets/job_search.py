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

class JobSearchConfig(Config):
    """Configuration parameters for job search using enhanced STAGE data."""
    # Search parameters
    keywords: List[str] = []  # List of keywords to search for
    job_titles: List[str] = []  # Specific job titles to search for
    excluded_keywords: List[str] = []  # Keywords to exclude
    locations: List[str] = []  # Locations to include
    remote: bool = None  # Include remote jobs (None = don't filter)

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

@asset(
    group_name="job_search",
    kinds={"snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=["stage_jobs_unified", "stage_jobs_llm_enriched_unified"]  # Changed from multiple RAW discovery assets to single STAGE asset
)
def search_jobs(context: AssetExecutionContext, config: JobSearchConfig) -> pd.DataFrame:
    """
    Search for jobs using cleaned, enriched, and deduplicated data from the STAGE layer.

    This enhanced version provides:
    - Unified schema across all platforms
    - Cleaned job titles and descriptions
    - Deduplicated results (no cross-platform duplicates)
    - Quality scores for filtering and ranking
    - Language detection for filtering
    - Standardized location data
    - Enhanced search capabilities

    ENHANCEMENT-005: Job Search Layer Migration - Stage Data Integration
    """
    # Initialize Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
    stage_schema = os.getenv("SNOWFLAKE_STAGE_SCHEMA", "STAGE")

    # Initialize basic stats (will be completed after date variables are set)
    stats = {
        "query_platforms": list(config.platforms),
        "total_results": 0,
        "filtered_results": 0,
        "execution_time_ms": 0
    }

    # Log basic search parameters (detailed params logged after date calculation)
    context.log.info(f"🔍 Searching STAGE data with platforms: {config.platforms}, days_back: {config.days_back}")

    cursor = conn.cursor()
    start_time = datetime.now()

    try:
        # Get the latest stage data materialization date for accurate date range reporting
        cursor.execute(f"""
        SELECT MAX(transformation_timestamp)::DATE as latest_stage_date
        FROM {database_name}.{stage_schema}.jobs_unified
        """)

        latest_stage_result = cursor.fetchone()
        latest_stage_date = latest_stage_result[0] if latest_stage_result and latest_stage_result[0] else datetime.now().date()

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
            "keywords": config.keywords,
            "job_titles": config.job_titles,
            "locations": config.locations,
            "remote": config.remote,
            "language_filter": config.language_filter,
            "min_quality_score": config.min_quality_score,
            "date_range": f"{config.days_back} days" if not config.date_from else f"{config.date_from} to {config.date_to or 'now'}",
            "actual_date_range": {
                "date_from": date_from,
                "date_to": date_to,
                "latest_stage_date": latest_stage_date.strftime("%Y-%m-%d") if isinstance(latest_stage_date, date) else str(latest_stage_date),
                "days_back_requested": config.days_back
            }
        }

        # Log complete search parameters
        context.log.info(f"📊 Search parameters: {stats['search_params']['date_range']}, platforms: {config.platforms}")

        # Build the unified search query using STAGE.jobs_unified with LLM enriched data (ENHANCEMENT-032)
        query = f"""
        SELECT
            j.job_uid,
            j.job_id,
            j.platform,
            j.company_id,
            j.company_name_clean as company_name,
            j.job_title_clean as job_title,
            {"j.job_description_clean as job_description," if config.include_descriptions else ""}
            j.location_standardized as location,
            j.job_url,
            j.date_posted as posting_date,
            j.date_retrieved,
            j.is_active,
            j.employment_status,
            j.department,
            j.detected_language,
            j.language_confidence,
            j.is_english,
            j.data_quality_score,
            {"j.raw_data," if config.include_raw_data else ""}
            j.transformation_timestamp,

            -- NEW: LLM enriched fields with confidence filtering (ENHANCEMENT-032)
            CASE
                WHEN llm.LLM_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN llm.SALARY_MIN
                ELSE NULL
            END as enriched_salary_min,
            CASE
                WHEN llm.LLM_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN llm.SALARY_MAX
                ELSE NULL
            END as enriched_salary_max,
            CASE
                WHEN llm.LLM_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN llm.SALARY_CURRENCY
                ELSE NULL
            END as enriched_salary_currency,
            CASE
                WHEN llm.LLM_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN llm.SALARY_PERIOD
                ELSE NULL
            END as enriched_salary_period,
            CASE
                WHEN llm.LLM_OVERALL_CONFIDENCE >= {config.min_salary_confidence} THEN llm.SALARY_TYPE
                ELSE NULL
            END as enriched_salary_type,

            CASE
                WHEN llm.EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN llm.MIN_YEARS_EXPERIENCE
                ELSE NULL
            END as enriched_min_years_experience,
            CASE
                WHEN llm.EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN llm.MAX_YEARS_EXPERIENCE
                ELSE NULL
            END as enriched_max_years_experience,
            CASE
                WHEN llm.EXPERIENCE_CONFIDENCE >= {config.min_experience_confidence} THEN llm.EXPERIENCE_LEVEL
                ELSE NULL
            END as enriched_experience_level,

            CASE
                WHEN llm.SKILLS_CONFIDENCE >= {config.min_skills_confidence} THEN llm.TECHNICAL_SKILLS
                ELSE NULL
            END as enriched_technical_skills,

            CASE
                WHEN llm.SKILLS_CONFIDENCE >= {config.min_skills_confidence} THEN llm.SOFT_SKILLS
                ELSE NULL
            END as enriched_soft_skills,

            CASE
                WHEN llm.WORK_ARRANGEMENT_CONFIDENCE >= {config.min_work_arrangement_confidence} THEN llm.WORK_TYPE
                ELSE NULL
            END as enriched_work_type,
            CASE
                WHEN llm.WORK_ARRANGEMENT_CONFIDENCE >= {config.min_work_arrangement_confidence} THEN llm.OFFICE_LOCATIONS
                ELSE NULL
            END as enriched_office_locations,

            CASE
                WHEN llm.CLASSIFICATION_CONFIDENCE >= {config.min_classification_confidence} THEN llm.PRIMARY_KEYWORDS
                ELSE NULL
            END as enriched_primary_keywords,
            CASE
                WHEN llm.CLASSIFICATION_CONFIDENCE >= {config.min_classification_confidence} THEN llm.INDUSTRY_KEYWORDS
                ELSE NULL
            END as enriched_industry_keywords,
            CASE
                WHEN llm.CLASSIFICATION_CONFIDENCE >= {config.min_classification_confidence} THEN llm.ROLE_TYPE
                ELSE NULL
            END as enriched_role_type,
            CASE
                WHEN llm.CLASSIFICATION_CONFIDENCE >= {config.min_classification_confidence} THEN llm.TEAM_SIZE
                ELSE NULL
            END as enriched_team_size,

            -- Confidence indicators for display decisions
            llm.LLM_OVERALL_CONFIDENCE as enriched_overall_confidence,
            llm.SALARY_CONFIDENCE as enriched_salary_confidence,
            llm.EXPERIENCE_CONFIDENCE as enriched_experience_confidence,
            llm.SKILLS_CONFIDENCE as enriched_skills_confidence,
            llm.WORK_ARRANGEMENT_CONFIDENCE as enriched_work_arrangement_confidence,
            llm.CLASSIFICATION_CONFIDENCE as enriched_classification_confidence,

            -- Processing metadata
            llm.LLM_PROCESSED as has_llm_enrichment,
            llm.LLM_PROCESSING_TIMESTAMP as enriched_processing_date

        FROM {database_name}.{stage_schema}.jobs_unified j
        LEFT JOIN {database_name}.{stage_schema}.jobs_llm_enriched llm
            ON j.job_uid = llm.job_uid
        WHERE j.is_active = TRUE
        """

        # Add date filters
        if date_from:
            query += f" AND date_posted >= '{date_from}'"
        if date_to:
            query += f" AND date_posted <= '{date_to}'"

        # Add platform filters
        if config.platforms and "all" not in config.platforms:
            platform_list = "', '".join(config.platforms)
            query += f" AND platform IN ('{platform_list}')"

        # Add language filters using enhanced STAGE data
        if config.language_filter == "english":
            query += f" AND is_english = TRUE"
            if config.language_confidence_min > 0:
                query += f" AND language_confidence >= {config.language_confidence_min}"
        elif config.language_filter != "all":
            query += f" AND detected_language = '{config.language_filter}'"
            if config.language_confidence_min > 0:
                query += f" AND language_confidence >= {config.language_confidence_min}"

        # Add quality score filter
        if config.min_quality_score > 0:
            query += f" AND data_quality_score >= {config.min_quality_score}"

                # Add keyword filters with word boundary checking
        if config.keywords:
            keyword_conditions = []
            for keyword in config.keywords:
                kw_lower = keyword.lower()

                # Handle single words vs multi-word phrases differently
                if ' ' in kw_lower:
                    # Multi-word phrase: use simple substring matching
                    keyword_conditions.append(f"LOWER(job_title_clean) LIKE '%{kw_lower}%'")
                    if config.include_descriptions:
                        keyword_conditions.append(f"LOWER(job_description_clean) LIKE '%{kw_lower}%'")
                else:
                    # Single word: use word boundary checking
                    title_condition = f"(LOWER(job_title_clean) LIKE '% {kw_lower} %' OR LOWER(job_title_clean) LIKE '{kw_lower} %' OR LOWER(job_title_clean) LIKE '% {kw_lower}' OR LOWER(job_title_clean) = '{kw_lower}')"
                    keyword_conditions.append(title_condition)

                    if config.include_descriptions:
                        desc_condition = f"(LOWER(job_description_clean) LIKE '% {kw_lower} %' OR LOWER(job_description_clean) LIKE '{kw_lower} %' OR LOWER(job_description_clean) LIKE '% {kw_lower}' OR LOWER(job_description_clean) = '{kw_lower}')"
                        keyword_conditions.append(desc_condition)

            # Combine with OR (any keyword match)
            query += f" AND ({' OR '.join(keyword_conditions)})"
            context.log.info(f"Applied keyword conditions: {len(keyword_conditions)} total")

        # Add job title filters
        if config.job_titles:
            title_conditions = []
            for title in config.job_titles:
                title_conditions.append(f"LOWER(job_title_clean) LIKE LOWER('%{title}%')")

            query += f" AND ({' OR '.join(title_conditions)})"
            context.log.info(f"Applied job title conditions: {len(title_conditions)} total")

        # Add location filters using standardized location data
        location_or_remote_conditions = []

        if config.locations:
            for location in config.locations:
                # Use standardized location field for better matching
                safe_location = location.replace("'", "''")
                if location == "":
                    # Empty location means "No Location"
                    location_or_remote_conditions.append("(location_standardized IS NULL OR TRIM(location_standardized) = '')")
                elif location == "Location":
                    # "Location" config value means "Various Locations" (e.g., "2 Locations", "3 Locations")
                    location_or_remote_conditions.append("REGEXP_LIKE(location_standardized, '[0-9]+\\\\s+[Ll]ocations')")
                else:
                    # Handle state abbreviations (2-3 chars) with word boundaries to avoid false positives
                    if len(safe_location) <= 3 and safe_location.isupper():
                        # State abbreviation: use word boundary with space
                        loc_condition = f"(LOWER(location_standardized) LIKE LOWER('% {safe_location} %') OR LOWER(location_standardized) LIKE LOWER('{safe_location} %') OR LOWER(location_standardized) LIKE LOWER('% {safe_location}') OR LOWER(location_standardized) = LOWER('{safe_location}'))"
                        location_or_remote_conditions.append(loc_condition)
                    else:
                        # Regular location: use substring matching
                        location_or_remote_conditions.append(f"LOWER(location_standardized) LIKE LOWER('%{safe_location}%')")

        # Add remote filter
        if config.remote is not None and config.remote:
            # Include jobs that mention remote in title or location
            location_or_remote_conditions.extend([
                "LOWER(location_standardized) LIKE LOWER('%remote%')",
                "LOWER(job_title_clean) LIKE LOWER('%remote%')"
            ])
            context.log.info("Adding remote filter: Include remote jobs")
        elif config.remote is not None and not config.remote:
            # Exclude jobs that mention remote in title or location
            query += f" AND NOT (LOWER(location_standardized) LIKE LOWER('%remote%') OR LOWER(job_title_clean) LIKE LOWER('%remote%'))"
            context.log.info("Adding remote filter: Exclude remote jobs")

        # Apply location/remote conditions
        if location_or_remote_conditions:
            query += f" AND ({' OR '.join(location_or_remote_conditions)})"
            context.log.info(f"Applied location/remote conditions: {len(location_or_remote_conditions)} total")

        # Add exclusion filters
        if config.excluded_keywords:
            exclusion_conditions = []
            for keyword in config.excluded_keywords:
                exclusion_title = f"LOWER(job_title_clean) LIKE LOWER('%{keyword}%')"
                if config.include_descriptions:
                    exclusion_desc = f"LOWER(job_description_clean) LIKE LOWER('%{keyword}%')"
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
        context.log.info("📊 Executing unified stage data query...")
        context.log.debug(f"Query: {query}")

        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0].lower() for desc in cursor.description]

        # Convert to DataFrame
        combined_results = pd.DataFrame(results, columns=column_names)

        context.log.info(f"✅ Found {len(combined_results)} results from unified stage data")

        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        stats["execution_time_ms"] = int(execution_time)

        # Calculate relevance scores if keywords are provided
        if config.keywords and not combined_results.empty:
            combined_results["relevance_score"] = combined_results.apply(
                lambda row: calculate_relevance_score(row, config.keywords, config.job_titles, config.locations),
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

        # Enhanced metadata with stage data benefits
        metadata = {
            "total_results": MetadataValue.int(stats["total_results"]),
            "filtered_results": MetadataValue.int(stats["filtered_results"]),
            "platforms_searched": MetadataValue.json(stats["query_platforms"]),
            "execution_time_ms": MetadataValue.int(stats["execution_time_ms"]),
            "data_source": MetadataValue.text("STAGE.jobs_unified (cleaned & deduplicated)"),
            "language_filter": MetadataValue.text(config.language_filter),
            "min_quality_score": MetadataValue.float(config.min_quality_score),
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
        context.log.error(f"Error executing job search: {str(e)}")
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
def process_enriched_data(job_row: pd.Series, config: JobSearchConfig) -> Dict[str, Any]:
    """
    Process enriched LLM data for display with confidence-based filtering.

    Args:
        job_row: Pandas Series containing job data with enriched fields
        config: JobSearchConfig with confidence thresholds

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
            skills_data = json.loads(job_row['enriched_technical_skills']) if isinstance(job_row['enriched_technical_skills'], str) else job_row['enriched_technical_skills']
            if skills_data and len(skills_data) > 0:
                # Limit skills to max display and ensure they're strings
                skills_list = [str(skill) for skill in skills_data[:config.max_skills_display]]
                enriched['technical_skills'] = {
                    'skills': skills_list,
                    'confidence': float(job_row.get('enriched_skills_confidence', 0)) if pd.notna(job_row.get('enriched_skills_confidence')) else 0.0
                }
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    # Soft skills (confidence-based filtering)
    if (job_row.get('enriched_soft_skills') and
        pd.notna(job_row.get('enriched_skills_confidence')) and
        job_row.get('enriched_skills_confidence', 0) >= config.min_skills_confidence):
        try:
            soft_skills_data = json.loads(job_row['enriched_soft_skills']) if isinstance(job_row['enriched_soft_skills'], str) else job_row['enriched_soft_skills']
            if soft_skills_data and len(soft_skills_data) > 0:
                skills_list = [str(skill) for skill in soft_skills_data[:config.max_skills_display]]
                enriched['soft_skills'] = {
                    'skills': skills_list,
                    'confidence': float(job_row.get('enriched_skills_confidence', 0)) if pd.notna(job_row.get('enriched_skills_confidence')) else 0.0
                }
        except (json.JSONDecodeError, TypeError, AttributeError):
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

    # Overall metadata
    enriched['has_enrichment'] = bool(job_row.get('has_llm_enrichment', False))
    enriched['overall_confidence'] = float(job_row.get('enriched_overall_confidence', 0)) if pd.notna(job_row.get('enriched_overall_confidence')) else 0.0
    enriched['processing_date'] = job_row.get('enriched_processing_date')

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

def generate_salary_section_html(salary_data: Dict, config: JobSearchConfig) -> str:
    """Generate inline text for salary information."""
    if not salary_data:
        return ""

    salary_display = format_salary_display(salary_data)

    return f"💰 <strong>Salary Range:</strong> {salary_display}"

def generate_experience_section_html(exp_data: Dict, config: JobSearchConfig) -> str:
    """Generate inline text for experience requirements."""
    if not exp_data:
        return ""

    experience_display = format_experience_display(exp_data)

    return f"🎯 <strong>Experience Required:</strong> {experience_display}"

def generate_skills_section_html(skills_data: Dict, config: JobSearchConfig) -> str:
    """Generate inline text for technical skills with badges."""
    if not skills_data or not skills_data.get('skills'):
        return ""

    skills = skills_data['skills']

    # Create skill badges
    skill_badges = ''.join([f'<span class="skill-badge">{html.escape(str(skill))}</span>' for skill in skills[:config.max_skills_display]])
    if len(skills) > config.max_skills_display:
        skill_badges += f'<span class="more-badge">+{len(skills) - config.max_skills_display} more</span>'

    return f"🛠️ <strong>Technical Skills:</strong> {skill_badges}"

def generate_soft_skills_section_html(soft_skills_data: Dict, config: JobSearchConfig) -> str:
    """Generate inline text for soft skills with badges."""
    if not soft_skills_data or not soft_skills_data.get('skills'):
        return ""

    skills = soft_skills_data['skills']

    skill_badges = ''.join([f'<span class="skill-badge">{html.escape(str(skill))}</span>' for skill in skills[:config.max_skills_display]])
    if len(skills) > config.max_skills_display:
        skill_badges += f'<span class="more-badge">+{len(skills) - config.max_skills_display} more</span>'

    return f"🤝 <strong>Soft Skills:</strong> {skill_badges}"

def generate_work_arrangement_section_html(work_data: Dict, config: JobSearchConfig) -> str:
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

def generate_classification_section_html(classification_data: Dict, config: JobSearchConfig) -> str:
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

def generate_keywords_section_html(classification_data: Dict, config: JobSearchConfig) -> str:
    """Generate keywords section for display on separate line."""
    if not classification_data or not classification_data.get('keywords'):
        return ""

    keywords = classification_data['keywords']
    keyword_badges = ''.join([f'<span class="keyword-badge">{html.escape(str(kw))}</span>' for kw in keywords[:config.max_keywords_display]])
    if len(keywords) > config.max_keywords_display:
        keyword_badges += f'<span class="more-badge">+{len(keywords) - config.max_keywords_display} more</span>'

    return f"🏷️ <strong>Keywords:</strong> {keyword_badges}"

def generate_enriched_insights_section_html(enriched_data: Dict, config: JobSearchConfig) -> str:
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

def generate_enhanced_html_report(results: pd.DataFrame, stats: Dict, config: JobSearchConfig) -> str:
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
            margin-bottom: 1rem;
        }

        .job-meta {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 0.75rem;
            margin-bottom: 1rem;
        }

        .job-meta-item {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.875rem;
            color: var(--text-secondary);
        }

        .job-meta-icon {
            width: 16px;
            height: 16px;
            opacity: 0.7;
        }

        .job-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 1rem;
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
    latest_stage_display = datetime.strptime(actual_range['latest_stage_date'], "%Y-%m-%d").strftime("%B %d, %Y")
    days_back = actual_range['days_back_requested']

    # Extract filter data for display
    keywords = stats['search_params']['keywords']
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
        <meta name="description" content="Professional job search results from enhanced STAGE data with quality scoring and deduplication">
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
                            <div class="header-stat-value">{latest_stage_display}</div>
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
                            <small style="opacity: 0.7;">Latest stage data: {latest_stage_display}</small>
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

                            <div class="job-meta">
                                <div class="job-meta-item">
                                    <svg class="job-meta-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2ZM12 20C7.59 20 4 16.41 4 12C4 7.59 7.59 4 12 4C16.41 4 20 7.59 20 12C20 16.41 16.41 20 12 20Z" fill="currentColor"/>
                                        <path d="M12.5 7H11V13L16.2 16.2L17 14.9L12.5 12.2V7Z" fill="currentColor"/>
                                    </svg>
                                    {posting_date}
                                </div>
                                <div class="job-meta-item">
                                    <svg class="job-meta-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M12 2C8.13 2 5 5.13 5 9C5 14.25 12 22 12 22C12 22 19 14.25 19 9C19 5.13 15.87 2 12 2ZM12 11.5C10.62 11.5 9.5 10.38 9.5 9C9.5 7.62 10.62 6.5 12 6.5C13.38 6.5 14.5 7.62 14.5 9C14.5 10.38 13.38 11.5 12 11.5Z" fill="currentColor"/>
                                    </svg>
                                    {location}
                                </div>
                                <div class="job-meta-item">
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

            if job.get('job_uid'):
                html += f'<span class="job-tag tag-uid">🔗 ID: {job.get("job_uid", "")[:8]}...</span>'

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

# Create Dagster Definitions object for deployment
defs = Definitions(
    assets=[search_jobs]
)