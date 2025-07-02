"""
Analytics Layer Dimension Assets

This module contains Dagster assets for creating and managing dimension tables
in the ANALYTICS layer following the Schema-as-Code approach.
"""

from typing import Dict, Any
from dagster import AssetExecutionContext, asset, MetadataValue
from dagster_snowflake import SnowflakeResource

from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["stage_company_profiles", "stage_jobs_unified"],
    description="Create company dimension with Type 2 SCD for company changes",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_company(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build company dimension from STAGE sources with SCD Type 2 processing.

    This asset integrates company data from JOBS_UNIFIED and COMPANY_PROFILES,
    applying Slowly Changing Dimension Type 2 logic to track historical changes
    to company attributes over time.

    Processing Logic:
    1. Integrate data from both STAGE sources
    2. Detect changes by comparing with existing dimension records
    3. Expire changed records (set is_current=FALSE, expiration_date=TODAY)
    4. Insert new/changed records with new surrogate keys
    5. Handle missing company profiles gracefully

    SCD Logic:
    - NEW companies: Create new records with is_current=TRUE
    - CHANGED companies: Expire old record, create new record
    - UNCHANGED companies: No action needed

    Returns:
        Dict containing execution results and change statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_company.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting SCD Type 2 processing for company dimension")

            # Step 1: Create staging table with current company state
            context.log.info("Creating current company state from STAGE sources")

            staging_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE company_current_state AS
            SELECT
                ju.COMPANY_ID,
                ju.COMPANY_NAME_CLEAN as company_name,
                COALESCE(cp.COMPANY_NAME_STANDARDIZED, ju.COMPANY_NAME_CLEAN) as company_name_standardized,
                COALESCE(cp.COMPANY_INDUSTRY_STANDARDIZED, 'Unknown') as industry,
                COALESCE(cp.EMPLOYEE_COUNT_RANGE, 'Unknown') as employee_count_range,
                COALESCE(cp.COMPANY_SIZE_CATEGORY, 'Unknown') as company_size_category,
                COALESCE(cp.HEADQUARTERS_LOCATION, 'Unknown') as headquarters_location,
                COALESCE(cp.FUNDING_STAGE, 'Unknown') as funding_stage,
                CURRENT_DATE as snapshot_date,
                COUNT(DISTINCT ju.JOB_UID) as active_job_count
            FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
            LEFT JOIN BETTERJOBS_DB.STAGE.COMPANY_PROFILES cp ON ju.COMPANY_ID = cp.COMPANY_ID
            WHERE ju.COMPANY_ID IS NOT NULL
              AND ju.COMPANY_ID != ''
              AND ju.IS_ACTIVE = TRUE
            GROUP BY ju.COMPANY_ID, ju.COMPANY_NAME_CLEAN, cp.COMPANY_NAME_STANDARDIZED,
                     cp.COMPANY_INDUSTRY_STANDARDIZED, cp.EMPLOYEE_COUNT_RANGE,
                     cp.COMPANY_SIZE_CATEGORY, cp.HEADQUARTERS_LOCATION, cp.FUNDING_STAGE
            """

            cursor.execute(staging_sql)

            # Step 2: Detect changes by comparing with existing dimension
            context.log.info("Detecting changes in company attributes")

            changes_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE changes_detected AS
            SELECT
                c.*,
                d.company_key as existing_key,
                d.version_number as current_version,
                d.effective_date as current_effective_date,
                CASE
                    WHEN d.company_key IS NULL THEN 'NEW'
                    WHEN (COALESCE(d.company_name, '') != COALESCE(c.company_name, '') OR
                          COALESCE(d.industry, '') != COALESCE(c.industry, '') OR
                          COALESCE(d.company_size_category, '') != COALESCE(c.company_size_category, '') OR
                          COALESCE(d.headquarters_location, '') != COALESCE(c.headquarters_location, '') OR
                          COALESCE(d.funding_stage, '') != COALESCE(c.funding_stage, '')) THEN 'CHANGED'
                    ELSE 'UNCHANGED'
                END as change_type
            FROM company_current_state c
            LEFT JOIN {table_name} d ON c.COMPANY_ID = d.company_id AND d.is_current = TRUE
            """

            cursor.execute(changes_sql)

            # Step 3: Get change statistics
            cursor.execute("""
                SELECT
                    change_type,
                    COUNT(*) as count
                FROM changes_detected
                GROUP BY change_type
            """)

            change_stats = {row[0]: row[1] for row in cursor.fetchall()}
            context.log.info(f"Change detection results: {change_stats}")

            # Step 4: Expire changed records
            if change_stats.get('CHANGED', 0) > 0:
                context.log.info(f"Expiring {change_stats['CHANGED']} changed company records")

                expire_sql = f"""
                UPDATE {table_name}
                SET
                    is_current = FALSE,
                    expiration_date = CURRENT_DATE,
                    updated_timestamp = CURRENT_TIMESTAMP
                WHERE company_id IN (
                    SELECT COMPANY_ID
                    FROM changes_detected
                    WHERE change_type = 'CHANGED'
                ) AND is_current = TRUE
                """

                cursor.execute(expire_sql)

            # Step 5: Insert new and changed records
            new_and_changed = change_stats.get('NEW', 0) + change_stats.get('CHANGED', 0)

            if new_and_changed > 0:
                context.log.info(f"Inserting {new_and_changed} new/changed company records")

                insert_sql = f"""
                INSERT INTO {table_name} (
                    company_key,
                    company_id,
                    company_name,
                    company_name_standardized,
                    industry,
                    employee_count_range,
                    company_size_category,
                    headquarters_location,
                    funding_stage,
                    effective_date,
                    expiration_date,
                    is_current,
                    version_number,
                    created_timestamp,
                    updated_timestamp,
                    source_stage_table
                )
                SELECT
                    'COMP_' || COMPANY_ID || '_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') || '_' ||
                    ROW_NUMBER() OVER (PARTITION BY COMPANY_ID ORDER BY snapshot_date) as company_key,
                    COMPANY_ID as company_id,
                    company_name,
                    company_name_standardized,
                    industry,
                    employee_count_range,
                    company_size_category,
                    headquarters_location,
                    funding_stage,
                    CURRENT_DATE as effective_date,
                    '9999-12-31'::DATE as expiration_date,
                    TRUE as is_current,
                    COALESCE(current_version, 0) + 1 as version_number,
                    CURRENT_TIMESTAMP as created_timestamp,
                    CURRENT_TIMESTAMP as updated_timestamp,
                    'STAGE.JOBS_UNIFIED,STAGE.COMPANY_PROFILES' as source_stage_table
                FROM changes_detected
                WHERE change_type IN ('NEW', 'CHANGED')
                """

                cursor.execute(insert_sql)

            # Step 6: Validate dimension integrity
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_companies,
                    COUNT(CASE WHEN is_current = TRUE THEN 1 END) as current_companies,
                    COUNT(DISTINCT company_id) as unique_company_ids,
                    MIN(effective_date) as min_effective_date,
                    MAX(effective_date) as max_effective_date
                FROM {table_name}
            """)

            validation_result = cursor.fetchone()

            context.log.info(f"Company dimension validation: {validation_result[0]} total records, "
                           f"{validation_result[1]} current companies, {validation_result[2]} unique companies")

            # Clean up temporary tables
            cursor.execute("DROP TABLE IF EXISTS company_current_state")
            cursor.execute("DROP TABLE IF EXISTS changes_detected")

            return {
                "status": "success",
                "table_name": table_name,
                "change_statistics": change_stats,
                "total_records": validation_result[0],
                "current_companies": validation_result[1],
                "unique_companies": validation_result[2],
                "date_range": {
                    "min_effective": str(validation_result[3]),
                    "max_effective": str(validation_result[4])
                }
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_jobs_unified"],
    description="Create date dimension table for analytics time-based analysis",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_date(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create comprehensive date dimension spanning 10 years for analytics.

    This asset uses the Schema-as-Code pattern to ensure the dim_date table exists,
    then populates it with a complete date range including business calendar attributes.

    Features:
    - Business day indicators
    - Week/month/quarter/year hierarchy
    - Weekend and holiday flags for business intelligence
    - 10-year date range (2020-2030) for historical and forecast analysis

    Returns:
        Dict containing execution results and row counts
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_date.sql", snowflake, context)

    # Clear existing data for fresh population
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Clearing existing date dimension data")
            cursor.execute(f"DELETE FROM {table_name}")

            # Generate date dimension data with comprehensive business calendar
            context.log.info("Populating date dimension with 10-year range (2020-2030)")

            populate_sql = f"""
            INSERT INTO {table_name} (
                date_key,
                full_date,
                day_name,
                day_of_week,
                week_beginning_date,
                week_ending_date,
                month_number,
                month_name,
                quarter_number,
                year_number,
                is_business_day,
                is_weekend,
                created_timestamp
            )
            WITH date_spine AS (
                SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY 1) - 1, '2020-01-01'::DATE) AS calendar_date
                FROM TABLE(GENERATOR(ROWCOUNT => 3653)) -- 10 years + leap days
            )
            SELECT
                TO_CHAR(calendar_date, 'YYYYMMDD') AS date_key,
                calendar_date AS full_date,
                DAYNAME(calendar_date) AS day_name,
                DAYOFWEEK(calendar_date) AS day_of_week,
                DATE_TRUNC('week', calendar_date) AS week_beginning_date,
                DATEADD(day, 6, DATE_TRUNC('week', calendar_date)) AS week_ending_date,
                MONTH(calendar_date) AS month_number,
                MONTHNAME(calendar_date) AS month_name,
                QUARTER(calendar_date) AS quarter_number,
                YEAR(calendar_date) AS year_number,
                CASE
                    WHEN DAYOFWEEK(calendar_date) IN (1,7) THEN FALSE
                    ELSE TRUE
                END AS is_business_day,
                CASE
                    WHEN DAYOFWEEK(calendar_date) IN (1,7) THEN TRUE
                    ELSE FALSE
                END AS is_weekend,
                CURRENT_TIMESTAMP AS created_timestamp
            FROM date_spine
            WHERE calendar_date <= '2030-12-31'::DATE
            ORDER BY calendar_date
            """

            cursor.execute(populate_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully populated date dimension with {rows_inserted} date records")

            # Validate data quality
            cursor.execute(f"SELECT COUNT(*) as total_dates, MIN(full_date) as min_date, MAX(full_date) as max_date FROM {table_name}")
            validation_result = cursor.fetchone()

            context.log.info(f"Date dimension validation: {validation_result[0]} total dates from {validation_result[1]} to {validation_result[2]}")

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "date_range": {
                    "start": str(validation_result[1]),
                    "end": str(validation_result[2]),
                    "total_dates": validation_result[0]
                }
            }
        finally:
            cursor.close()


@asset(
    deps=["stage_locations_normalized"],
    description="Create location dimension with geographic hierarchy and intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_location(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build location dimension from STAGE.LOCATIONS_NORMALIZED with geographic hierarchy.

    This asset creates a location dimension from the normalized location data,
    applying data quality filters and generating surrogate keys for analytics queries.

    Processing:
    - Generate surrogate keys for each location
    - Map STAGE fields to ANALYTICS dimension structure
    - Apply data quality filters for location completeness
    - Preserve geographic hierarchy and intelligence fields
    - Handle international locations and remote work designations

    Data Quality Rules:
    - Filter locations with CONFIDENCE_SCORE >= 0.5
    - Exclude manual review locations unless admin approved
    - Ensure required geographic fields are present
    - Handle special location types (Remote, Hybrid, Global)

    Returns:
        Dict containing execution results and quality metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_location.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting location dimension build from STAGE.LOCATIONS_NORMALIZED")

            # Step 1: Clear existing data for fresh population
            context.log.info("Clearing existing location dimension data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build location dimension with data quality filters
            context.log.info("Building location dimension with quality filters")

            build_sql = f"""
            INSERT INTO {table_name} (
                location_key,
                location_id,
                location_name,
                city,
                state_province,
                country,
                region,
                metro_area,
                location_type,
                is_remote_friendly,
                is_major_tech_hub,
                cost_of_living_index,
                average_salary_adjustment,
                stage_confidence_score,
                frequency_count,
                created_timestamp
            )
            SELECT
                'LOC_' || LOCATION_ID as location_key,
                LOCATION_ID as location_id,
                COALESCE(LOCATION_NAME_CLEAN, LOCATION_NAME) as location_name,
                CITY,
                STATE_PROVINCE as state_province,
                COUNTRY,
                REGION,
                METRO_AREA as metro_area,
                COALESCE(LOCATION_TYPE, 'Unknown') as location_type,
                COALESCE(IS_REMOTE_FRIENDLY, FALSE) as is_remote_friendly,
                COALESCE(IS_MAJOR_TECH_HUB, FALSE) as is_major_tech_hub,
                COST_OF_LIVING_INDEX as cost_of_living_index,
                AVERAGE_SALARY_ADJUSTMENT as average_salary_adjustment,
                COALESCE(CONFIDENCE_SCORE, 0.0) as stage_confidence_score,
                COALESCE(FREQUENCY_COUNT, 0) as frequency_count,
                CURRENT_TIMESTAMP as created_timestamp
            FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
            WHERE CONFIDENCE_SCORE >= 0.5
              AND (MANUAL_REVIEW_FLAG = FALSE OR APPROVED_BY_ADMIN = TRUE)
              AND CITY IS NOT NULL
              AND COUNTRY IS NOT NULL
              AND LOCATION_ID IS NOT NULL
              AND LOCATION_ID != ''
            ORDER BY COUNTRY, STATE_PROVINCE, CITY
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} location records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating location dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_locations,
                COUNT(DISTINCT country) as unique_countries,
                COUNT(DISTINCT state_province) as unique_states_provinces,
                COUNT(DISTINCT city) as unique_cities,
                COUNT(CASE WHEN location_type = 'Remote' THEN 1 END) as remote_locations,
                COUNT(CASE WHEN is_major_tech_hub = TRUE THEN 1 END) as tech_hubs,
                COUNT(CASE WHEN is_remote_friendly = TRUE THEN 1 END) as remote_friendly_locations,
                AVG(stage_confidence_score) as avg_confidence_score,
                MIN(stage_confidence_score) as min_confidence_score,
                MAX(stage_confidence_score) as max_confidence_score,
                SUM(frequency_count) as total_frequency_count
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Geographic distribution analysis
            cursor.execute(f"""
            SELECT
                country,
                COUNT(*) as location_count,
                COUNT(CASE WHEN is_major_tech_hub = TRUE THEN 1 END) as tech_hubs_count,
                AVG(stage_confidence_score) as avg_confidence
            FROM {table_name}
            GROUP BY country
            ORDER BY location_count DESC
            LIMIT 10
            """)

            top_countries = cursor.fetchall()
            country_stats = {row[0]: {"locations": row[1], "tech_hubs": row[2], "avg_confidence": float(row[3])}
                           for row in top_countries}

            # Step 5: Location type distribution
            cursor.execute(f"""
            SELECT
                location_type,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM {table_name}
            GROUP BY location_type
            ORDER BY count DESC
            """)

            location_type_stats = {row[0]: {"count": row[1], "percentage": float(row[2])}
                                 for row in cursor.fetchall()}

            context.log.info(f"Location dimension validation: {validation_result[0]} total locations, "
                           f"{validation_result[1]} countries, {validation_result[2]} states/provinces, "
                           f"{validation_result[3]} cities, {validation_result[5]} tech hubs")

            context.log.info(f"Average confidence score: {validation_result[7]:.3f}, "
                           f"Remote-friendly locations: {validation_result[6]}")

            # Step 6: Quality threshold validation
            low_confidence_count = 0
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE stage_confidence_score < 0.7")
            low_confidence_count = cursor.fetchone()[0]

            if low_confidence_count > 0:
                context.log.warning(f"Found {low_confidence_count} locations with confidence < 0.7")

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_locations": validation_result[0],
                "geographic_distribution": {
                    "countries": validation_result[1],
                    "states_provinces": validation_result[2],
                    "cities": validation_result[3],
                    "top_countries": country_stats
                },
                "location_intelligence": {
                    "remote_locations": validation_result[4],
                    "tech_hubs": validation_result[5],
                    "remote_friendly": validation_result[6]
                },
                "quality_metrics": {
                    "avg_confidence": float(validation_result[7]),
                    "min_confidence": float(validation_result[8]),
                    "max_confidence": float(validation_result[9]),
                    "low_confidence_count": low_confidence_count,
                    "total_frequency": validation_result[10]
                },
                "location_type_distribution": location_type_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Create job classification dimension with hierarchical role taxonomy from LLM-enriched job data",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_job_family(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build job family dimension from STAGE.JOBS_LLM_ENRICHED with role hierarchy.

    This asset creates a job family dimension from LLM-enriched job data,
    applying data quality filters and generating surrogate keys for analytics queries.

    Processing:
    - Generate surrogate keys for unique job family combinations
    - Map LLM fields directly to dimension structure
    - Apply simple derivations for seniority ordering and management classification
    - Apply data quality filters for job classification completeness
    - Handle various job family and seniority level combinations

    Data Quality Rules:
    - Filter jobs with valid JOB_FAMILY (not null, not 'Unknown')
    - Require minimum LLM_OVERALL_CONFIDENCE >= 0.6
    - Exclude manual review jobs unless high confidence
    - Ensure core job classification fields are present

    Returns:
        Dict containing execution results and quality metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_job_family.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting job family dimension build from STAGE.JOBS_LLM_ENRICHED")

            # Step 1: Clear existing data for fresh population
            context.log.info("Clearing existing job family dimension data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build job family dimension with data quality filters
            context.log.info("Building job family dimension with quality filters")

            build_sql = f"""
            INSERT INTO {table_name} (
                job_family_key,
                job_family,
                job_sub_family,
                seniority_level,
                role_type,
                seniority_order,
                is_management_role,
                created_timestamp
            )
            WITH job_family_prep AS (
                SELECT DISTINCT
                    MD5(CONCAT(
                        COALESCE(JOB_FAMILY, 'Unknown'),
                        '|',
                        COALESCE(JOB_SUB_FAMILY, 'General'),
                        '|',
                        COALESCE(SENIORITY_LEVEL, 'Not Specified'),
                        '|',
                        COALESCE(ROLE_TYPE, 'Not Specified')
                    )) as job_family_key,

                    -- Direct LLM Fields (no transformation)
                    COALESCE(JOB_FAMILY, 'Unknown') as job_family,
                    COALESCE(JOB_SUB_FAMILY, 'General') as job_sub_family,
                    COALESCE(SENIORITY_LEVEL, 'Not Specified') as seniority_level,
                    COALESCE(ROLE_TYPE, 'Not Specified') as role_type,

                    -- Simple Derived Fields Only
                    CASE
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%entry%' OR LOWER(SENIORITY_LEVEL) LIKE '%junior%' THEN 1
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%mid%' OR LOWER(SENIORITY_LEVEL) LIKE '%intermediate%' THEN 2
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%senior%' THEN 3
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%staff%' THEN 4
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%principal%' THEN 5
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%director%' THEN 6
                        WHEN LOWER(SENIORITY_LEVEL) LIKE '%vp%' OR LOWER(SENIORITY_LEVEL) LIKE '%vice%' THEN 7
                        ELSE 0
                    END as seniority_order,

                    -- Simple management role identification
                    (LOWER(ROLE_TYPE) LIKE '%manager%'
                     OR LOWER(ROLE_TYPE) LIKE '%director%'
                     OR LOWER(ROLE_TYPE) LIKE '%lead%'
                     OR LOWER(SENIORITY_LEVEL) LIKE '%manager%'
                     OR LOWER(SENIORITY_LEVEL) LIKE '%director%'
                     OR LOWER(SENIORITY_LEVEL) LIKE '%vp%') as is_management_role,

                    CURRENT_TIMESTAMP as created_timestamp

                FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
                WHERE JOB_FAMILY IS NOT NULL
                  AND JOB_FAMILY != 'Unknown'
                  AND LLM_OVERALL_CONFIDENCE >= 0.6
                  AND (LLM_NEEDS_MANUAL_REVIEW = FALSE OR LLM_OVERALL_CONFIDENCE >= 0.8)
            )

            SELECT
                'JF_' || job_family_key as job_family_key,
                job_family,
                job_sub_family,
                seniority_level,
                role_type,
                seniority_order,
                is_management_role,
                created_timestamp
            FROM job_family_prep
            ORDER BY job_family, seniority_order, job_sub_family
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} job family records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating job family dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_job_families,
                COUNT(DISTINCT job_family) as unique_families,
                COUNT(DISTINCT job_sub_family) as unique_sub_families,
                COUNT(DISTINCT seniority_level) as unique_seniority_levels,
                COUNT(DISTINCT role_type) as unique_role_types,
                COUNT(CASE WHEN is_management_role = TRUE THEN 1 END) as management_roles,
                COUNT(CASE WHEN seniority_order > 0 THEN 1 END) as roles_with_seniority,
                AVG(seniority_order) as avg_seniority_order,
                MIN(seniority_order) as min_seniority_order,
                MAX(seniority_order) as max_seniority_order
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Job family distribution analysis
            cursor.execute(f"""
            SELECT
                job_family,
                COUNT(*) as combination_count,
                COUNT(DISTINCT job_sub_family) as sub_families_count,
                COUNT(CASE WHEN is_management_role = TRUE THEN 1 END) as management_roles_count,
                AVG(seniority_order) as avg_seniority
            FROM {table_name}
            GROUP BY job_family
            ORDER BY combination_count DESC
            LIMIT 10
            """)

            top_families = cursor.fetchall()
            family_stats = {
                row[0]: {
                    "combinations": row[1],
                    "sub_families": row[2],
                    "management_roles": row[3],
                    "avg_seniority": float(row[4]) if row[4] is not None else 0.0
                }
                for row in top_families
            }

            # Step 5: Seniority level distribution
            cursor.execute(f"""
            SELECT
                seniority_level,
                seniority_order,
                COUNT(*) as count,
                COUNT(CASE WHEN is_management_role = TRUE THEN 1 END) as management_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM {table_name}
            GROUP BY seniority_level, seniority_order
            ORDER BY seniority_order, count DESC
            """)

            seniority_stats = {
                row[0]: {
                    "order": row[1],
                    "count": row[2],
                    "management_count": row[3],
                    "percentage": float(row[4]) if row[4] is not None else 0.0
                }
                for row in cursor.fetchall()
            }

            # Step 6: Role type distribution
            cursor.execute(f"""
            SELECT
                role_type,
                COUNT(*) as count,
                COUNT(CASE WHEN is_management_role = TRUE THEN 1 END) as flagged_as_management,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM {table_name}
            GROUP BY role_type
            ORDER BY count DESC
            """)

            role_type_stats = {
                row[0]: {
                    "count": row[1],
                    "flagged_management": row[2],
                    "percentage": float(row[3]) if row[3] is not None else 0.0
                }
                for row in cursor.fetchall()
            }

            context.log.info(f"Job family dimension validation: {validation_result[0]} total combinations, "
                           f"{validation_result[1]} unique families, {validation_result[2]} sub-families, "
                           f"{validation_result[3]} seniority levels, {validation_result[5]} management roles")

            context.log.info(f"Seniority distribution: avg order {validation_result[7]:.2f}, "
                           f"roles with seniority: {validation_result[6]}")

            # Step 7: Quality threshold validation
            unordered_seniority_count = 0
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE seniority_order = 0")
            unordered_seniority_count = cursor.fetchone()[0]

            if unordered_seniority_count > 0:
                context.log.warning(f"Found {unordered_seniority_count} job families with unrecognized seniority levels")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_combinations": MetadataValue.int(validation_result[0]),
                "unique_families": MetadataValue.int(validation_result[1]),
                "unique_sub_families": MetadataValue.int(validation_result[2]),
                "unique_seniority_levels": MetadataValue.int(validation_result[3]),
                "unique_role_types": MetadataValue.int(validation_result[4]),
                "management_roles": MetadataValue.int(validation_result[5]),
                "avg_seniority_order": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "unordered_seniority_count": MetadataValue.int(unordered_seniority_count),
                "top_families": MetadataValue.json(family_stats),
                "seniority_distribution": MetadataValue.json(seniority_stats)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_combinations": validation_result[0],
                "hierarchy_distribution": {
                    "unique_families": validation_result[1],
                    "unique_sub_families": validation_result[2],
                    "unique_seniority_levels": validation_result[3],
                    "unique_role_types": validation_result[4],
                    "top_families": family_stats
                },
                "role_classification": {
                    "management_roles": validation_result[5],
                    "roles_with_seniority": validation_result[6],
                    "role_type_distribution": role_type_stats
                },
                "seniority_analysis": {
                    "avg_seniority_order": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "min_seniority_order": validation_result[8],
                    "max_seniority_order": validation_result[9],
                    "unordered_count": unordered_seniority_count,
                    "seniority_distribution": seniority_stats
                }
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_jobs_unified", "stage_jobs_llm_enriched_unified"],
    description="Create platform dimension with ATS characteristics from job data",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_platform(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build platform dimension from STAGE.JOBS_UNIFIED platform data.

    This asset creates a comprehensive platform dimension that includes:
    - Platform identification and standardized codes
    - Platform characteristics derived from actual job data
    - Data quality metrics and volume classifications
    - Activity status tracking

    Processing Logic:
    1. Calculate platform metrics from job posting data
    2. Generate platform codes and characteristics
    3. Determine activity status and data quality scores
    4. Validate platform dimension completeness

    Returns:
        Dict containing execution results and platform statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_platform.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting platform dimension build from STAGE.JOBS_UNIFIED")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing platform dimension data")
            cursor.execute(f"DELETE FROM {table_name}")

            # Step 2: Build platform dimension with characteristics
            context.log.info("Building platform dimension with calculated characteristics")

            build_sql = f"""
            INSERT INTO {table_name} (
                platform_key,
                platform_name,
                platform_code,
                supports_salary_disclosure,
                data_richness_score,
                job_volume_category,
                is_active,
                created_timestamp
            )
                         WITH platform_metrics AS (
                 SELECT
                     ju.PLATFORM,
                     COUNT(*) as total_jobs,
                     COUNT(CASE WHEN lle.SALARY_MIN IS NOT NULL AND lle.SALARY_MAX IS NOT NULL THEN 1 END) as jobs_with_salary,
                     AVG(COALESCE(ju.DATA_QUALITY_SCORE, 0.0)) as avg_quality_score,
                     MAX(ju.DATE_RETRIEVED) as last_activity_date,
                     COUNT(CASE WHEN ju.IS_ACTIVE = TRUE THEN 1 END) as active_jobs
                 FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                 LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle ON ju.JOB_UID = lle.JOB_UID
                 WHERE ju.PLATFORM IS NOT NULL
                   AND TRIM(ju.PLATFORM) != ''
                 GROUP BY ju.PLATFORM
             )
                         SELECT
                 'PLT_' || UPPER(PLATFORM) as platform_key,
                 PLATFORM as platform_name,
                 LOWER(PLATFORM) as platform_code,

                -- Salary disclosure support (10% threshold)
                CASE
                    WHEN total_jobs > 0 THEN (jobs_with_salary::FLOAT / total_jobs) >= 0.10
                    ELSE FALSE
                END as supports_salary_disclosure,

                -- Data richness score (0-1 scale, bounded)
                LEAST(1.0, GREATEST(0.0, COALESCE(avg_quality_score, 0.0))) as data_richness_score,

                -- Job volume categorization
                CASE
                    WHEN total_jobs >= 1000 THEN 'High'
                    WHEN total_jobs >= 100 THEN 'Medium'
                    ELSE 'Low'
                END as job_volume_category,

                -- Activity status (jobs retrieved in last 30 days)
                CASE
                    WHEN last_activity_date IS NOT NULL
                         AND DATEDIFF('day', last_activity_date, CURRENT_DATE) <= 30
                    THEN TRUE
                    ELSE FALSE
                END as is_active,

                CURRENT_TIMESTAMP as created_timestamp

            FROM platform_metrics
            WHERE total_jobs > 0  -- Only include platforms with actual job data
            ORDER BY total_jobs DESC, PLATFORM
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} platform records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating platform dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_platforms,
                COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_platforms,
                COUNT(CASE WHEN supports_salary_disclosure = TRUE THEN 1 END) as salary_disclosure_platforms,
                COUNT(CASE WHEN job_volume_category = 'High' THEN 1 END) as high_volume_platforms,
                COUNT(CASE WHEN job_volume_category = 'Medium' THEN 1 END) as medium_volume_platforms,
                COUNT(CASE WHEN job_volume_category = 'Low' THEN 1 END) as low_volume_platforms,
                AVG(data_richness_score) as avg_data_richness,
                MIN(data_richness_score) as min_data_richness,
                MAX(data_richness_score) as max_data_richness
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Platform characteristics analysis
            cursor.execute(f"""
            SELECT
                platform_name,
                platform_code,
                supports_salary_disclosure,
                data_richness_score,
                job_volume_category,
                is_active
            FROM {table_name}
            ORDER BY
                CASE job_volume_category
                    WHEN 'High' THEN 1
                    WHEN 'Medium' THEN 2
                    ELSE 3
                END,
                data_richness_score DESC,
                platform_name
            """)

            platform_details = [
                {
                    "platform_name": row[0],
                    "platform_code": row[1],
                    "supports_salary_disclosure": row[2],
                    "data_richness_score": float(row[3]) if row[3] is not None else 0.0,
                    "job_volume_category": row[4],
                    "is_active": row[5]
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Volume category distribution
            cursor.execute(f"""
            SELECT
                job_volume_category,
                COUNT(*) as platform_count,
                AVG(data_richness_score) as avg_quality,
                COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_count,
                COUNT(CASE WHEN supports_salary_disclosure = TRUE THEN 1 END) as salary_disclosure_count
            FROM {table_name}
            GROUP BY job_volume_category
            ORDER BY CASE job_volume_category
                WHEN 'High' THEN 1
                WHEN 'Medium' THEN 2
                ELSE 3
            END
            """)

            volume_stats = {
                row[0]: {
                    "platform_count": row[1],
                    "avg_quality": float(row[2]) if row[2] is not None else 0.0,
                    "active_count": row[3],
                    "salary_disclosure_count": row[4]
                }
                for row in cursor.fetchall()
            }

            # Step 6: Data quality validation
            quality_issues = []

            # Check for platforms with very low data quality
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE data_richness_score < 0.1")
            low_quality_count = cursor.fetchone()[0]
            if low_quality_count > 0:
                quality_issues.append(f"{low_quality_count} platforms with very low data quality (<0.1)")

            # Check for inactive platforms
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE is_active = FALSE")
            inactive_count = cursor.fetchone()[0]
            if inactive_count > 0:
                quality_issues.append(f"{inactive_count} inactive platforms (no jobs in last 30 days)")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            context.log.info(f"Platform dimension validation: {validation_result[0]} total platforms, "
                           f"{validation_result[1]} active, {validation_result[2]} support salary disclosure")

            context.log.info(f"Volume distribution: {validation_result[3]} High, "
                           f"{validation_result[4]} Medium, {validation_result[5]} Low volume platforms")

            context.log.info(f"Data quality: avg {validation_result[6]:.3f}, "
                           f"range {validation_result[7]:.3f} - {validation_result[8]:.3f}")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_platforms": MetadataValue.int(validation_result[0]),
                "active_platforms": MetadataValue.int(validation_result[1]),
                "salary_disclosure_platforms": MetadataValue.int(validation_result[2]),
                "high_volume_platforms": MetadataValue.int(validation_result[3]),
                "medium_volume_platforms": MetadataValue.int(validation_result[4]),
                "low_volume_platforms": MetadataValue.int(validation_result[5]),
                "avg_data_richness": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "platform_details": MetadataValue.json(platform_details[:10]),  # Top 10 platforms
                "volume_category_stats": MetadataValue.json(volume_stats),
                "quality_issues_count": MetadataValue.int(len(quality_issues))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_platforms": validation_result[0],
                "platform_characteristics": {
                    "active_platforms": validation_result[1],
                    "salary_disclosure_platforms": validation_result[2],
                    "volume_distribution": {
                        "high": validation_result[3],
                        "medium": validation_result[4],
                        "low": validation_result[5]
                    }
                },
                "data_quality_metrics": {
                    "avg_data_richness": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "min_data_richness": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "max_data_richness": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "platform_details": platform_details,
                "volume_category_analysis": volume_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_skills_consolidated"],
    description="Create skills dimension with taxonomy hierarchy and market intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_skills(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build skills dimension from STAGE.SKILLS_CONSOLIDATED taxonomy.

    This asset creates a skills dimension table with hierarchical classification
    and market intelligence metrics for skills demand analysis.

    Processing Logic:
    1. Generate surrogate keys for each skill
    2. Map STAGE fields to dimension structure
    3. Apply data quality filters (confidence >= 0.5)
    4. Preserve skill hierarchy and market intelligence
    5. Validate skill categorization and completeness

    Data Quality Rules:
    - Filter skills with CONFIDENCE_SCORE >= 0.5
    - Exclude skills marked for manual review unless approved
    - Ensure required fields (skill_name, skill_category) are not null
    - Handle skill categorization and variant mappings

    Returns:
        Dict containing execution results and skills statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_skills.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting skills dimension build from STAGE.SKILLS_CONSOLIDATED")

            # Step 1: Build skills dimension from STAGE source
            context.log.info("Building skills dimension with quality filters")

            build_sql = f"""
            INSERT OVERWRITE INTO {table_name} (
                skill_key,
                skill_id,
                skill_name,
                skill_category,
                skill_subcategory,
                canonical_form,
                common_aliases,
                original_variants,
                stage_confidence_score,
                frequency_count,
                trend_direction,
                created_timestamp
            )
            WITH skills_prep AS (
                SELECT
                    'SKL_' || CONSOLIDATED_SKILL_ID AS skill_key,
                    CONSOLIDATED_SKILL_ID AS skill_id,
                    CANONICAL_SKILL_NAME AS skill_name,

                    -- Skill hierarchy
                    SKILL_CATEGORY AS skill_category,
                    COALESCE(SKILL_SUBCATEGORY, 'General') AS skill_subcategory,

                    -- Standardization fields
                    CANONICAL_SKILL_NAME AS canonical_form,
                    NULL AS common_aliases,
                    ORIGINAL_SKILL_NAMES AS original_variants,

                    -- Market intelligence
                    CONSOLIDATED_CONFIDENCE_SCORE AS stage_confidence_score,
                    TOTAL_FREQUENCY_COUNT AS frequency_count,
                    TREND_DIRECTION AS trend_direction,

                    CURRENT_TIMESTAMP AS created_timestamp

                FROM BETTERJOBS_DB.STAGE.SKILLS_CONSOLIDATED
                WHERE CONSOLIDATED_CONFIDENCE_SCORE >= 0.5
                  AND (MANUAL_REVIEW_FLAG = FALSE OR APPROVED_BY_ADMIN = TRUE)
                  AND CANONICAL_SKILL_NAME IS NOT NULL
                  AND TRIM(CANONICAL_SKILL_NAME) != ''
                  AND SKILL_CATEGORY IS NOT NULL
                  AND TRIM(SKILL_CATEGORY) != ''
            )
            SELECT * FROM skills_prep
            ORDER BY skill_category, skill_subcategory, frequency_count DESC, skill_name
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} skills records")

            # Step 2: Validate data quality and gather statistics
            context.log.info("Validating skills dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_skills,
                COUNT(DISTINCT skill_category) as unique_categories,
                COUNT(DISTINCT skill_subcategory) as unique_subcategories,
                COUNT(CASE WHEN canonical_form IS NOT NULL THEN 1 END) as skills_with_canonical_form,
                COUNT(CASE WHEN common_aliases IS NOT NULL THEN 1 END) as skills_with_aliases,
                COUNT(CASE WHEN original_variants IS NOT NULL THEN 1 END) as skills_with_variants,
                AVG(stage_confidence_score) as avg_confidence_score,
                MIN(stage_confidence_score) as min_confidence_score,
                MAX(stage_confidence_score) as max_confidence_score,
                SUM(frequency_count) as total_skill_frequency,
                AVG(frequency_count) as avg_frequency_count,
                COUNT(CASE WHEN trend_direction = 'GROWING' THEN 1 END) as growing_skills,
                COUNT(CASE WHEN trend_direction = 'STABLE' THEN 1 END) as stable_skills,
                COUNT(CASE WHEN trend_direction = 'DECLINING' THEN 1 END) as declining_skills
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 3: Skills hierarchy analysis
            context.log.info("Analyzing skills hierarchy distribution")

            cursor.execute(f"""
            SELECT
                skill_category,
                COUNT(*) as skill_count,
                COUNT(DISTINCT skill_subcategory) as subcategory_count,
                AVG(stage_confidence_score) as avg_confidence,
                SUM(frequency_count) as category_frequency,
                COUNT(CASE WHEN trend_direction = 'GROWING' THEN 1 END) as growing_count,
                COUNT(CASE WHEN canonical_form IS NOT NULL THEN 1 END) as canonical_count
            FROM {table_name}
            GROUP BY skill_category
            ORDER BY skill_count DESC
            """)

            category_stats = [
                {
                    "skill_category": row[0],
                    "skill_count": row[1],
                    "subcategory_count": row[2],
                    "avg_confidence": float(row[3]) if row[3] is not None else 0.0,
                    "category_frequency": row[4] if row[4] is not None else 0,
                    "growing_count": row[5],
                    "canonical_count": row[6]
                }
                for row in cursor.fetchall()
            ]

            # Step 4: Top skills by frequency analysis
            cursor.execute(f"""
            SELECT
                skill_name,
                skill_category,
                skill_subcategory,
                frequency_count,
                stage_confidence_score,
                trend_direction,
                CASE WHEN canonical_form IS NOT NULL THEN TRUE ELSE FALSE END as has_canonical_form
            FROM {table_name}
            ORDER BY frequency_count DESC
            LIMIT 20
            """)

            top_skills = [
                {
                    "skill_name": row[0],
                    "skill_category": row[1],
                    "skill_subcategory": row[2],
                    "frequency_count": row[3] if row[3] is not None else 0,
                    "stage_confidence_score": float(row[4]) if row[4] is not None else 0.0,
                    "trend_direction": row[5],
                    "has_canonical_form": row[6]
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Trend analysis
            cursor.execute(f"""
            SELECT
                trend_direction,
                COUNT(*) as skill_count,
                AVG(frequency_count) as avg_frequency,
                AVG(stage_confidence_score) as avg_confidence
            FROM {table_name}
            WHERE trend_direction IS NOT NULL
            GROUP BY trend_direction
            ORDER BY skill_count DESC
            """)

            trend_stats = {
                row[0]: {
                    "skill_count": row[1],
                    "avg_frequency": float(row[2]) if row[2] is not None else 0.0,
                    "avg_confidence": float(row[3]) if row[3] is not None else 0.0
                }
                for row in cursor.fetchall()
            }

            # Step 6: Data quality validation
            quality_issues = []

            # Check for skills with very low confidence
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE stage_confidence_score < 0.6")
            low_confidence_count = cursor.fetchone()[0]
            if low_confidence_count > 0:
                quality_issues.append(f"{low_confidence_count} skills with low confidence (<0.6)")

            # Check for skills without subcategories
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE skill_subcategory = 'General'")
            general_subcategory_count = cursor.fetchone()[0]
            if general_subcategory_count > 0:
                quality_issues.append(f"{general_subcategory_count} skills without specific subcategories")

            # Check for skills without canonical forms
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE canonical_form IS NULL")
            no_canonical_count = cursor.fetchone()[0]
            if no_canonical_count > 0:
                quality_issues.append(f"{no_canonical_count} skills without canonical forms")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 7: Skills completeness check
            cursor.execute(f"""
            SELECT
                COUNT(*) as total_skills,
                COUNT(CASE WHEN skill_category IS NOT NULL THEN 1 END) as skills_with_category,
                COUNT(CASE WHEN skill_subcategory IS NOT NULL AND skill_subcategory != 'General' THEN 1 END) as skills_with_subcategory,
                COUNT(CASE WHEN frequency_count > 0 THEN 1 END) as skills_with_frequency
            FROM {table_name}
            """)

            completeness_result = cursor.fetchone()
            completeness_percentage = (completeness_result[1] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0

            context.log.info(f"Skills dimension validation: {validation_result[0]} total skills, "
                           f"{validation_result[1]} categories, {validation_result[2]} subcategories")

            context.log.info(f"Data quality: avg confidence {validation_result[6]:.3f}, "
                           f"range {validation_result[7]:.3f} - {validation_result[8]:.3f}")

            context.log.info(f"Market intelligence: {validation_result[9]} total frequency, "
                           f"{validation_result[11]} growing, {validation_result[12]} stable, {validation_result[13]} declining")

            context.log.info(f"Skills completeness: {completeness_percentage:.1f}% with proper categorization")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_skills": MetadataValue.int(validation_result[0]),
                "unique_categories": MetadataValue.int(validation_result[1]),
                "unique_subcategories": MetadataValue.int(validation_result[2]),
                "skills_with_canonical_form": MetadataValue.int(validation_result[3]),
                "skills_with_aliases": MetadataValue.int(validation_result[4]),
                "skills_with_variants": MetadataValue.int(validation_result[5]),
                "avg_confidence_score": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "total_skill_frequency": MetadataValue.int(validation_result[9] if validation_result[9] is not None else 0),
                "growing_skills": MetadataValue.int(validation_result[11]),
                "stable_skills": MetadataValue.int(validation_result[12]),
                "declining_skills": MetadataValue.int(validation_result[13]),
                "category_distribution": MetadataValue.json(category_stats[:10]),  # Top 10 categories
                "top_skills_by_frequency": MetadataValue.json(top_skills[:10]),    # Top 10 skills
                "trend_analysis": MetadataValue.json(trend_stats),
                "quality_issues_count": MetadataValue.int(len(quality_issues)),
                "completeness_percentage": MetadataValue.float(completeness_percentage)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_skills": validation_result[0],
                "skills_taxonomy": {
                    "unique_categories": validation_result[1],
                    "unique_subcategories": validation_result[2],
                    "category_distribution": category_stats
                },
                "data_quality_metrics": {
                    "skills_with_canonical_form": validation_result[3],
                    "skills_with_aliases": validation_result[4],
                    "skills_with_variants": validation_result[5],
                    "avg_confidence_score": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "min_confidence_score": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "max_confidence_score": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "completeness_percentage": completeness_percentage,
                    "quality_issues": quality_issues
                },
                "market_intelligence": {
                    "total_skill_frequency": validation_result[9] if validation_result[9] is not None else 0,
                    "avg_frequency_count": float(validation_result[10]) if validation_result[10] is not None else 0.0,
                    "growing_skills": validation_result[11],
                    "stable_skills": validation_result[12],
                    "declining_skills": validation_result[13],
                    "trend_analysis": trend_stats
                },
                "top_skills": top_skills,
                "skills_hierarchy": category_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_experience_normalized"],
    description="Create experience requirements dimension table",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_experience(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build experience dimension from normalized experience requirements.

    This asset creates an experience dimension table from normalized experience
    requirements data, applying data quality filters and generating surrogate keys
    for analytics queries.

    Processing Logic:
    1. Generate surrogate keys using 'EXP_' + EXPERIENCE_ID format
    2. Map STAGE fields directly to dimension structure
    3. Apply data quality filters (confidence >= 0.5)
    4. Validate experience year ranges and seniority ordering
    5. Handle both general and technology-specific experience classifications

    Data Quality Rules:
    - Filter experience levels with CONFIDENCE_SCORE >= 0.5
    - Ensure required fields are not null: experience_name, experience_category
    - Validate year ranges: MIN_YEARS_REQUIRED <= MAX_YEARS_REQUIRED when both present
         - Handle seniority ordering consistency (0-99 scale validation)

    Returns:
        Dict containing execution results and experience level statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_experience.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting experience dimension build from STAGE.EXPERIENCE_NORMALIZED")

            # Step 1: Clear existing data for fresh population
            context.log.info("Clearing existing experience dimension data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build experience dimension with data quality filters
            context.log.info("Building experience dimension with quality filters")

            build_sql = f"""
            INSERT INTO {table_name} (
                experience_key,
                experience_id,
                experience_name,
                experience_category,
                min_years_required,
                max_years_required,
                seniority_order,
                experience_description,
                market_frequency,
                confidence_score,
                created_timestamp
            )
            WITH experience_prep AS (
                SELECT
                    'EXP_' || EXPERIENCE_ID as experience_key,
                    EXPERIENCE_ID as experience_id,
                    EXPERIENCE_NAME as experience_name,
                    EXPERIENCE_CATEGORY as experience_category,
                    MIN_YEARS_REQUIRED as min_years_required,
                    MAX_YEARS_REQUIRED as max_years_required,
                    SENIORITY_ORDER as seniority_order,
                    EXPERIENCE_DESCRIPTION as experience_description,
                    MARKET_FREQUENCY as market_frequency,
                    CONFIDENCE_SCORE as confidence_score,
                    CURRENT_TIMESTAMP as created_timestamp
                FROM BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED
                WHERE CONFIDENCE_SCORE >= 0.5
                  AND EXPERIENCE_NAME IS NOT NULL
                  AND TRIM(EXPERIENCE_NAME) != ''
                  AND EXPERIENCE_CATEGORY IS NOT NULL
                  AND TRIM(EXPERIENCE_CATEGORY) != ''
                  AND (MIN_YEARS_REQUIRED IS NULL OR MAX_YEARS_REQUIRED IS NULL
                       OR MIN_YEARS_REQUIRED <= MAX_YEARS_REQUIRED)
            )
            SELECT * FROM experience_prep
            ORDER BY experience_category, seniority_order, experience_name
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} experience records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating experience dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_experience_levels,
                COUNT(DISTINCT experience_category) as unique_categories,
                COUNT(DISTINCT seniority_order) as unique_seniority_orders,
                COUNT(CASE WHEN min_years_required IS NOT NULL THEN 1 END) as with_min_years,
                COUNT(CASE WHEN max_years_required IS NOT NULL THEN 1 END) as with_max_years,
                COUNT(CASE WHEN min_years_required IS NOT NULL AND max_years_required IS NOT NULL THEN 1 END) as with_year_ranges,
                COUNT(CASE WHEN seniority_order IS NOT NULL AND seniority_order BETWEEN 0 AND 99 THEN 1 END) as valid_seniority_orders,
                AVG(confidence_score) as avg_confidence_score,
                MIN(confidence_score) as min_confidence_score,
                MAX(confidence_score) as max_confidence_score,
                SUM(COALESCE(market_frequency, 0)) as total_market_frequency,
                AVG(COALESCE(market_frequency, 0)) as avg_market_frequency,
                COUNT(CASE WHEN market_frequency > 0 THEN 1 END) as with_market_data
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Experience category distribution analysis
            context.log.info("Analyzing experience category distribution")

            cursor.execute(f"""
            SELECT
                experience_category,
                COUNT(*) as level_count,
                COUNT(CASE WHEN min_years_required IS NOT NULL THEN 1 END) as with_min_years,
                COUNT(CASE WHEN max_years_required IS NOT NULL THEN 1 END) as with_max_years,
                AVG(confidence_score) as avg_confidence,
                SUM(COALESCE(market_frequency, 0)) as category_frequency,
                MIN(COALESCE(seniority_order, 99)) as min_seniority_order,
                MAX(COALESCE(seniority_order, 0)) as max_seniority_order
            FROM {table_name}
            GROUP BY experience_category
            ORDER BY level_count DESC
            """)

            category_stats = [
                {
                    "experience_category": row[0],
                    "level_count": row[1],
                    "with_min_years": row[2],
                    "with_max_years": row[3],
                    "avg_confidence": float(row[4]) if row[4] is not None else 0.0,
                    "category_frequency": row[5] if row[5] is not None else 0,
                    "min_seniority_order": row[6] if row[6] != 99 else None,
                    "max_seniority_order": row[7] if row[7] != 0 else None
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Seniority order analysis
            cursor.execute(f"""
            SELECT
                seniority_order,
                COUNT(*) as count,
                LISTAGG(DISTINCT experience_name, ', ') as experience_names,
                AVG(confidence_score) as avg_confidence,
                SUM(COALESCE(market_frequency, 0)) as order_frequency
            FROM {table_name}
            WHERE seniority_order IS NOT NULL
            GROUP BY seniority_order
            ORDER BY seniority_order
            """)

            seniority_stats = [
                {
                    "seniority_order": row[0],
                    "count": row[1],
                    "experience_names": row[2],
                    "avg_confidence": float(row[3]) if row[3] is not None else 0.0,
                    "order_frequency": row[4] if row[4] is not None else 0
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Experience level details (top by market frequency)
            cursor.execute(f"""
            SELECT
                experience_name,
                experience_category,
                min_years_required,
                max_years_required,
                seniority_order,
                market_frequency,
                confidence_score
            FROM {table_name}
            ORDER BY COALESCE(market_frequency, 0) DESC, confidence_score DESC
            LIMIT 15
            """)

            top_experience_levels = [
                {
                    "experience_name": row[0],
                    "experience_category": row[1],
                    "min_years_required": row[2],
                    "max_years_required": row[3],
                    "seniority_order": row[4],
                    "market_frequency": row[5] if row[5] is not None else 0,
                    "confidence_score": float(row[6]) if row[6] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 7: Data quality validation
            quality_issues = []

            # Check for experience levels with very low confidence
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE confidence_score < 0.6")
            low_confidence_count = cursor.fetchone()[0]
            if low_confidence_count > 0:
                quality_issues.append(f"{low_confidence_count} experience levels with low confidence (<0.6)")

            # Check for invalid year ranges
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE min_years_required IS NOT NULL
                  AND max_years_required IS NOT NULL
                  AND min_years_required > max_years_required
            """)
            invalid_ranges_count = cursor.fetchone()[0]
            if invalid_ranges_count > 0:
                quality_issues.append(f"{invalid_ranges_count} experience levels with invalid year ranges")

            # Check for missing seniority orders (expected for technology-specific experience levels)
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE seniority_order IS NULL AND experience_category NOT IN ('technology_specific')")
            missing_seniority_count = cursor.fetchone()[0]
            if missing_seniority_count > 0:
                quality_issues.append(f"{missing_seniority_count} experience levels without seniority order")

                        # Check for seniority orders outside expected range (0-99)
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE seniority_order IS NOT NULL
                  AND (seniority_order < 0 OR seniority_order > 99)
            """)
            invalid_seniority_count = cursor.fetchone()[0]
            if invalid_seniority_count > 0:
                quality_issues.append(f"{invalid_seniority_count} experience levels with invalid seniority order (not 0-99)")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 8: Completeness check
            cursor.execute(f"""
            SELECT
                COUNT(*) as total_levels,
                COUNT(CASE WHEN experience_category IS NOT NULL THEN 1 END) as with_category,
                COUNT(CASE WHEN seniority_order IS NOT NULL THEN 1 END) as with_seniority,
                COUNT(CASE WHEN market_frequency IS NOT NULL AND market_frequency > 0 THEN 1 END) as with_market_data
            FROM {table_name}
            """)

            completeness_result = cursor.fetchone()
            category_completeness = (completeness_result[1] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0
            seniority_completeness = (completeness_result[2] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0
            market_completeness = (completeness_result[3] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0

            context.log.info(f"Experience dimension validation: {validation_result[0]} total experience levels, "
                           f"{validation_result[1]} categories, {validation_result[2]} seniority orders")

            context.log.info(f"Year ranges: {validation_result[5]} with complete ranges, "
                           f"{validation_result[6]} with valid seniority orders")

            context.log.info(f"Data quality: avg confidence {validation_result[7]:.3f}, "
                           f"range {validation_result[8]:.3f} - {validation_result[9]:.3f}")

            context.log.info(f"Market intelligence: {validation_result[10]} total frequency, "
                           f"{validation_result[12]} levels with market data")

            context.log.info(f"Completeness: {category_completeness:.1f}% categorized, "
                           f"{seniority_completeness:.1f}% with seniority, "
                           f"{market_completeness:.1f}% with market data")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_experience_levels": MetadataValue.int(validation_result[0]),
                "unique_categories": MetadataValue.int(validation_result[1]),
                "unique_seniority_orders": MetadataValue.int(validation_result[2]),
                "with_year_ranges": MetadataValue.int(validation_result[5]),
                "valid_seniority_orders": MetadataValue.int(validation_result[6]),
                "avg_confidence_score": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "total_market_frequency": MetadataValue.int(validation_result[10] if validation_result[10] is not None else 0),
                "with_market_data": MetadataValue.int(validation_result[12]),
                "category_distribution": MetadataValue.json(category_stats),
                "seniority_distribution": MetadataValue.json(seniority_stats[:10]),  # Top 10 seniority levels
                "top_experience_levels": MetadataValue.json(top_experience_levels[:10]),  # Top 10 by frequency
                "quality_issues_count": MetadataValue.int(len(quality_issues)),
                "category_completeness": MetadataValue.float(category_completeness),
                "seniority_completeness": MetadataValue.float(seniority_completeness),
                "market_completeness": MetadataValue.float(market_completeness)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_experience_levels": validation_result[0],
                "experience_classification": {
                    "unique_categories": validation_result[1],
                    "unique_seniority_orders": validation_result[2],
                    "category_distribution": category_stats
                },
                "year_requirements": {
                    "with_min_years": validation_result[3],
                    "with_max_years": validation_result[4],
                    "with_year_ranges": validation_result[5]
                },
                "seniority_analysis": {
                    "valid_seniority_orders": validation_result[6],
                    "seniority_distribution": seniority_stats
                },
                "data_quality_metrics": {
                    "avg_confidence_score": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "min_confidence_score": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "max_confidence_score": float(validation_result[9]) if validation_result[9] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "market_intelligence": {
                    "total_market_frequency": validation_result[10] if validation_result[10] is not None else 0,
                    "avg_market_frequency": float(validation_result[11]) if validation_result[11] is not None else 0.0,
                    "with_market_data": validation_result[12]
                },
                "completeness_metrics": {
                    "category_completeness": category_completeness,
                    "seniority_completeness": seniority_completeness,
                    "market_completeness": market_completeness
                },
                "top_experience_levels": top_experience_levels
            }

        finally:
            cursor.close()


@asset(
    deps=["stage_salary_normalized"],
    description="Create salary dimension with normalized ranges and market intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_salary(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build salary dimension from STAGE.SALARY_NORMALIZED with market intelligence.

    This asset creates a dimension table containing normalized salary ranges with
    quality indicators, market percentiles, and audit trail information. All salary
    values are converted to annual USD for consistent analysis.

    Processing:
    - Generate surrogate keys for each salary range
    - Map normalized salary fields to dimension structure
    - Include quality indicators and market percentile data
    - Handle outlier flags and manual review requirements
    - Optimize for salary-based analytical queries

    Returns:
        Dict containing execution results and salary statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_salary.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("🔄 Starting salary dimension creation...")

            # Clear existing data for fresh population
            context.log.info("🧹 Clearing existing salary dimension data")
            cursor.execute(f"DELETE FROM {table_name}")

            # Insert salary dimension data
            context.log.info("📊 Populating salary dimension from normalized salary data")

            insert_sql = f"""
            INSERT INTO {table_name} (
                SALARY_KEY,
                SALARY_ID,
                SALARY_RANGE_NAME,
                SALARY_MIN_ORIGINAL,
                SALARY_MAX_ORIGINAL,
                SALARY_PERIOD_ORIGINAL,
                SALARY_CURRENCY_ORIGINAL,
                SALARY_MIN_ANNUAL_USD,
                SALARY_MAX_ANNUAL_USD,
                SALARY_MIDPOINT_ANNUAL_USD,
                NORMALIZATION_FACTOR,
                CONFIDENCE_SCORE,
                OUTLIER_FLAG,
                MANUAL_REVIEW_FLAG,
                APPROVED_BY_ADMIN,
                FREQUENCY_COUNT,
                MARKET_PERCENTILE,
                CREATED_TIMESTAMP
            )
            SELECT
                'SAL_' || SALARY_ID as SALARY_KEY,
                SALARY_ID,
                SALARY_RANGE_NAME,
                SALARY_MIN_ORIGINAL,
                SALARY_MAX_ORIGINAL,
                SALARY_PERIOD_ORIGINAL,
                SALARY_CURRENCY_ORIGINAL,
                SALARY_MIN_ANNUAL_USD,
                SALARY_MAX_ANNUAL_USD,
                SALARY_MIDPOINT_ANNUAL_USD,
                NORMALIZATION_FACTOR,
                CONFIDENCE_SCORE,
                OUTLIER_FLAG,
                MANUAL_REVIEW_FLAG,
                APPROVED_BY_ADMIN,
                FREQUENCY_COUNT,
                MARKET_PERCENTILE,
                CURRENT_TIMESTAMP as CREATED_TIMESTAMP
            FROM BETTERJOBS_DB.STAGE.SALARY_NORMALIZED
            WHERE CONFIDENCE_SCORE >= 0.5
              AND SALARY_MIN_ANNUAL_USD > 0
              AND SALARY_MAX_ANNUAL_USD > 0
              AND SALARY_MIN_ANNUAL_USD <= SALARY_MAX_ANNUAL_USD
            ORDER BY SALARY_MIDPOINT_ANNUAL_USD
            """

            cursor.execute(insert_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"✅ Inserted {rows_inserted:,} salary dimension records")

            # Get dimension statistics
            cursor.execute(f"""
            SELECT
                COUNT(*) as total_salaries,
                COUNT(CASE WHEN OUTLIER_FLAG = TRUE THEN 1 END) as outlier_count,
                COUNT(CASE WHEN MANUAL_REVIEW_FLAG = TRUE THEN 1 END) as manual_review_count,
                COUNT(CASE WHEN APPROVED_BY_ADMIN = TRUE THEN 1 END) as admin_approved_count,
                COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as high_confidence_count,
                AVG(CONFIDENCE_SCORE) as avg_confidence,
                MIN(SALARY_MIDPOINT_ANNUAL_USD) as min_salary,
                MAX(SALARY_MIDPOINT_ANNUAL_USD) as max_salary,
                AVG(SALARY_MIDPOINT_ANNUAL_USD) as avg_salary,
                COUNT(DISTINCT SALARY_PERIOD_ORIGINAL) as unique_periods,
                COUNT(DISTINCT SALARY_CURRENCY_ORIGINAL) as unique_currencies,
                SUM(FREQUENCY_COUNT) as total_frequency
            FROM {table_name}
            """)

            stats = cursor.fetchone()

            # Get period distribution
            cursor.execute(f"""
            SELECT
                SALARY_PERIOD_ORIGINAL,
                COUNT(*) as count,
                AVG(SALARY_MIDPOINT_ANNUAL_USD) as avg_salary,
                AVG(CONFIDENCE_SCORE) as avg_confidence
            FROM {table_name}
            GROUP BY SALARY_PERIOD_ORIGINAL
            ORDER BY count DESC
            """)

            period_distribution = cursor.fetchall()

            context.log.info(f"""
            🎯 Salary Dimension Creation Complete:
            • Total Salary Ranges: {stats[0]:,}
            • Outliers: {stats[1]:,}
            • Manual Review Needed: {stats[2]:,}
            • Admin Approved: {stats[3]:,}
            • High Confidence (≥90%): {stats[4]:,}
            • Average Confidence: {stats[5]:.3f}
            • Salary Range: ${stats[6]:,.0f} - ${stats[7]:,.0f}
            • Average Salary: ${stats[8]:,.0f}
            • Unique Periods: {stats[9]}
            • Unique Currencies: {stats[10]}
            • Total Market Frequency: {stats[11]:,}
            """)

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_salaries": MetadataValue.int(stats[0]),
                "outlier_count": MetadataValue.int(stats[1]),
                "manual_review_count": MetadataValue.int(stats[2]),
                "admin_approved_count": MetadataValue.int(stats[3]),
                "high_confidence_count": MetadataValue.int(stats[4]),
                "avg_confidence": MetadataValue.float(stats[5]),
                "salary_range": MetadataValue.text(f"${stats[6]:,.0f} - ${stats[7]:,.0f}"),
                "avg_salary": MetadataValue.int(int(stats[8])),
                "unique_periods": MetadataValue.int(stats[9]),
                "unique_currencies": MetadataValue.int(stats[10]),
                "total_frequency": MetadataValue.int(stats[11])
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "statistics": {
                    "total_salaries": stats[0],
                    "outlier_count": stats[1],
                    "manual_review_count": stats[2],
                    "admin_approved_count": stats[3],
                    "high_confidence_count": stats[4],
                    "avg_confidence": round(stats[5], 3),
                    "min_salary": int(stats[6]),
                    "max_salary": int(stats[7]),
                    "avg_salary": int(stats[8]),
                    "unique_periods": stats[9],
                    "unique_currencies": stats[10],
                    "total_frequency": stats[11]
                },
                "period_distribution": [
                    {
                        "period": row[0],
                        "count": row[1],
                        "avg_salary": int(row[2]) if row[2] else 0,
                        "avg_confidence": round(row[3], 3) if row[3] else 0
                    }
                    for row in period_distribution
                ]
            }

        except Exception as e:
            context.log.error(f"❌ Salary dimension creation failed: {str(e)}")
            raise

        finally:
            cursor.close()


@asset(
    deps=["stage_keywords_normalized"],
    description="Create keywords taxonomy dimension table",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_keywords(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build keywords dimension from normalized keywords taxonomy.

    This asset creates a keywords dimension table with hierarchical classification
    and market intelligence metrics for keyword analysis.

    Processing Logic:
    1. Generate surrogate keys for each keyword
    2. Map STAGE fields to dimension structure
    3. Apply data quality filters (confidence >= 0.5)
    4. Preserve keyword hierarchy and market intelligence
    5. Validate keyword categorization and completeness

    Data Quality Rules:
    - Filter keywords with CONFIDENCE_SCORE >= 0.5
    - Ensure required fields (keyword_text, keyword_type) are not null
    - Handle keyword categorization and variant mappings
    - Include admin approval tracking for quality assurance

    Returns:
        Dict containing execution results and keywords statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_dim_keywords.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting keywords dimension build from STAGE.KEYWORDS_NORMALIZED")

            # Step 1: Clear existing data for fresh population
            context.log.info("Clearing existing keywords dimension data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build keywords dimension with data quality filters
            context.log.info("Building keywords dimension with quality filters")

            build_sql = f"""
            INSERT INTO {table_name} (
                keyword_key,
                keyword_id,
                keyword_text,
                keyword_text_clean,
                keyword_type,
                keyword_category,
                canonical_form,
                original_variants,
                frequency_count,
                trend_score,
                confidence_score,
                approved_by_admin,
                created_timestamp
            )
            WITH keywords_prep AS (
                SELECT
                    'KWD_' || KEYWORD_ID as keyword_key,
                    KEYWORD_ID as keyword_id,
                    KEYWORD_TEXT as keyword_text,
                    KEYWORD_TEXT_CLEAN as keyword_text_clean,

                    -- Keyword hierarchy
                    KEYWORD_TYPE as keyword_type,
                    COALESCE(KEYWORD_CATEGORY, 'General') as keyword_category,

                    -- Standardization fields
                    CANONICAL_FORM as canonical_form,
                    ORIGINAL_VARIANTS as original_variants,

                    -- Market intelligence
                    FREQUENCY_COUNT as frequency_count,
                    TREND_SCORE as trend_score,
                    CONFIDENCE_SCORE as confidence_score,
                    COALESCE(APPROVED_BY_ADMIN, FALSE) as approved_by_admin,

                    CURRENT_TIMESTAMP as created_timestamp

                FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED
                WHERE CONFIDENCE_SCORE >= 0.5
                  AND KEYWORD_TEXT IS NOT NULL
                  AND TRIM(KEYWORD_TEXT) != ''
                  AND KEYWORD_TYPE IS NOT NULL
                  AND TRIM(KEYWORD_TYPE) != ''
            )
            SELECT * FROM keywords_prep
            ORDER BY keyword_type, keyword_category, frequency_count DESC, keyword_text
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} keywords records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating keywords dimension data quality")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_keywords,
                COUNT(DISTINCT keyword_type) as unique_types,
                COUNT(DISTINCT keyword_category) as unique_categories,
                COUNT(CASE WHEN canonical_form IS NOT NULL THEN 1 END) as keywords_with_canonical_form,
                COUNT(CASE WHEN original_variants IS NOT NULL THEN 1 END) as keywords_with_variants,
                COUNT(CASE WHEN approved_by_admin = TRUE THEN 1 END) as admin_approved_keywords,
                AVG(confidence_score) as avg_confidence_score,
                MIN(confidence_score) as min_confidence_score,
                MAX(confidence_score) as max_confidence_score,
                SUM(COALESCE(frequency_count, 0)) as total_keyword_frequency,
                AVG(COALESCE(frequency_count, 0)) as avg_frequency_count,
                COUNT(CASE WHEN trend_score > 0 THEN 1 END) as positive_trend_keywords,
                COUNT(CASE WHEN trend_score < 0 THEN 1 END) as negative_trend_keywords,
                COUNT(CASE WHEN frequency_count > 0 THEN 1 END) as keywords_with_frequency
            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Keywords hierarchy analysis
            context.log.info("Analyzing keywords hierarchy distribution")

            cursor.execute(f"""
            SELECT
                keyword_type,
                COUNT(*) as keyword_count,
                COUNT(DISTINCT keyword_category) as category_count,
                AVG(confidence_score) as avg_confidence,
                SUM(COALESCE(frequency_count, 0)) as type_frequency,
                COUNT(CASE WHEN trend_score > 0 THEN 1 END) as positive_trend_count,
                COUNT(CASE WHEN canonical_form IS NOT NULL THEN 1 END) as canonical_count,
                COUNT(CASE WHEN approved_by_admin = TRUE THEN 1 END) as admin_approved_count
            FROM {table_name}
            GROUP BY keyword_type
            ORDER BY keyword_count DESC
            """)

            type_stats = [
                {
                    "keyword_type": row[0],
                    "keyword_count": row[1],
                    "category_count": row[2],
                    "avg_confidence": float(row[3]) if row[3] is not None else 0.0,
                    "type_frequency": row[4] if row[4] is not None else 0,
                    "positive_trend_count": row[5],
                    "canonical_count": row[6],
                    "admin_approved_count": row[7]
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Top keywords by frequency analysis
            cursor.execute(f"""
            SELECT
                keyword_text,
                keyword_type,
                keyword_category,
                frequency_count,
                trend_score,
                confidence_score,
                approved_by_admin,
                CASE WHEN canonical_form IS NOT NULL THEN TRUE ELSE FALSE END as has_canonical_form
            FROM {table_name}
            ORDER BY COALESCE(frequency_count, 0) DESC
            LIMIT 20
            """)

            top_keywords = [
                {
                    "keyword_text": row[0],
                    "keyword_type": row[1],
                    "keyword_category": row[2],
                    "frequency_count": row[3] if row[3] is not None else 0,
                    "trend_score": float(row[4]) if row[4] is not None else 0.0,
                    "confidence_score": float(row[5]) if row[5] is not None else 0.0,
                    "approved_by_admin": row[6],
                    "has_canonical_form": row[7]
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Trend analysis
            cursor.execute(f"""
            SELECT
                CASE
                    WHEN trend_score > 0.1 THEN 'POSITIVE'
                    WHEN trend_score < -0.1 THEN 'NEGATIVE'
                    ELSE 'STABLE'
                END as trend_category,
                COUNT(*) as keyword_count,
                AVG(COALESCE(frequency_count, 0)) as avg_frequency,
                AVG(confidence_score) as avg_confidence
            FROM {table_name}
            WHERE trend_score IS NOT NULL
            GROUP BY CASE
                WHEN trend_score > 0.1 THEN 'POSITIVE'
                WHEN trend_score < -0.1 THEN 'NEGATIVE'
                ELSE 'STABLE'
            END
            ORDER BY keyword_count DESC
            """)

            trend_stats = {
                row[0]: {
                    "keyword_count": row[1],
                    "avg_frequency": float(row[2]) if row[2] is not None else 0.0,
                    "avg_confidence": float(row[3]) if row[3] is not None else 0.0
                }
                for row in cursor.fetchall()
            }

            # Step 7: Data quality validation
            quality_issues = []

            # Check for keywords with very low confidence
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE confidence_score < 0.5")
            low_confidence_count = cursor.fetchone()[0]
            if low_confidence_count > 0:
                quality_issues.append(f"{low_confidence_count} keywords with low confidence (<0.5)")

            # Check for keywords without categories
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE keyword_category = 'General'")
            general_category_count = cursor.fetchone()[0]
            if general_category_count > 0:
                quality_issues.append(f"{general_category_count} keywords without specific categories")

            # Check for keywords without canonical forms
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE canonical_form IS NULL")
            no_canonical_count = cursor.fetchone()[0]
            if no_canonical_count > 0:
                quality_issues.append(f"{no_canonical_count} keywords without canonical forms")

            # Check for keywords pending manual review
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE approved_by_admin = FALSE")
            pending_review_count = cursor.fetchone()[0]
            if pending_review_count > 0:
                quality_issues.append(f"{pending_review_count} keywords pending admin approval")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 8: Keywords completeness check
            cursor.execute(f"""
            SELECT
                COUNT(*) as total_keywords,
                COUNT(CASE WHEN keyword_type IS NOT NULL THEN 1 END) as keywords_with_type,
                COUNT(CASE WHEN keyword_category IS NOT NULL AND keyword_category != 'General' THEN 1 END) as keywords_with_category,
                COUNT(CASE WHEN frequency_count > 0 THEN 1 END) as keywords_with_frequency
            FROM {table_name}
            """)

            completeness_result = cursor.fetchone()
            type_completeness = (completeness_result[1] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0
            category_completeness = (completeness_result[2] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0
            frequency_completeness = (completeness_result[3] / completeness_result[0] * 100) if completeness_result[0] > 0 else 0

            context.log.info(f"Keywords dimension validation: {validation_result[0]} total keywords, "
                           f"{validation_result[1]} types, {validation_result[2]} categories")

            context.log.info(f"Data quality: avg confidence {validation_result[6]:.3f}, "
                           f"range {validation_result[7]:.3f} - {validation_result[8]:.3f}")

            context.log.info(f"Market intelligence: {validation_result[9]} total frequency, "
                           f"{validation_result[11]} positive trends, {validation_result[12]} negative trends")

            context.log.info(f"Admin approval: {validation_result[5]} approved keywords")

            context.log.info(f"Completeness: {type_completeness:.1f}% with types, "
                           f"{category_completeness:.1f}% with specific categories, "
                           f"{frequency_completeness:.1f}% with frequency data")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_keywords": MetadataValue.int(validation_result[0]),
                "unique_types": MetadataValue.int(validation_result[1]),
                "unique_categories": MetadataValue.int(validation_result[2]),
                "keywords_with_canonical_form": MetadataValue.int(validation_result[3]),
                "keywords_with_variants": MetadataValue.int(validation_result[4]),
                "admin_approved_keywords": MetadataValue.int(validation_result[5]),
                "avg_confidence_score": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "total_keyword_frequency": MetadataValue.int(validation_result[9] if validation_result[9] is not None else 0),
                "positive_trend_keywords": MetadataValue.int(validation_result[11]),
                "negative_trend_keywords": MetadataValue.int(validation_result[12]),
                "type_distribution": MetadataValue.json(type_stats[:10]),  # Top 10 types
                "top_keywords_by_frequency": MetadataValue.json(top_keywords[:10]),    # Top 10 keywords
                "trend_analysis": MetadataValue.json(trend_stats),
                "quality_issues_count": MetadataValue.int(len(quality_issues)),
                "type_completeness": MetadataValue.float(type_completeness),
                "category_completeness": MetadataValue.float(category_completeness),
                "frequency_completeness": MetadataValue.float(frequency_completeness)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_keywords": validation_result[0],
                "keywords_taxonomy": {
                    "unique_types": validation_result[1],
                    "unique_categories": validation_result[2],
                    "type_distribution": type_stats
                },
                "data_quality_metrics": {
                    "keywords_with_canonical_form": validation_result[3],
                    "keywords_with_variants": validation_result[4],
                    "admin_approved_keywords": validation_result[5],
                    "avg_confidence_score": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "min_confidence_score": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "max_confidence_score": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "market_intelligence": {
                    "total_keyword_frequency": validation_result[9] if validation_result[9] is not None else 0,
                    "avg_frequency_count": float(validation_result[10]) if validation_result[10] is not None else 0.0,
                    "positive_trend_keywords": validation_result[11],
                    "negative_trend_keywords": validation_result[12],
                    "keywords_with_frequency": validation_result[13],
                    "trend_analysis": trend_stats
                },
                "completeness_metrics": {
                    "type_completeness": type_completeness,
                    "category_completeness": category_completeness,
                    "frequency_completeness": frequency_completeness
                },
                "top_keywords": top_keywords,
                "keywords_hierarchy": type_stats
            }

        finally:
            cursor.close()