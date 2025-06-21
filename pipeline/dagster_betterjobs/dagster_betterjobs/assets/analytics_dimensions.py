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
    group_name="analytics_dimensions",
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
    group_name="analytics_dimensions",
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
    group_name="analytics_dimensions",
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
    group_name="analytics_dimensions",
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