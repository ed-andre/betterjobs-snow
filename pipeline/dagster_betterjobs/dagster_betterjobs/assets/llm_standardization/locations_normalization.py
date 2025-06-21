"""
Location Standardization Assets for LLM Data

This module contains Dagster assets for standardizing and normalizing location data
from LLM-enriched job postings. It follows the established patterns from Phase 1 (Skills)
and Phase 2 (Keywords) normalization.

Phase 3 Assets:
- stage_llm_locations_raw_extraction: Extract and flatten location data
- stage_location_standardization_rules: Manage location standardization rules
- stage_locations_normalized: Create normalized locations master table
- stage_job_locations_bridge: Create job-location relationships
"""

from typing import Dict, Any, List
import json
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    FreshnessPolicy
)
from dagster_snowflake import SnowflakeResource


class LocationStandardizationConfig(Config):
    """Configuration for location standardization processing"""
    confidence_threshold: float = 0.7
    min_location_frequency: int = 2
    batch_size: int = 1000
    enable_facility_extraction: bool = True


@asset(
    deps=["stage_jobs_llm_enriched_unified", "stage_jobs_unified"],
    description="Extract and flatten locations from multiple data sources",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_locations_raw_extraction(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Extract all locations from VARIANT columns and text fields into workable format.

    Processes:
    - office_locations: VARIANT array from LLM with office location strings
    - location_standardized: Basic location from jobs_unified table
    - headquarters: Company location data for context

    Output: Raw locations with source tracking, deduplication, and frequency analysis
    """

    context.log.info("Starting location raw extraction process")

    # Create the raw extraction view
    create_view_sql = """
    CREATE OR REPLACE VIEW BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION AS
    WITH OFFICE_LOCATIONS_EXPLODED AS (
        -- Extract office locations from VARIANT array
        SELECT
            jle.JOB_UID,
            'office_locations' as LOCATION_SOURCE,
            'office' as LOCATION_TYPE,
            TRIM(LOWER(LOCATION.VALUE::STRING)) as LOCATION_NAME_RAW,
            LOCATION.VALUE::STRING as LOCATION_NAME_ORIGINAL
        FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
        LATERAL FLATTEN(input => jle.OFFICE_LOCATIONS) LOCATION
        WHERE jle.OFFICE_LOCATIONS IS NOT NULL
          AND IS_ARRAY(jle.OFFICE_LOCATIONS)
          AND ARRAY_SIZE(jle.OFFICE_LOCATIONS) > 0
          AND LOCATION.VALUE IS NOT NULL
          AND LENGTH(TRIM(LOCATION.VALUE::STRING)) > 1
          AND LOWER(TRIM(LOCATION.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '', 'unknown')
    ),

    BASE_LOCATIONS AS (
        -- Include basic location from jobs_unified
        SELECT
            ju.JOB_UID,
            'location_standardized' as LOCATION_SOURCE,
            'standard' as LOCATION_TYPE,
            TRIM(LOWER(ju.LOCATION_STANDARDIZED)) as LOCATION_NAME_RAW,
            ju.LOCATION_STANDARDIZED as LOCATION_NAME_ORIGINAL
                FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        WHERE ju.LOCATION_STANDARDIZED IS NOT NULL
          AND LENGTH(TRIM(ju.LOCATION_STANDARDIZED)) > 1
          AND LOWER(TRIM(ju.LOCATION_STANDARDIZED)) NOT IN ('null', 'none', 'n/a', '', 'no location found', 'unknown')
    )

    SELECT * FROM OFFICE_LOCATIONS_EXPLODED
    UNION ALL
    SELECT * FROM BASE_LOCATIONS;
    """

    try:
        with snowflake.get_connection() as conn:
            # Create the view
            cursor = conn.cursor()
            cursor.execute(create_view_sql)
            cursor.close()
            context.log.info("Successfully created LOCATIONS_RAW_EXTRACTION view")

            # Get extraction statistics
            stats_sql = """
            SELECT
                LOCATION_SOURCE,
                LOCATION_TYPE,
                COUNT(*) as RECORD_COUNT,
                COUNT(DISTINCT LOCATION_NAME_RAW) as UNIQUE_LOCATIONS,
                COUNT(DISTINCT JOB_UID) as JOBS_WITH_LOCATIONS
            FROM BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION
            GROUP BY LOCATION_SOURCE, LOCATION_TYPE
            ORDER BY RECORD_COUNT DESC;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            extraction_stats = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Calculate totals
            total_records = sum(int(row['RECORD_COUNT']) for row in extraction_stats)
            total_unique_locations = sum(int(row['UNIQUE_LOCATIONS']) for row in extraction_stats)
            total_jobs_with_locations = sum(int(row['JOBS_WITH_LOCATIONS']) for row in extraction_stats)

            context.log.info(f"Location extraction completed:")
            context.log.info(f"  - Total location records: {total_records}")
            context.log.info(f"  - Unique raw locations: {total_unique_locations}")
            context.log.info(f"  - Jobs with locations: {total_jobs_with_locations}")

            for stat in extraction_stats:
                context.log.info(f"  - {stat['LOCATION_SOURCE']}: {stat['RECORD_COUNT']} records, {stat['UNIQUE_LOCATIONS']} unique")

            return {
                "status": "success",
                "total_records": total_records,
                "unique_locations": total_unique_locations,
                "jobs_with_locations": total_jobs_with_locations,
                "extraction_stats": extraction_stats,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in location raw extraction: {str(e)}")
        raise


@asset(
    description="Maintain countries mapping for international location standardization",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7)  # Weekly updates
)
def stage_countries_mapping(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Create and maintain comprehensive countries mapping for international location parsing.

    Features:
    - All major countries with official and common names
    - ISO country codes for standardization
    - Tech hub classification for market intelligence
    - Common variations and abbreviations
    - Lookup views for efficient parsing logic

    Note: Data is loaded from SQL configuration file insert_countries_mapping.sql
    """

    context.log.info("Checking countries mapping")

    try:
        with snowflake.get_connection() as conn:
            # Check if countries table exists and has data
            check_sql = """
            SELECT COUNT(*) as COUNTRY_COUNT
            FROM BETTERJOBS_DB.STAGE.COUNTRIES_MAPPING;
            """

            cursor = conn.cursor()
            cursor.execute(check_sql)
            country_count = cursor.fetchone()[0]
            cursor.close()

            if country_count == 0:
                context.log.warning("No countries mapping found - data needs to be loaded from SQL file")
                context.log.info("Please run: pipeline/sql/llm_standardization/insert_countries_mapping.sql")

                return {
                    "status": "warning",
                    "country_count": 0,
                    "message": "Countries data needs to be loaded from SQL configuration file"
                }

            # Get country statistics
            stats_sql = """
            SELECT
                COUNT(*) as TOTAL_COUNTRIES,
                COUNT(DISTINCT COUNTRY_NAME_COMMON) as UNIQUE_COMMON_NAMES,
                COUNT(DISTINCT COUNTRY_CODE_ISO2) as UNIQUE_ISO2_CODES,
                COUNT(CASE WHEN IS_MAJOR_TECH_HUB THEN 1 END) as TECH_HUB_COUNTRIES,
                COUNT(DISTINCT REGION) as REGIONS
            FROM BETTERJOBS_DB.STAGE.COUNTRIES_MAPPING;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            # Verify lookup views exist
            views_check_sql = """
            SELECT
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.COUNTRIES_LOOKUP) as COUNTRIES_LOOKUP_COUNT,
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LOCATION_PARSING_LOOKUP) as PARSING_LOOKUP_COUNT;
            """

            cursor = conn.cursor()
            cursor.execute(views_check_sql)
            views_stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            context.log.info(f"Countries mapping verified:")
            context.log.info(f"  - Total country records: {stats['TOTAL_COUNTRIES']}")
            context.log.info(f"  - Unique countries: {stats['UNIQUE_COMMON_NAMES']}")
            context.log.info(f"  - ISO codes: {stats['UNIQUE_ISO2_CODES']}")
            context.log.info(f"  - Tech hub countries: {stats['TECH_HUB_COUNTRIES']}")
            context.log.info(f"  - Geographic regions: {stats['REGIONS']}")
            context.log.info(f"  - Countries lookup view: {views_stats['COUNTRIES_LOOKUP_COUNT']} records")
            context.log.info(f"  - Location parsing lookup: {views_stats['PARSING_LOOKUP_COUNT']} records")

            return {
                "status": "success",
                **stats,
                **views_stats,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in countries mapping check: {str(e)}")
        raise


@asset(
    description="Maintain US states mapping for automatic country inference",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7)  # Weekly updates
)
def stage_us_states_mapping(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Create and maintain US states mapping for automatic country inference.

    Features:
    - All 50 US states with full names and abbreviations
    - US territories and DC
    - Lookup view for efficient country inference during location normalization

    Note: Data is loaded from SQL configuration file insert_us_states_mapping.sql
    """

    context.log.info("Checking US states mapping")

    try:
        with snowflake.get_connection() as conn:
            # Check if states table exists and has data
            check_sql = """
            SELECT COUNT(*) as STATE_COUNT
            FROM BETTERJOBS_DB.STAGE.US_STATES_MAPPING;
            """

            cursor = conn.cursor()
            cursor.execute(check_sql)
            state_count = cursor.fetchone()[0]
            cursor.close()

            if state_count == 0:
                context.log.warning("No US states mapping found - data needs to be loaded from SQL file")
                context.log.info("Please run: pipeline/sql/llm_standardization/insert_us_states_mapping.sql")

                return {
                    "status": "warning",
                    "state_count": 0,
                    "message": "US states data needs to be loaded from SQL configuration file"
                }

            # Get state statistics
            stats_sql = """
            SELECT
                COUNT(*) as TOTAL_STATES,
                COUNT(DISTINCT STATE_NAME_FULL) as UNIQUE_FULL_NAMES,
                COUNT(DISTINCT STATE_ABBREVIATION) as UNIQUE_ABBREVIATIONS,
                COUNT(DISTINCT COUNTRY) as COUNTRIES
            FROM BETTERJOBS_DB.STAGE.US_STATES_MAPPING;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            # Verify lookup view exists
            view_check_sql = """
            SELECT COUNT(*) as LOOKUP_COUNT
            FROM BETTERJOBS_DB.STAGE.US_STATES_LOOKUP;
            """

            cursor = conn.cursor()
            cursor.execute(view_check_sql)
            lookup_count = cursor.fetchone()[0]
            cursor.close()

            context.log.info(f"US states mapping verified:")
            context.log.info(f"  - Total state records: {stats['TOTAL_STATES']}")
            context.log.info(f"  - Unique full names: {stats['UNIQUE_FULL_NAMES']}")
            context.log.info(f"  - Unique abbreviations: {stats['UNIQUE_ABBREVIATIONS']}")
            context.log.info(f"  - Lookup view records: {lookup_count}")

            return {
                "status": "success",
                **stats,
                "lookup_count": lookup_count,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in US states mapping check: {str(e)}")
        raise


@asset(
    description="Maintain location standardization rules and geographic mappings",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7)  # Weekly updates
)
def stage_location_standardization_rules(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Create and maintain comprehensive location standardization rules.

    Features:
    - Geographic standardization (SF → San Francisco, NYC → New York)
    - Remote work detection patterns
    - Tech hub classification
    - Facility type patterns
    - Geographic hierarchy mapping

    Note: Rules are loaded from SQL configuration file insert_location_standardization_rules.sql
    """

    context.log.info("Checking location standardization rules")

    try:
        with snowflake.get_connection() as conn:
            # Check if rules table exists and has data
            check_sql = """
            SELECT COUNT(*) as RULE_COUNT
            FROM BETTERJOBS_DB.STAGE.LOCATION_STANDARDIZATION_RULES;
            """

            cursor = conn.cursor()
            cursor.execute(check_sql)
            rule_count = cursor.fetchone()[0]
            cursor.close()

            if rule_count == 0:
                context.log.warning("No location standardization rules found - rules need to be loaded from SQL file")
                context.log.info("Please run: pipeline/sql/llm_standardization/insert_location_standardization_rules.sql")

                return {
                    "status": "warning",
                    "rule_count": 0,
                    "message": "Rules need to be loaded from SQL configuration file"
                }

            # Get rule statistics
            stats_sql = """
            SELECT
                RULE_TYPE,
                LOCATION_TYPE,
                COUNT(*) as RULE_COUNT,
                AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE
            FROM BETTERJOBS_DB.STAGE.LOCATION_STANDARDIZATION_RULES
            GROUP BY RULE_TYPE, LOCATION_TYPE
            ORDER BY RULE_COUNT DESC;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            rule_stats = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            context.log.info(f"Location standardization rules validated:")
            context.log.info(f"  - Total rules: {rule_count}")

            for stat in rule_stats:
                context.log.info(f"  - {stat['RULE_TYPE']} ({stat['LOCATION_TYPE']}): {stat['RULE_COUNT']} rules, avg confidence: {stat['AVG_CONFIDENCE']:.3f}")

            return {
                "status": "success",
                "rule_count": rule_count,
                "rule_stats": rule_stats,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error validating location standardization rules: {str(e)}")
        raise


@asset(
    deps=["stage_llm_locations_raw_extraction", "stage_location_standardization_rules", "stage_us_states_mapping", "stage_countries_mapping"],
    description="Create normalized locations master table with geographic intelligence",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_locations_normalized(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Apply standardization rules and create locations master table.

    Processing:
    - Apply standardization rules with confidence scoring
    - Deduplicate location variations
    - Extract facility types from location names
    - Calculate frequency and trend metrics
    - Flag low-confidence items for manual review

    Output: Clean locations master table for analytics
    """

    context.log.info("Starting location normalization process")

    # Clear existing data
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED;"

    # Populate normalized locations
    populate_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED (
        LOCATION_ID,
        LOCATION_NAME,
        LOCATION_NAME_CLEAN,
        LOCATION_NAME_ORIGINAL,
        CITY,
        STATE_PROVINCE,
        COUNTRY,
        METRO_AREA,
        REGION,
        LOCATION_TYPE,
        IS_REMOTE_FRIENDLY,
        IS_MAJOR_TECH_HUB,
        ORIGINAL_VARIANTS,
        CONFIDENCE_SCORE,
        FREQUENCY_COUNT,
        FIRST_SEEN_DATE,
        LAST_SEEN_DATE,
        MANUAL_REVIEW_FLAG,
        CREATED_TIMESTAMP,
        UPDATED_TIMESTAMP,
        CREATED_BY
    )
        WITH location_aggregation AS (
        SELECT
            -- Use standardized name if available, otherwise use most common original
            COALESCE(lsr.STANDARDIZED_NAME, lre.LOCATION_NAME_ORIGINAL) as location_name,
            -- Create accent-free clean name for better matching
            LOWER(TRIM(TRANSLATE(
                COALESCE(lsr.STANDARDIZED_NAME, lre.LOCATION_NAME_ORIGINAL),
                'áàäâåãéèëêíìîïóòôöõúùûüýÿñçÁÀÄÂÅÃÉÈËÊÍÌÎÏÓÒÔÖÕÚÙÛÜÝŸÑÇ',
                'aaaaaaeeeeiiiioooouuuuyyncAAAAAEEEEIIIIOOOOUUUUYYNC'
            ))) as location_name_clean,

            -- Enhanced geographic hierarchy parsing with international support
            COALESCE(
                lsr.CITY,
                CASE
                    -- For international locations, first part is city
                    WHEN lpl_country.LOCATION_TYPE = 'COUNTRY' THEN TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 1))
                    -- For US locations, use existing logic
                    ELSE TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 1))
                END
            ) as city,

            COALESCE(
                lsr.STATE_PROVINCE,
                CASE
                    -- For international locations, no state/province (set to null)
                    WHEN lpl_country.LOCATION_TYPE = 'COUNTRY' THEN NULL
                    -- For US locations, second part is state
                    WHEN lpl_state.LOCATION_TYPE = 'US_STATE' THEN TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 2))
                    -- Default fallback
                    ELSE TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 2))
                END
            ) as state_province,

            -- Enhanced country inference: Rules > International parsing > US states > null
            COALESCE(
                lsr.COUNTRY,
                CASE
                    -- International location detected
                    WHEN lpl_country.LOCATION_TYPE = 'COUNTRY' THEN lpl_country.COUNTRY
                    -- US state detected
                    WHEN lpl_state.LOCATION_TYPE = 'US_STATE' THEN lpl_state.COUNTRY
                    ELSE NULL
                END
            ) as country,

            -- Location type and intelligence
            COALESCE(lsr.LOCATION_TYPE,
                CASE
                    WHEN CONTAINS(UPPER(lre.LOCATION_NAME_ORIGINAL), 'REMOTE') THEN 'remote'
                    WHEN CONTAINS(UPPER(lre.LOCATION_NAME_ORIGINAL), 'HYBRID') THEN 'hybrid'
                    ELSE 'office'
                END
            ) as location_type,

            -- Aggregate metrics
            ARRAY_AGG(DISTINCT lre.LOCATION_NAME_ORIGINAL) as original_variants,
            COUNT(*) as frequency_count,
            MIN(ju.DATE_RETRIEVED::DATE) as first_seen_date,
            MAX(ju.DATE_RETRIEVED::DATE) as last_seen_date,
            AVG(COALESCE(lsr.CONFIDENCE_SCORE, 0.7)) as confidence_score

        FROM BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION lre
        LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_STANDARDIZATION_RULES lsr
            ON LOWER(lre.LOCATION_NAME_RAW) = LOWER(lsr.PATTERN)

        -- Check if second part matches a US state
        LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_PARSING_LOOKUP lpl_state
            ON UPPER(TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 2))) = UPPER(lpl_state.LOCATION_NAME)
            AND lpl_state.LOCATION_TYPE = 'US_STATE'

        -- Check if second part matches a country (for international locations)
        LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_PARSING_LOOKUP lpl_country
            ON UPPER(TRIM(SPLIT_PART(lre.LOCATION_NAME_ORIGINAL, ',', 2))) = UPPER(lpl_country.LOCATION_NAME)
            AND lpl_country.LOCATION_TYPE = 'COUNTRY'
            AND lpl_state.LOCATION_NAME IS NULL  -- Only if it's not a US state

        JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju ON lre.JOB_UID = ju.JOB_UID
        WHERE LENGTH(lre.LOCATION_NAME_RAW) >= 2  -- Filter out single characters
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING COUNT(*) >= :min_frequency  -- Only include locations appearing at least N times
    )
    SELECT
        CONCAT('loc_', ROW_NUMBER() OVER (ORDER BY la.frequency_count DESC)) as location_id,
        la.location_name,
        la.location_name_clean,
        la.original_variants[0]::STRING as location_name_original,
        la.city,
        la.state_province,
        la.country,

        -- Metro area detection using lookup table
        COALESCE(mam.METRO_AREA, 'Other') as metro_area,

        -- Region detection using lookup table
        COALESCE(rm.REGION, 'Other') as region,

        la.location_type,

        -- Remote work friendly detection
        CASE
            WHEN la.location_type = 'remote' THEN TRUE
            WHEN la.location_type = 'hybrid' THEN TRUE
            ELSE FALSE
        END as is_remote_friendly,

        -- Tech hub classification using lookup table
        COALESCE(thm.IS_MAJOR_TECH_HUB, FALSE) as is_major_tech_hub,

        la.original_variants,
        la.confidence_score,
        la.frequency_count,
        la.first_seen_date,
        la.last_seen_date,

        -- Flag for manual review
        CASE
            WHEN la.confidence_score < :confidence_threshold THEN TRUE
            WHEN la.frequency_count = 1 THEN TRUE
            ELSE FALSE
        END as manual_review_flag,

        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'system'
    FROM location_aggregation la
    LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_METRO_AREA_MAPPING mam
        ON la.city = mam.CITY
        AND la.state_province = mam.STATE_PROVINCE
        AND la.country = mam.COUNTRY
        AND mam.IS_ACTIVE = TRUE
    LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_REGION_MAPPING rm
        ON la.state_province = rm.STATE_PROVINCE
        AND la.country = rm.COUNTRY
        AND rm.IS_ACTIVE = TRUE
    LEFT JOIN BETTERJOBS_DB.STAGE.LOCATION_TECH_HUB_MAPPING thm
        ON la.city = thm.CITY
        AND la.state_province = thm.STATE_PROVINCE
        AND la.country = thm.COUNTRY
        AND thm.IS_ACTIVE = TRUE;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing data
            context.log.info("Clearing existing location normalization data")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Populate normalized locations
            context.log.info("Populating normalized locations")
            cursor = conn.cursor()
            # Replace parameters in SQL
            final_sql = populate_sql.replace(':min_frequency', '2').replace(':confidence_threshold', '0.7')
            cursor.execute(final_sql)
            cursor.close()

            # Get statistics
            stats_sql = """
            SELECT
                COUNT(*) as TOTAL_LOCATIONS,
                COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as HIGH_CONFIDENCE_LOCATIONS,
                COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as LOW_CONFIDENCE_LOCATIONS,
                COUNT(CASE WHEN MANUAL_REVIEW_FLAG THEN 1 END) as NEEDS_REVIEW,
                COUNT(CASE WHEN IS_MAJOR_TECH_HUB THEN 1 END) as TECH_HUB_LOCATIONS,
                COUNT(CASE WHEN IS_REMOTE_FRIENDLY THEN 1 END) as REMOTE_FRIENDLY_LOCATIONS,
                AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE,
                AVG(FREQUENCY_COUNT) as AVG_FREQUENCY
            FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for key, value in stats.items():
                if hasattr(value, 'to_eng_string'):  # Decimal object
                    stats[key] = float(value)

            context.log.info(f"Location normalization completed:")
            context.log.info(f"  - Total locations: {stats['TOTAL_LOCATIONS']}")
            context.log.info(f"  - High confidence: {stats['HIGH_CONFIDENCE_LOCATIONS']}")
            context.log.info(f"  - Low confidence: {stats['LOW_CONFIDENCE_LOCATIONS']}")
            context.log.info(f"  - Needs review: {stats['NEEDS_REVIEW']}")
            context.log.info(f"  - Tech hubs: {stats['TECH_HUB_LOCATIONS']}")
            context.log.info(f"  - Remote friendly: {stats['REMOTE_FRIENDLY_LOCATIONS']}")
            context.log.info(f"  - Average confidence: {stats['AVG_CONFIDENCE']:.3f}")

            return {
                "status": "success",
                **stats,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in location normalization: {str(e)}")
        raise


@asset(
    deps=["stage_locations_normalized", "stage_jobs_unified"],
    description="Create job-location relationships with context tracking",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_locations_bridge(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Map jobs to normalized locations with rich context.

    Features:
    - Source tracking (office_locations vs location_standardized vs headquarters)
    - Work arrangement classification (on_site, hybrid, remote)
    - Facility type extraction (plant, office, campus, etc.)
    - Confidence scoring for location-job associations
    """

    context.log.info("Starting job-location bridge creation")

    # Clear existing data
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE;"

    # Populate bridge table
    populate_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE (
        BRIDGE_ID,
        JOB_UID,
        LOCATION_ID,
        LOCATION_SOURCE,
        ORIGINAL_TEXT,
        LOCATION_CONTEXT,
        WORK_ARRANGEMENT,
        EXTRACTION_CONFIDENCE,
        STANDARDIZATION_CONFIDENCE,
        OVERALL_CONFIDENCE,
        PROCESSING_METHOD,
        NEEDS_REVIEW,
        CREATED_TIMESTAMP,
        CREATED_BY
    )
    SELECT
        CONCAT('bridge_', lre.JOB_UID, '_', ln.LOCATION_ID, '_', ROW_NUMBER() OVER (ORDER BY lre.JOB_UID)) as bridge_id,
        lre.JOB_UID,
        ln.LOCATION_ID,
        lre.LOCATION_SOURCE,
        lre.LOCATION_NAME_ORIGINAL,

        -- Location context
        CASE
            WHEN lre.LOCATION_SOURCE = 'office_locations' THEN 'primary'
            WHEN lre.LOCATION_SOURCE = 'location_standardized' THEN 'standard'
            WHEN lre.LOCATION_SOURCE = 'headquarters' THEN 'secondary'
            ELSE 'unknown'
        END as location_context,

        -- Work arrangement from job data
        CASE
            WHEN jle.WORK_TYPE = 'On-site' THEN 'on_site'
            WHEN jle.WORK_TYPE = 'Hybrid' THEN 'hybrid'
            WHEN jle.WORK_TYPE = 'Flexible' THEN 'flexible'
            WHEN ln.LOCATION_TYPE = 'remote' THEN 'remote'
            ELSE 'on_site'  -- default
        END as work_arrangement,



        -- Confidence scoring
        0.9 as extraction_confidence,  -- High confidence for LLM extracted data
        ln.CONFIDENCE_SCORE as standardization_confidence,
        (0.9 + ln.CONFIDENCE_SCORE) / 2 as overall_confidence,

        'llm_auto' as processing_method,

        -- Needs review flag
        CASE
            WHEN ln.CONFIDENCE_SCORE < :confidence_threshold THEN TRUE
            WHEN ln.MANUAL_REVIEW_FLAG THEN TRUE
            ELSE FALSE
        END as needs_review,

        CURRENT_TIMESTAMP,
        'system'

    FROM BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION lre
    JOIN BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED ln
        ON (
            -- Primary matching: Accent-insensitive matching on raw location name
            LOWER(TRIM(TRANSLATE(
                lre.LOCATION_NAME_RAW,
                'áàäâåãéèëêíìîïóòôöõúùûüýÿñçÁÀÄÂÅÃÉÈËÊÍÌÎÏÓÒÔÖÕÚÙÛÜÝŸÑÇ',
                'aaaaaaeeeeiiiioooouuuuyyncAAAAAEEEEIIIIOOOOUUUUYYNC'
            ))) = ln.LOCATION_NAME_CLEAN
            -- Or accent-insensitive matching on original location name
            OR LOWER(TRIM(TRANSLATE(
                lre.LOCATION_NAME_ORIGINAL,
                'áàäâåãéèëêíìîïóòôöõúùûüýÿñçÁÀÄÂÅÃÉÈËÊÍÌÎÏÓÒÔÖÕÚÙÛÜÝŸÑÇ',
                'aaaaaaeeeeiiiioooouuuuyyncAAAAAEEEEIIIIOOOOUUUUYYNC'
            ))) = ln.LOCATION_NAME_CLEAN
            -- Fallback: Direct name matching
            OR LOWER(lre.LOCATION_NAME_ORIGINAL) = LOWER(ln.LOCATION_NAME_ORIGINAL)
            OR LOWER(lre.LOCATION_NAME_ORIGINAL) = LOWER(ln.LOCATION_NAME)
        )
    LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle ON lre.JOB_UID = jle.JOB_UID;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing data
            context.log.info("Clearing existing job-location bridge data")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Populate bridge table
            context.log.info("Populating job-location bridge")
            cursor = conn.cursor()
            # Replace parameters in SQL
            final_sql = populate_sql.replace(':confidence_threshold', '0.7')
            cursor.execute(final_sql)
            cursor.close()

            # Get statistics
            stats_sql = """
            SELECT
                COUNT(*) as TOTAL_RELATIONSHIPS,
                COUNT(DISTINCT JOB_UID) as JOBS_WITH_LOCATIONS,
                COUNT(DISTINCT LOCATION_ID) as LOCATIONS_WITH_JOBS,
                COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as HIGH_CONFIDENCE_RELATIONSHIPS,
                COUNT(CASE WHEN NEEDS_REVIEW THEN 1 END) as NEEDS_REVIEW,
                AVG(OVERALL_CONFIDENCE) as AVG_CONFIDENCE
            FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE;
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            bridge_stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            # Get breakdown by source and facility type
            breakdown_sql = """
            SELECT
                LOCATION_SOURCE,
                WORK_ARRANGEMENT,
                COUNT(*) as RELATIONSHIP_COUNT
            FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE
            GROUP BY LOCATION_SOURCE, WORK_ARRANGEMENT
            ORDER BY RELATIONSHIP_COUNT DESC;
            """

            cursor = conn.cursor()
            cursor.execute(breakdown_sql)
            breakdown_stats = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for key, value in bridge_stats.items():
                if hasattr(value, 'to_eng_string'):  # Decimal object
                    bridge_stats[key] = float(value)

            context.log.info(f"Job-location bridge creation completed:")
            context.log.info(f"  - Total relationships: {bridge_stats['TOTAL_RELATIONSHIPS']}")
            context.log.info(f"  - Jobs with locations: {bridge_stats['JOBS_WITH_LOCATIONS']}")
            context.log.info(f"  - Locations with jobs: {bridge_stats['LOCATIONS_WITH_JOBS']}")
            context.log.info(f"  - High confidence: {bridge_stats['HIGH_CONFIDENCE_RELATIONSHIPS']}")
            context.log.info(f"  - Needs review: {bridge_stats['NEEDS_REVIEW']}")
            context.log.info(f"  - Average confidence: {bridge_stats['AVG_CONFIDENCE']:.3f}")

            # Log top breakdown categories
            context.log.info("Top relationship categories:")
            for breakdown in breakdown_stats[:5]:
                context.log.info(f"  - {breakdown['LOCATION_SOURCE']}/{breakdown['WORK_ARRANGEMENT']}: {breakdown['RELATIONSHIP_COUNT']}")

            return {
                "status": "success",
                **bridge_stats,
                "breakdown_stats": breakdown_stats,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in job-location bridge creation: {str(e)}")
        raise