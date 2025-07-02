"""
Analytics Bridge Assets

This module contains Dagster assets for creating and managing bridge tables
that handle many-to-many relationships in the analytics layer:

- analytics_job_experience_bridge: Job postings to experience requirements
- analytics_job_keywords_bridge: Job postings to keywords
"""

from typing import Dict, Any
from dagster import AssetExecutionContext, asset, MetadataValue
from dagster_snowflake import SnowflakeResource

from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["analytics_fact_job_postings", "analytics_dim_experience", "stage_job_experience_bridge"],
    description="Create bridge table for many-to-many job posting to experience relationships",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_job_experience_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build the analytics job experience bridge table from stage data.

    This asset creates the bridge table that properly handles the many-to-many relationship
    between job postings and experience requirements, eliminating duplicates in the fact table
    while preserving all experience data for comprehensive analysis.

    Processing Logic:
    1. Source data from STAGE.JOB_EXPERIENCE_BRIDGE with confidence filtering
    2. Join with FACT_JOB_POSTINGS and DIM_EXPERIENCE for key lookups
    3. Apply business rules for primary experience selection
    4. Generate weighted relationships based on confidence and importance
    5. Validate bridge completeness and data quality

    Business Rules:
    - Primary experience selection: Highest confidence → Minimum requirement → Lowest years
    - Include only experience requirements with confidence ≥ 0.7
    - Generate experience weights based on extraction confidence
    - Preserve technology context and processing method audit trail

    Returns:
        Dict containing execution results and bridge relationship statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_job_experience_bridge.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting analytics job experience bridge build from STAGE sources")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing job experience bridge data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build bridge table with dimension lookups and business rules
            context.log.info("Building job experience bridge with quality filters and primary experience selection")

            build_sql = f"""
            INSERT INTO {table_name} (
                EXPERIENCE_BRIDGE_KEY,
                JOB_POSTING_KEY,
                EXPERIENCE_KEY,
                EXPERIENCE_WEIGHT,
                IS_PRIMARY_REQUIREMENT,
                EXTRACTION_CONFIDENCE,
                TECHNOLOGY_CONTEXT,
                PROCESSING_METHOD,
                CREATED_TIMESTAMP
            )
            WITH quality_job_experience AS (
                SELECT
                    jeb.JOB_UID,
                    jeb.EXPERIENCE_ID,
                    jeb.EXPERIENCE_SOURCE,
                    jeb.EXPERIENCE_VALUE_NUMERIC,
                    jeb.EXPERIENCE_VALUE_TEXT,
                    jeb.TECHNOLOGY_CONTEXT,
                    jeb.IS_MINIMUM_REQUIREMENT,
                    jeb.EXTRACTION_CONFIDENCE,
                    jeb.PROCESSING_METHOD,

                    -- Generate bridge key
                    'EXP_BRIDGE_' || jeb.JOB_UID || '_' || jeb.EXPERIENCE_ID as experience_bridge_key

                FROM BETTERJOBS_DB.STAGE.JOB_EXPERIENCE_BRIDGE jeb
                WHERE jeb.EXTRACTION_CONFIDENCE >= 0.7
                  AND jeb.EXPERIENCE_ID IS NOT NULL
                  AND jeb.JOB_UID IS NOT NULL
            ),

            bridge_with_keys AS (
                SELECT
                    qje.*,

                    -- Job posting key lookup
                    fjp.JOB_POSTING_KEY,

                    -- Experience dimension key lookup
                    de.EXPERIENCE_KEY,

                    -- Calculate experience weight based on confidence and type
                    CASE
                        WHEN qje.IS_MINIMUM_REQUIREMENT = TRUE THEN qje.EXTRACTION_CONFIDENCE * 1.2
                        ELSE qje.EXTRACTION_CONFIDENCE
                    END as experience_weight

                FROM quality_job_experience qje

                -- Join with fact table to get job posting keys
                INNER JOIN BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
                    ON qje.JOB_UID = fjp.JOB_UID

                -- Join with experience dimension to get experience keys
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_EXPERIENCE de
                    ON qje.EXPERIENCE_ID = de.EXPERIENCE_ID

                WHERE fjp.JOB_POSTING_KEY IS NOT NULL
                  AND de.EXPERIENCE_KEY IS NOT NULL
            ),

            bridge_with_primary_flags AS (
                SELECT
                    bwk.*,

                    -- Determine primary experience per job posting
                    CASE WHEN ROW_NUMBER() OVER (
                        PARTITION BY bwk.JOB_POSTING_KEY
                        ORDER BY
                            bwk.EXTRACTION_CONFIDENCE DESC,
                            bwk.IS_MINIMUM_REQUIREMENT DESC,
                            COALESCE(bwk.EXPERIENCE_VALUE_NUMERIC, 999) ASC
                    ) = 1 THEN TRUE ELSE FALSE END as is_primary_requirement

                FROM bridge_with_keys bwk
            )

            SELECT
                experience_bridge_key as EXPERIENCE_BRIDGE_KEY,
                JOB_POSTING_KEY,
                EXPERIENCE_KEY,
                ROUND(experience_weight, 3) as EXPERIENCE_WEIGHT,
                is_primary_requirement as IS_PRIMARY_REQUIREMENT,
                EXTRACTION_CONFIDENCE,
                TECHNOLOGY_CONTEXT,
                PROCESSING_METHOD,
                CURRENT_TIMESTAMP as CREATED_TIMESTAMP

            FROM bridge_with_primary_flags
            ORDER BY JOB_POSTING_KEY, EXPERIENCE_WEIGHT DESC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} job experience bridge records")

            # Step 3: Validate bridge data quality and gather statistics
            context.log.info("Validating bridge table data quality and gathering relationship statistics")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_bridge_records,
                COUNT(DISTINCT jeb.JOB_POSTING_KEY) as unique_job_postings,
                COUNT(DISTINCT jeb.EXPERIENCE_KEY) as unique_experience_keys,
                COUNT(CASE WHEN jeb.IS_PRIMARY_REQUIREMENT = TRUE THEN 1 END) as primary_requirements,

                -- Bridge relationship metrics
                AVG(CASE WHEN cnt.experience_count > 1 THEN cnt.experience_count ELSE NULL END) as avg_experiences_per_multi_exp_job,
                COUNT(CASE WHEN cnt.experience_count > 1 THEN 1 END) as jobs_with_multiple_experiences,
                COUNT(CASE WHEN cnt.experience_count = 1 THEN 1 END) as jobs_with_single_experience,
                MAX(cnt.experience_count) as max_experiences_per_job,

                -- Quality metrics
                AVG(jeb.EXTRACTION_CONFIDENCE) as avg_extraction_confidence,
                AVG(jeb.EXPERIENCE_WEIGHT) as avg_experience_weight,
                COUNT(CASE WHEN jeb.TECHNOLOGY_CONTEXT IS NOT NULL THEN 1 END) as with_technology_context,

                -- Processing method distribution
                COUNT(CASE WHEN jeb.PROCESSING_METHOD = 'llm_auto' THEN 1 END) as llm_auto_processed,
                COUNT(CASE WHEN jeb.PROCESSING_METHOD = 'manual_review' THEN 1 END) as manually_reviewed

            FROM {table_name} jeb
            LEFT JOIN (
                SELECT
                    JOB_POSTING_KEY,
                    COUNT(*) as experience_count
                FROM {table_name}
                GROUP BY JOB_POSTING_KEY
            ) cnt ON jeb.JOB_POSTING_KEY = cnt.JOB_POSTING_KEY
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Analyze experience distribution patterns
            context.log.info("Analyzing experience requirement distribution patterns")

            cursor.execute(f"""
            SELECT
                cnt.experience_count,
                COUNT(*) as job_count,
                ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 2) as percentage
            FROM (
                SELECT
                    JOB_POSTING_KEY,
                    COUNT(*) as experience_count
                FROM {table_name}
                GROUP BY JOB_POSTING_KEY
            ) cnt
            GROUP BY cnt.experience_count
            ORDER BY cnt.experience_count
            """)

            experience_distribution = [
                {
                    "experience_count": row[0],
                    "job_count": row[1],
                    "percentage": float(row[2]) if row[2] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Top experience requirements analysis
            cursor.execute(f"""
            SELECT
                de.EXPERIENCE_NAME,
                de.EXPERIENCE_CATEGORY,
                COUNT(*) as job_count,
                COUNT(CASE WHEN jeb.IS_PRIMARY_REQUIREMENT = TRUE THEN 1 END) as primary_requirement_count,
                AVG(jeb.EXTRACTION_CONFIDENCE) as avg_confidence,
                AVG(jeb.EXPERIENCE_WEIGHT) as avg_weight
            FROM {table_name} jeb
            JOIN BETTERJOBS_DB.ANALYTICS.DIM_EXPERIENCE de ON jeb.EXPERIENCE_KEY = de.EXPERIENCE_KEY
            GROUP BY de.EXPERIENCE_NAME, de.EXPERIENCE_CATEGORY
            ORDER BY job_count DESC
            LIMIT 20
            """)

            top_experiences = [
                {
                    "experience_name": row[0],
                    "experience_category": row[1],
                    "job_count": row[2],
                    "primary_requirement_count": row[3],
                    "avg_confidence": float(row[4]) if row[4] is not None else 0.0,
                    "avg_weight": float(row[5]) if row[5] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Validate bridge completeness
            context.log.info("Validating bridge completeness against fact table")

            cursor.execute(f"""
            SELECT
                COUNT(DISTINCT fjp.JOB_POSTING_KEY) as total_job_postings_in_fact,
                COUNT(DISTINCT jeb.JOB_POSTING_KEY) as job_postings_with_experience,
                COUNT(DISTINCT fjp.JOB_POSTING_KEY) - COUNT(DISTINCT jeb.JOB_POSTING_KEY) as job_postings_without_experience
            FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
            LEFT JOIN {table_name} jeb ON fjp.JOB_POSTING_KEY = jeb.JOB_POSTING_KEY
            """)

            completeness_check = cursor.fetchone()

            # Step 7: Data quality validation checks
            quality_issues = []

            # Check for jobs without primary experience
            cursor.execute(f"""
                SELECT COUNT(DISTINCT jeb1.JOB_POSTING_KEY)
                FROM {table_name} jeb1
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} jeb2
                    WHERE jeb2.JOB_POSTING_KEY = jeb1.JOB_POSTING_KEY
                    AND jeb2.IS_PRIMARY_REQUIREMENT = TRUE
                )
            """)
            jobs_without_primary = cursor.fetchone()[0]
            if jobs_without_primary > 0:
                quality_issues.append(f"{jobs_without_primary} jobs without primary experience designation")

            # Check for jobs with multiple primary experiences
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM (
                    SELECT JOB_POSTING_KEY, COUNT(*) as primary_count
                    FROM {table_name}
                    WHERE IS_PRIMARY_REQUIREMENT = TRUE
                    GROUP BY JOB_POSTING_KEY
                    HAVING COUNT(*) > 1
                ) multi_primary
            """)
            jobs_with_multiple_primary = cursor.fetchone()[0]
            if jobs_with_multiple_primary > 0:
                quality_issues.append(f"{jobs_with_multiple_primary} jobs with multiple primary experience requirements")

            # Check for invalid confidence scores
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE EXTRACTION_CONFIDENCE < 0.7 OR EXTRACTION_CONFIDENCE > 1.0
            """)
            invalid_confidence_count = cursor.fetchone()[0]
            if invalid_confidence_count > 0:
                quality_issues.append(f"{invalid_confidence_count} bridge records with invalid confidence scores")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 8: Calculate derived statistics
            total_records = validation_result[0]
            unique_jobs = validation_result[1]
            unique_experiences = validation_result[2]
            coverage_percentage = (completeness_check[1] / completeness_check[0] * 100) if completeness_check[0] > 0 else 0
            multi_experience_percentage = (validation_result[5] / unique_jobs * 100) if unique_jobs > 0 else 0

            context.log.info(f"Bridge table validation: {total_records} total bridge records, "
                           f"{unique_jobs} unique job postings, {unique_experiences} unique experiences")

            context.log.info(f"Experience patterns: {validation_result[5]} jobs with multiple experiences "
                           f"({multi_experience_percentage:.1f}%), max {validation_result[7]} experiences per job")

            context.log.info(f"Bridge completeness: {coverage_percentage:.1f}% job postings have experience data "
                           f"({completeness_check[1]}/{completeness_check[0]})")

            context.log.info(f"Quality metrics: avg confidence {validation_result[8]:.3f}, "
                           f"avg weight {validation_result[9]:.3f}")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_bridge_records": MetadataValue.int(total_records),
                "unique_job_postings": MetadataValue.int(unique_jobs),
                "unique_experience_keys": MetadataValue.int(unique_experiences),
                "primary_requirements": MetadataValue.int(validation_result[3]),
                "jobs_with_multiple_experiences": MetadataValue.int(validation_result[5]),
                "jobs_with_single_experience": MetadataValue.int(validation_result[6]),
                "max_experiences_per_job": MetadataValue.int(validation_result[7]),
                "avg_experiences_per_multi_exp_job": MetadataValue.float(float(validation_result[4]) if validation_result[4] is not None else 0.0),
                "avg_extraction_confidence": MetadataValue.float(float(validation_result[8]) if validation_result[8] is not None else 0.0),
                "avg_experience_weight": MetadataValue.float(float(validation_result[9]) if validation_result[9] is not None else 0.0),
                "with_technology_context": MetadataValue.int(validation_result[10]),
                "llm_auto_processed": MetadataValue.int(validation_result[11]),
                "manually_reviewed": MetadataValue.int(validation_result[12]),
                "coverage_percentage": MetadataValue.float(coverage_percentage),
                "multi_experience_percentage": MetadataValue.float(multi_experience_percentage),
                "job_postings_without_experience": MetadataValue.int(completeness_check[2]),
                "experience_distribution": MetadataValue.json(experience_distribution[:10]),
                "top_experience_requirements": MetadataValue.json(top_experiences),
                "quality_issues_count": MetadataValue.int(len(quality_issues))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "bridge_statistics": {
                    "total_bridge_records": total_records,
                    "unique_job_postings": unique_jobs,
                    "unique_experience_keys": unique_experiences,
                    "primary_requirements": validation_result[3]
                },
                "experience_patterns": {
                    "jobs_with_multiple_experiences": validation_result[5],
                    "jobs_with_single_experience": validation_result[6],
                    "max_experiences_per_job": validation_result[7],
                    "avg_experiences_per_multi_exp_job": float(validation_result[4]) if validation_result[4] is not None else 0.0,
                    "multi_experience_percentage": multi_experience_percentage,
                    "experience_distribution": experience_distribution
                },
                "quality_metrics": {
                    "avg_extraction_confidence": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "avg_experience_weight": float(validation_result[9]) if validation_result[9] is not None else 0.0,
                    "with_technology_context": validation_result[10],
                    "quality_issues": quality_issues
                },
                "processing_metrics": {
                    "llm_auto_processed": validation_result[11],
                    "manually_reviewed": validation_result[12]
                },
                "bridge_completeness": {
                    "total_job_postings_in_fact": completeness_check[0],
                    "job_postings_with_experience": completeness_check[1],
                    "job_postings_without_experience": completeness_check[2],
                    "coverage_percentage": coverage_percentage
                },
                "top_experience_requirements": top_experiences
            }

        finally:
            cursor.close()


@asset(
    deps=["analytics_fact_job_postings", "analytics_dim_keywords", "stage_job_keywords_bridge"],
    description="Create bridge table for many-to-many job posting to keywords relationships",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_job_keywords_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build the analytics job keywords bridge table from stage data.

    This asset creates the bridge table that properly handles the many-to-many relationship
    between job postings and keywords, capturing all keywords extracted from job descriptions
    to enable comprehensive keyword analysis and market intelligence.

    Processing Logic:
    1. Source data from STAGE.JOB_KEYWORDS_BRIDGE with confidence filtering
    2. Join with FACT_JOB_POSTINGS and DIM_KEYWORDS for key lookups
    3. Generate weighted relationships based on confidence and position
    4. Validate bridge completeness and data quality

    Business Rules:
    - Include only keywords with confidence ≥ 0.5
    - Generate keyword weights based on position and extraction confidence
    - Preserve keyword position and category information for analysis

    Returns:
        Dict containing execution results and bridge relationship statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_job_keywords_bridge.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting analytics job keywords bridge build from STAGE sources")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing job keywords bridge data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build bridge table with dimension lookups and business rules
            context.log.info("Building job keywords bridge with quality filters and weighting logic")

            build_sql = f"""
            INSERT INTO {table_name} (
                KEYWORDS_BRIDGE_KEY,
                JOB_POSTING_KEY,
                KEYWORD_KEY,
                KEYWORD_WEIGHT,
                KEYWORD_POSITION,
                EXTRACTION_CONFIDENCE,
                KEYWORD_TYPE,
                KEYWORD_CATEGORY,
                PROCESSING_METHOD,
                CREATED_TIMESTAMP
            )
            WITH quality_job_keywords AS (
                SELECT
                    jkb.JOB_UID,
                    jkb.KEYWORD_ID,
                    jkb.KEYWORD_SOURCE,
                    jkb.ORIGINAL_TEXT,
                    jkb.EXTRACTION_CONFIDENCE,
                    jkb.STANDARDIZATION_CONFIDENCE,
                    jkb.OVERALL_CONFIDENCE,

                    -- Generate bridge key
                    'KW_BRIDGE_' || jkb.JOB_UID || '_' || jkb.KEYWORD_ID as keywords_bridge_key,

                    -- Create synthetic position based on overall confidence (higher confidence = lower position number)
                    ROW_NUMBER() OVER (PARTITION BY jkb.JOB_UID ORDER BY jkb.OVERALL_CONFIDENCE DESC, jkb.KEYWORD_ID) as keyword_position

                FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb
                WHERE jkb.OVERALL_CONFIDENCE >= 0.5
                  AND jkb.KEYWORD_ID IS NOT NULL
                  AND jkb.JOB_UID IS NOT NULL
            ),

            bridge_with_keys AS (
                SELECT
                    qjk.*,

                    -- Job posting key lookup
                    fjp.JOB_POSTING_KEY,

                    -- Keyword dimension key lookup
                    dk.KEYWORD_KEY,
                    dk.KEYWORD_TYPE,
                    dk.KEYWORD_CATEGORY,

                    -- Calculate keyword weight based on confidence and position
                    CASE
                        WHEN qjk.keyword_position = 1 AND qjk.OVERALL_CONFIDENCE >= 0.8 THEN qjk.OVERALL_CONFIDENCE * 1.0
                        WHEN qjk.keyword_position <= 3 AND qjk.OVERALL_CONFIDENCE >= 0.8 THEN qjk.OVERALL_CONFIDENCE * 0.9
                        WHEN qjk.keyword_position <= 5 AND qjk.OVERALL_CONFIDENCE >= 0.7 THEN qjk.OVERALL_CONFIDENCE * 0.8
                        ELSE qjk.OVERALL_CONFIDENCE * 0.7
                    END as keyword_weight,

                    -- Set processing method based on available data
                    CASE
                        WHEN qjk.KEYWORD_SOURCE = 'manual' THEN 'manual_review'
                        ELSE 'llm_auto'
                    END as processing_method

                FROM quality_job_keywords qjk

                -- Join with fact table to get job posting keys
                INNER JOIN BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
                    ON qjk.JOB_UID = fjp.JOB_UID

                -- Join with keywords dimension to get keyword keys
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_KEYWORDS dk
                    ON qjk.KEYWORD_ID = dk.KEYWORD_ID

                WHERE fjp.JOB_POSTING_KEY IS NOT NULL
                  AND dk.KEYWORD_KEY IS NOT NULL
            )

            SELECT
                keywords_bridge_key as KEYWORDS_BRIDGE_KEY,
                JOB_POSTING_KEY,
                KEYWORD_KEY,
                ROUND(keyword_weight, 3) as KEYWORD_WEIGHT,
                keyword_position as KEYWORD_POSITION,
                EXTRACTION_CONFIDENCE,
                KEYWORD_TYPE,
                KEYWORD_CATEGORY,
                processing_method as PROCESSING_METHOD,
                CURRENT_TIMESTAMP as CREATED_TIMESTAMP

            FROM bridge_with_keys
            ORDER BY JOB_POSTING_KEY, keyword_position ASC, KEYWORD_WEIGHT DESC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} job keywords bridge records")

            # Step 3: Validate bridge data quality and gather statistics
            context.log.info("Validating bridge table data quality and gathering relationship statistics")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_bridge_records,
                COUNT(DISTINCT jkb.JOB_POSTING_KEY) as unique_job_postings,
                COUNT(DISTINCT jkb.KEYWORD_KEY) as unique_keyword_keys,

                -- Bridge relationship metrics
                AVG(CASE WHEN cnt.keyword_count > 1 THEN cnt.keyword_count ELSE NULL END) as avg_keywords_per_multi_kw_job,
                COUNT(CASE WHEN cnt.keyword_count > 1 THEN 1 END) as jobs_with_multiple_keywords,
                COUNT(CASE WHEN cnt.keyword_count = 1 THEN 1 END) as jobs_with_single_keyword,
                MAX(cnt.keyword_count) as max_keywords_per_job,

                -- Quality metrics
                AVG(jkb.EXTRACTION_CONFIDENCE) as avg_extraction_confidence,
                AVG(jkb.KEYWORD_WEIGHT) as avg_keyword_weight,
                COUNT(CASE WHEN jkb.KEYWORD_CATEGORY IS NOT NULL THEN 1 END) as with_keyword_category,

                -- Processing method distribution
                COUNT(CASE WHEN jkb.PROCESSING_METHOD = 'llm_auto' THEN 1 END) as llm_auto_processed,
                COUNT(CASE WHEN jkb.PROCESSING_METHOD = 'manual_review' THEN 1 END) as manually_reviewed,

                -- Keyword type distribution
                COUNT(CASE WHEN jkb.KEYWORD_TYPE = 'primary' THEN 1 END) as primary_type_keywords,
                COUNT(CASE WHEN jkb.KEYWORD_TYPE = 'secondary' THEN 1 END) as secondary_type_keywords,

                -- Position distribution
                AVG(jkb.KEYWORD_POSITION) as avg_keyword_position,
                COUNT(CASE WHEN jkb.KEYWORD_POSITION = 1 THEN 1 END) as first_position_keywords

            FROM {table_name} jkb
            LEFT JOIN (
                SELECT
                    JOB_POSTING_KEY,
                    COUNT(*) as keyword_count
                FROM {table_name}
                GROUP BY JOB_POSTING_KEY
            ) cnt ON jkb.JOB_POSTING_KEY = cnt.JOB_POSTING_KEY
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Analyze keyword distribution patterns
            context.log.info("Analyzing keyword requirement distribution patterns")

            cursor.execute(f"""
            SELECT
                cnt.keyword_count,
                COUNT(*) as job_count,
                ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 2) as percentage
            FROM (
                SELECT
                    JOB_POSTING_KEY,
                    COUNT(*) as keyword_count
                FROM {table_name}
                GROUP BY JOB_POSTING_KEY
            ) cnt
            GROUP BY cnt.keyword_count
            ORDER BY cnt.keyword_count
            """)

            keyword_distribution = [
                {
                    "keyword_count": row[0],
                    "job_count": row[1],
                    "percentage": float(row[2]) if row[2] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Top keyword requirements analysis
            cursor.execute(f"""
            SELECT
                dk.KEYWORD_TEXT,
                dk.KEYWORD_CATEGORY,
                dk.KEYWORD_TYPE,
                COUNT(*) as job_count,
                AVG(jkb.EXTRACTION_CONFIDENCE) as avg_confidence,
                AVG(jkb.KEYWORD_WEIGHT) as avg_weight,
                AVG(jkb.KEYWORD_POSITION) as avg_position
            FROM {table_name} jkb
            JOIN BETTERJOBS_DB.ANALYTICS.DIM_KEYWORDS dk ON jkb.KEYWORD_KEY = dk.KEYWORD_KEY
            GROUP BY dk.KEYWORD_TEXT, dk.KEYWORD_CATEGORY, dk.KEYWORD_TYPE
            ORDER BY job_count DESC
            LIMIT 25
            """)

            top_keywords = [
                {
                    "keyword_text": row[0],
                    "keyword_category": row[1],
                    "keyword_type": row[2],
                    "job_count": row[3],
                    "avg_confidence": float(row[4]) if row[4] is not None else 0.0,
                    "avg_weight": float(row[5]) if row[5] is not None else 0.0,
                    "avg_position": float(row[6]) if row[6] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Validate bridge completeness
            context.log.info("Validating bridge completeness against fact table")

            cursor.execute(f"""
            SELECT
                COUNT(DISTINCT fjp.JOB_POSTING_KEY) as total_job_postings_in_fact,
                COUNT(DISTINCT jkb.JOB_POSTING_KEY) as job_postings_with_keywords,
                COUNT(DISTINCT fjp.JOB_POSTING_KEY) - COUNT(DISTINCT jkb.JOB_POSTING_KEY) as job_postings_without_keywords
            FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
            LEFT JOIN {table_name} jkb ON fjp.JOB_POSTING_KEY = jkb.JOB_POSTING_KEY
            """)

            completeness_check = cursor.fetchone()

            # Step 7: Data quality validation checks
            quality_issues = []



            # Check for invalid confidence scores
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE EXTRACTION_CONFIDENCE < 0.5 OR EXTRACTION_CONFIDENCE > 1.0
            """)
            invalid_confidence_count = cursor.fetchone()[0]
            if invalid_confidence_count > 0:
                quality_issues.append(f"{invalid_confidence_count} bridge records with invalid confidence scores")

            # Check for invalid positions
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE KEYWORD_POSITION IS NULL OR KEYWORD_POSITION < 1
            """)
            invalid_position_count = cursor.fetchone()[0]
            if invalid_position_count > 0:
                quality_issues.append(f"{invalid_position_count} bridge records with invalid keyword positions")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 8: Calculate derived statistics
            total_records = validation_result[0]
            unique_jobs = validation_result[1]
            unique_keywords = validation_result[2]
            coverage_percentage = (completeness_check[1] / completeness_check[0] * 100) if completeness_check[0] > 0 else 0
            multi_keyword_percentage = (validation_result[4] / unique_jobs * 100) if unique_jobs > 0 else 0

            context.log.info(f"Bridge table validation: {total_records} total bridge records, "
                           f"{unique_jobs} unique job postings, {unique_keywords} unique keywords")

            context.log.info(f"Keyword patterns: {validation_result[4]} jobs with multiple keywords "
                           f"({multi_keyword_percentage:.1f}%), max {validation_result[6]} keywords per job")

            context.log.info(f"Bridge completeness: {coverage_percentage:.1f}% job postings have keyword data "
                           f"({completeness_check[1]}/{completeness_check[0]})")

            context.log.info(f"Quality metrics: avg confidence {validation_result[7]:.3f}, "
                           f"avg weight {validation_result[8]:.3f}, avg position {validation_result[14]:.1f}")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_bridge_records": MetadataValue.int(total_records),
                "unique_job_postings": MetadataValue.int(unique_jobs),
                "unique_keyword_keys": MetadataValue.int(unique_keywords),
                "jobs_with_multiple_keywords": MetadataValue.int(validation_result[4]),
                "jobs_with_single_keyword": MetadataValue.int(validation_result[5]),
                "max_keywords_per_job": MetadataValue.int(validation_result[6]),
                "avg_keywords_per_multi_kw_job": MetadataValue.float(float(validation_result[3]) if validation_result[3] is not None else 0.0),
                "avg_extraction_confidence": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "avg_keyword_weight": MetadataValue.float(float(validation_result[8]) if validation_result[8] is not None else 0.0),
                "with_keyword_category": MetadataValue.int(validation_result[9]),
                "llm_auto_processed": MetadataValue.int(validation_result[10]),
                "manually_reviewed": MetadataValue.int(validation_result[11]),
                "primary_type_keywords": MetadataValue.int(validation_result[12]),
                "secondary_type_keywords": MetadataValue.int(validation_result[13]),
                "avg_keyword_position": MetadataValue.float(float(validation_result[14]) if validation_result[14] is not None else 0.0),
                "first_position_keywords": MetadataValue.int(validation_result[15]),
                "coverage_percentage": MetadataValue.float(coverage_percentage),
                "multi_keyword_percentage": MetadataValue.float(multi_keyword_percentage),
                "job_postings_without_keywords": MetadataValue.int(completeness_check[2]),
                "keyword_distribution": MetadataValue.json(keyword_distribution[:10]),
                "top_keyword_requirements": MetadataValue.json(top_keywords),
                "quality_issues_count": MetadataValue.int(len(quality_issues))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "bridge_statistics": {
                    "total_bridge_records": total_records,
                    "unique_job_postings": unique_jobs,
                    "unique_keyword_keys": unique_keywords
                },
                "keyword_patterns": {
                    "jobs_with_multiple_keywords": validation_result[4],
                    "jobs_with_single_keyword": validation_result[5],
                    "max_keywords_per_job": validation_result[6],
                    "avg_keywords_per_multi_kw_job": float(validation_result[3]) if validation_result[3] is not None else 0.0,
                    "multi_keyword_percentage": multi_keyword_percentage,
                    "keyword_distribution": keyword_distribution
                },
                "quality_metrics": {
                    "avg_extraction_confidence": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "avg_keyword_weight": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "with_keyword_category": validation_result[9],
                    "avg_keyword_position": float(validation_result[14]) if validation_result[14] is not None else 0.0,
                    "first_position_keywords": validation_result[15],
                    "quality_issues": quality_issues
                },
                "processing_metrics": {
                    "llm_auto_processed": validation_result[10],
                    "manually_reviewed": validation_result[11],
                    "primary_type_keywords": validation_result[12],
                    "secondary_type_keywords": validation_result[13]
                },
                "bridge_completeness": {
                    "total_job_postings_in_fact": completeness_check[0],
                    "job_postings_with_keywords": completeness_check[1],
                    "job_postings_without_keywords": completeness_check[2],
                    "coverage_percentage": coverage_percentage
                },
                "top_keyword_requirements": top_keywords
            }

        finally:
            cursor.close()