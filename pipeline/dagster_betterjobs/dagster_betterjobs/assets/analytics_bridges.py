"""
Analytics Job Experience Bridge Asset

This module contains the Dagster asset for creating and managing the bridge table
that handles the many-to-many relationship between job postings and experience requirements.
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