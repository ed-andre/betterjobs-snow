"""
Experience Normalization Assets

Assets for extracting, standardizing, and normalizing experience data from LLM-enriched
VARIANT columns into proper relational structures for analytics.

Addresses BUG-015: Dead EXPERIENCE_LEVEL_CONTEXT Column and Missing Experience Extraction Pipeline

Dependencies:
- stage_jobs_llm_enriched: Source of LLM-extracted experience data
- stage_jobs_unified: Source of job metadata

Output Tables:
- EXPERIENCE_RAW_EXTRACTION: Flattened experience data from LLM sources
- EXPERIENCE_NORMALIZED: Master experience levels with standardized definitions
- JOB_EXPERIENCE_BRIDGE: Many-to-many job-experience relationships
"""

import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource
from dagster_betterjobs.utils.schema_utils import ensure_object_exists, execute_sql_file


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten experience requirements from LLM VARIANT columns",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_experience_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all experience requirements from JOBS_LLM_ENRICHED and flatten into workable format.

    Processes:
    - MIN_YEARS_EXPERIENCE/MAX_YEARS_EXPERIENCE: Numeric year requirements
    - EXPERIENCE_LEVEL: Entry/Mid/Senior/Executive classifications
    - SENIORITY_LEVEL: Staff/Principal/Director/VP levels
    - SPECIFIC_TECHNOLOGIES_YEARS: Technology-specific requirements (JSON)

    Output: Raw experience requirements with source tracking and confidence scores
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "experience_records_extracted": 0,
        "min_years_count": 0,
        "max_years_count": 0,
        "experience_level_count": 0,
        "seniority_level_count": 0,
        "technology_specific_count": 0,
        "unique_jobs_processed": 0,
        "extraction_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM experience raw extraction...")

        # Create the raw extraction view using schema-as-code pattern
        view_name = ensure_object_exists("views/stage_experience_raw_extraction.sql", snowflake, context)
        context.log.info(f"✅ Created {view_name} view using schema-as-code")

        # Get extraction statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'min_years' THEN 1 END) as min_years,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'max_years' THEN 1 END) as max_years,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'experience_level' THEN 1 END) as exp_level,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'seniority_level' THEN 1 END) as seniority,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'specific_tech' THEN 1 END) as tech_specific
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "experience_records_extracted": result[0],
                "unique_jobs_processed": result[1],
                "min_years_count": result[2],
                "max_years_count": result[3],
                "experience_level_count": result[4],
                "seniority_level_count": result[5],
                "technology_specific_count": result[6]
            })

        # Sample some data for validation
        cursor.execute("""
        SELECT
            EXPERIENCE_SOURCE,
            EXPERIENCE_TYPE,
            ORIGINAL_TEXT,
            TECHNOLOGY_NAME,
            COUNT(*) as frequency
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION
        GROUP BY EXPERIENCE_SOURCE, EXPERIENCE_TYPE, ORIGINAL_TEXT, TECHNOLOGY_NAME
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_experience_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Experience Raw Extraction Complete:
        • Total Records Extracted: {stats['experience_records_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Min Years: {stats['min_years_count']:,}
        • Max Years: {stats['max_years_count']:,}
        • Experience Levels: {stats['experience_level_count']:,}
        • Seniority Levels: {stats['seniority_level_count']:,}
        • Technology-Specific: {stats['technology_specific_count']:,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "experience_records_extracted": MetadataValue.int(stats["experience_records_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "min_years_count": MetadataValue.int(stats["min_years_count"]),
            "max_years_count": MetadataValue.int(stats["max_years_count"]),
            "experience_level_count": MetadataValue.int(stats["experience_level_count"]),
            "seniority_level_count": MetadataValue.int(stats["seniority_level_count"]),
            "technology_specific_count": MetadataValue.int(stats["technology_specific_count"]),
            "top_experience_sample": MetadataValue.json(stats.get("top_experience_sample", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Experience raw extraction failed: {str(e)}")
        stats["extraction_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    description="Maintain experience standardization rules and mappings",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_experience_standardization_rules(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Ensure experience standardization rules table exists and has rules.

    Note: Rules should be managed via insert_experience_standardization_rules.sql
    in the data_population directory. This asset only ensures the table exists
    and validates that rules are present.
    """

    # Ensure the table exists using schema-as-code pattern
    table_name = ensure_object_exists("tables/stage_experience_standardization_rules.sql", snowflake, context)

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "rules_timestamp": datetime.now().isoformat(),
        "total_rules_found": 0,
        "categories_covered": 0,
        "high_confidence_rules": 0,
        "rules_table_created": True,
        "data_population_executed": False,
        "sql_statements_executed": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("📋 Checking experience standardization rules...")

        # Check if rules exist
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rule_count = cursor.fetchone()[0]

        if rule_count == 0:
            context.log.info("📥 No standardization rules found. Loading from data population script...")

            # Execute the data population script
            import os

            # Get the absolute path to the SQL file
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent.parent.parent
            sql_file_path = project_root / "sql" / "data_population" / "insert_experience_standardization_rules.sql"

            if sql_file_path.exists():
                context.log.info(f"📂 Executing data population from: {sql_file_path}")
                execution_result = execute_sql_file(snowflake, str(sql_file_path), context)

                if execution_result["status"] == "success":
                    context.log.info(f"✅ Successfully executed {execution_result['statements_executed']} statements")
                    stats["data_population_executed"] = True
                    stats["sql_statements_executed"] = execution_result['statements_executed']

                    # CRITICAL: Fail asset if any SQL statements failed
                    if execution_result['statements_failed'] > 0:
                        failed_statements = [r for r in execution_result.get('results', []) if r.get('status') == 'error']
                        error_details = []
                        for failed in failed_statements[:3]:  # Show first 3 failures
                            error_details.append(f"Statement {failed['statement_num']}: {failed.get('error', 'Unknown error')}")

                        error_summary = "; ".join(error_details)
                        if len(failed_statements) > 3:
                            error_summary += f" (and {len(failed_statements) - 3} more failures)"

                        context.log.error(f"❌ {execution_result['statements_failed']} SQL statements failed: {error_summary}")
                        raise Exception(f"Critical SQL operation failed: {execution_result['statements_failed']} statements failed during data population. {error_summary}")
                else:
                    error_msg = execution_result.get('error', 'Unknown error')
                    context.log.error(f"❌ Failed to execute SQL file: {error_msg}")
                    raise Exception(f"Critical SQL file execution failed: {error_msg}")

                # Re-check rule count after population
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                rule_count = cursor.fetchone()[0]
                context.log.info(f"📊 Rules populated: {rule_count} experience standardization rules now available")

                # CRITICAL: Verify data was actually populated
                if rule_count == 0:
                    context.log.error("❌ No rules found after data population - SQL execution may have failed silently")
                    raise Exception("Data population verification failed: No rules found in table after execution")
            else:
                context.log.error(f"❌ Data population file not found: {sql_file_path}")
        else:
            context.log.info(f"✅ Found {rule_count} existing experience standardization rules")

        # Get statistics on existing rules
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_rules,
            COUNT(DISTINCT EXPERIENCE_CATEGORY) as categories,
            COUNT(CASE WHEN IS_ACTIVE = TRUE THEN 1 END) as active_rules
        FROM {table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "total_rules_found": result[0],
                "categories_covered": result[1],
                "high_confidence_rules": result[2]
            })

        # Sample some rules for validation
        cursor.execute(f"""
        SELECT EXPERIENCE_CATEGORY, EXPERIENCE_NAME, MIN_YEARS_REQUIRED, MAX_YEARS_REQUIRED, SENIORITY_ORDER
        FROM {table_name}
        WHERE IS_ACTIVE = TRUE
        ORDER BY SENIORITY_ORDER
        LIMIT 10
        """)

        sample_rules = cursor.fetchall()
        if sample_rules:
            columns = [desc[0] for desc in cursor.description]
            stats["sample_rules"] = [dict(zip(columns, row)) for row in sample_rules]

        context.log.info(f"""
        📋 Experience Standardization Rules Status:
        • Total Rules Found: {stats['total_rules_found']}
        • Categories Covered: {stats['categories_covered']}
        • Active Rules: {stats['high_confidence_rules']}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "total_rules_found": MetadataValue.int(stats["total_rules_found"]),
            "categories_covered": MetadataValue.int(stats["categories_covered"]),
            "active_rules": MetadataValue.int(stats["high_confidence_rules"]),
            "rules_table_created": MetadataValue.bool(stats["rules_table_created"]),
            "data_population_executed": MetadataValue.bool(stats.get("data_population_executed", False)),
            "sql_statements_executed": MetadataValue.int(stats.get("sql_statements_executed", 0)),
            "sample_rules": MetadataValue.json(stats.get("sample_rules", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Experience standardization rules check failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()

@asset(
    deps=["stage_llm_experience_raw_extraction", "stage_experience_standardization_rules"],
    description="Create normalized experience master table with standardized levels",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_experience_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create experience master table.

    Processing:
    - Standardize experience levels (Entry: 0-2 years, Mid: 3-5 years, etc.)
    - Create seniority ordering for analytics
    - Calculate market frequency for each experience level
    - Handle technology-specific experience requirements

    Output: Clean experience master table for analytics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/stage_experience_normalized.sql", snowflake, context)

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "normalization_timestamp": datetime.now().isoformat(),
        "normalized_levels_created": 0,
        "general_experience_levels": 0,
        "technology_specific_levels": 0,
        "seniority_levels": 0,
        "normalization_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔧 Starting experience normalization...")

        # Clear existing data
        context.log.info("🧹 Clearing existing normalized experience data...")
        cursor.execute(f"DELETE FROM {table_name}")

        # Insert standardized experience levels from rules table
        context.log.info("📋 Loading experience levels from standardization rules...")
        rules_insert_sql = f"""
        INSERT INTO {table_name} (
            EXPERIENCE_ID, EXPERIENCE_NAME, EXPERIENCE_CATEGORY,
            MIN_YEARS_REQUIRED, MAX_YEARS_REQUIRED, SENIORITY_ORDER,
            EXPERIENCE_DESCRIPTION, CONFIDENCE_SCORE
        )
        SELECT
            CONCAT('EXP_', UPPER(REGEXP_REPLACE(esr.RULE_ID, '[^A-Za-z0-9]', '_'))) as EXPERIENCE_ID,
            esr.EXPERIENCE_NAME,
            esr.EXPERIENCE_CATEGORY,
            esr.MIN_YEARS_REQUIRED,
            esr.MAX_YEARS_REQUIRED,
            esr.SENIORITY_ORDER,
            esr.EXPERIENCE_DESCRIPTION,
            1.0 as CONFIDENCE_SCORE  -- Rules have high confidence by default
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_STANDARDIZATION_RULES esr
        WHERE esr.IS_ACTIVE = TRUE
          AND esr.EXPERIENCE_CATEGORY IN ('general', 'role_seniority')
        ORDER BY esr.SENIORITY_ORDER
        """

        cursor.execute(rules_insert_sql)
        rules_count = cursor.rowcount
        context.log.info(f"✅ Loaded {rules_count} experience levels from standardization rules")

        # Calculate market frequency for extracted experience levels
        context.log.info("📊 Calculating market frequency for experience requirements...")

        # Update market frequency for general experience (based on years)
        cursor.execute(f"""
        UPDATE {table_name}
        SET MARKET_FREQUENCY = (
            SELECT COUNT(DISTINCT ere.JOB_UID)
            FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION ere
            WHERE ere.EXPERIENCE_TYPE = 'general'
              AND TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) BETWEEN
                  {table_name}.MIN_YEARS_REQUIRED AND {table_name}.MAX_YEARS_REQUIRED
        )
        WHERE EXPERIENCE_CATEGORY = 'general'
        """)

        # Create technology-specific experience levels from raw data
        context.log.info("💻 Creating technology-specific experience levels...")

        cursor.execute(f"""
        INSERT INTO {table_name} (
            EXPERIENCE_ID, EXPERIENCE_NAME, EXPERIENCE_CATEGORY,
            MIN_YEARS_REQUIRED, MAX_YEARS_REQUIRED, SENIORITY_ORDER,
            EXPERIENCE_DESCRIPTION, MARKET_FREQUENCY, CONFIDENCE_SCORE
        )
        SELECT DISTINCT
            CONCAT('EXP_TECH_', UPPER(REGEXP_REPLACE(ere.TECHNOLOGY_NAME, '[^A-Za-z0-9]', '_'))) as EXPERIENCE_ID,
            CONCAT(ere.TECHNOLOGY_NAME, ' Experience') as EXPERIENCE_NAME,
            'technology_specific' as EXPERIENCE_CATEGORY,
            MIN(TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER)) as MIN_YEARS_REQUIRED,
            MAX(TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER)) as MAX_YEARS_REQUIRED,
            NULL as SENIORITY_ORDER,  -- Not applicable for tech-specific
            CONCAT('Experience with ', ere.TECHNOLOGY_NAME, ' technology') as EXPERIENCE_DESCRIPTION,
            COUNT(DISTINCT ere.JOB_UID) as MARKET_FREQUENCY,
            AVG(ere.EXTRACTION_CONFIDENCE) as CONFIDENCE_SCORE
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION ere
        WHERE ere.EXPERIENCE_TYPE = 'technology_specific'
          AND ere.TECHNOLOGY_NAME IS NOT NULL
          AND TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) > 0
        GROUP BY ere.TECHNOLOGY_NAME
        HAVING COUNT(DISTINCT ere.JOB_UID) >= 1  -- Only include tech with 1+ job mentions
        """)

        tech_count = cursor.rowcount
        context.log.info(f"✅ Created {tech_count} technology-specific experience levels")

        # Get normalization statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_levels,
            COUNT(CASE WHEN EXPERIENCE_CATEGORY = 'general' THEN 1 END) as general_levels,
            COUNT(CASE WHEN EXPERIENCE_CATEGORY = 'technology_specific' THEN 1 END) as tech_levels,
            COUNT(CASE WHEN EXPERIENCE_CATEGORY = 'role_seniority' THEN 1 END) as seniority_levels,
            SUM(MARKET_FREQUENCY) as total_market_frequency,
            AVG(CONFIDENCE_SCORE) as avg_confidence
        FROM {table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "normalized_levels_created": result[0],
                "general_experience_levels": result[1],
                "technology_specific_levels": result[2],
                "seniority_levels": result[3],
                "total_market_frequency": result[4],
                "average_confidence_score": round(result[5] or 0, 3)
            })

        # Sample normalized data for validation
        cursor.execute(f"""
        SELECT
            EXPERIENCE_CATEGORY,
            EXPERIENCE_NAME,
            MIN_YEARS_REQUIRED,
            MAX_YEARS_REQUIRED,
            MARKET_FREQUENCY,
            CONFIDENCE_SCORE
        FROM {table_name}
        ORDER BY EXPERIENCE_CATEGORY, SENIORITY_ORDER, MARKET_FREQUENCY DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["normalized_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Experience Normalization Complete:
        • Total Levels Created: {stats['normalized_levels_created']:,}
        • General Experience Levels: {stats['general_experience_levels']:,}
        • Technology-Specific Levels: {stats['technology_specific_levels']:,}
        • Seniority Levels: {stats['seniority_levels']:,}
        • Total Market Coverage: {stats.get('total_market_frequency', 0):,} job mentions
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "normalized_levels_created": MetadataValue.int(stats["normalized_levels_created"]),
            "general_experience_levels": MetadataValue.int(stats["general_experience_levels"]),
            "technology_specific_levels": MetadataValue.int(stats["technology_specific_levels"]),
            "seniority_levels": MetadataValue.int(stats["seniority_levels"]),
            "average_confidence_score": MetadataValue.float(stats.get("average_confidence_score", 0)),
            "normalized_sample": MetadataValue.json(stats.get("normalized_sample", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Experience normalization failed: {str(e)}")
        stats["normalization_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_experience_normalized", "stage_jobs_unified"],
    description="Create job-experience relationships with requirement context",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_experience_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized experience requirements with context.

    Features:
    - Source tracking (min_years vs max_years vs level vs seniority)
    - Technology-specific experience requirements
    - Minimum vs preferred requirement classification
    - Confidence scoring for job-experience associations
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/stage_job_experience_bridge.sql", snowflake, context)

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "bridge_timestamp": datetime.now().isoformat(),
        "job_experience_relationships": 0,
        "jobs_with_experience_requirements": 0,
        "general_experience_mappings": 0,
        "technology_specific_mappings": 0,
        "seniority_level_mappings": 0,
        "bridge_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Starting job-experience bridge creation...")

        # Clear existing data
        context.log.info("🧹 Clearing existing job-experience bridge data...")
        cursor.execute(f"DELETE FROM {table_name}")

        # Create bridges for general experience (years-based)
        context.log.info("📈 Creating general experience mappings...")
        general_mappings_sql = f"""
        INSERT INTO {table_name} (
            BRIDGE_ID, JOB_UID, EXPERIENCE_ID, EXPERIENCE_SOURCE,
            EXPERIENCE_VALUE_NUMERIC, EXPERIENCE_VALUE_TEXT, TECHNOLOGY_CONTEXT,
            IS_MINIMUM_REQUIREMENT, EXTRACTION_CONFIDENCE, PROCESSING_METHOD
        )
        SELECT
            CONCAT('bridge_', ere.JOB_UID, '_', en.EXPERIENCE_ID, '_', ere.EXPERIENCE_SOURCE) as BRIDGE_ID,
            ere.JOB_UID,
            en.EXPERIENCE_ID,
            CONCAT('llm_', ere.EXPERIENCE_SOURCE) as EXPERIENCE_SOURCE,
            TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) as EXPERIENCE_VALUE_NUMERIC,
            CONCAT(ere.EXPERIENCE_VALUE::STRING, ' years') as EXPERIENCE_VALUE_TEXT,
            NULL as TECHNOLOGY_CONTEXT,
            CASE WHEN ere.EXPERIENCE_SOURCE = 'min_years' THEN TRUE ELSE FALSE END as IS_MINIMUM_REQUIREMENT,
            ere.EXTRACTION_CONFIDENCE,
            'llm_auto' as PROCESSING_METHOD
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION ere
        JOIN BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED en
          ON ere.EXPERIENCE_TYPE = 'general'
          AND en.EXPERIENCE_CATEGORY = 'general'
          AND TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) BETWEEN en.MIN_YEARS_REQUIRED AND en.MAX_YEARS_REQUIRED
        WHERE ere.EXPERIENCE_TYPE = 'general'
          AND TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) > 0
        """

        cursor.execute(general_mappings_sql)
        general_count = cursor.rowcount
        context.log.info(f"✅ Created {general_count} general experience mappings")

        # Create bridges for technology-specific experience
        context.log.info("💻 Creating technology-specific experience mappings...")
        tech_mappings_sql = f"""
        INSERT INTO {table_name} (
            BRIDGE_ID, JOB_UID, EXPERIENCE_ID, EXPERIENCE_SOURCE,
            EXPERIENCE_VALUE_NUMERIC, EXPERIENCE_VALUE_TEXT, TECHNOLOGY_CONTEXT,
            IS_MINIMUM_REQUIREMENT, EXTRACTION_CONFIDENCE, PROCESSING_METHOD
        )
        SELECT
            CONCAT('bridge_', ere.JOB_UID, '_', en.EXPERIENCE_ID, '_tech') as BRIDGE_ID,
            ere.JOB_UID,
            en.EXPERIENCE_ID,
            'llm_specific_tech' as EXPERIENCE_SOURCE,
            TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) as EXPERIENCE_VALUE_NUMERIC,
            CONCAT(ere.TECHNOLOGY_NAME, ': ', ere.EXPERIENCE_VALUE::STRING, ' years') as EXPERIENCE_VALUE_TEXT,
            ere.TECHNOLOGY_NAME as TECHNOLOGY_CONTEXT,
            TRUE as IS_MINIMUM_REQUIREMENT,  -- Tech-specific is typically minimum
            ere.EXTRACTION_CONFIDENCE,
            'llm_auto' as PROCESSING_METHOD
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION ere
        JOIN BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED en
          ON ere.EXPERIENCE_TYPE = 'technology_specific'
          AND en.EXPERIENCE_CATEGORY = 'technology_specific'
          AND UPPER(REGEXP_REPLACE(ere.TECHNOLOGY_NAME, '[^A-Za-z0-9]', '_')) =
              REGEXP_REPLACE(en.EXPERIENCE_ID, '^EXP_TECH_', '')
        WHERE ere.EXPERIENCE_TYPE = 'technology_specific'
          AND ere.TECHNOLOGY_NAME IS NOT NULL
          AND TRY_CAST(ere.EXPERIENCE_VALUE::STRING AS INTEGER) > 0
        """

        cursor.execute(tech_mappings_sql)
        tech_count = cursor.rowcount
        context.log.info(f"✅ Created {tech_count} technology-specific experience mappings")

        # Create bridges for role-level experience (text-based matching)
        context.log.info("👔 Creating role-level experience mappings...")
        role_mappings_sql = f"""
        INSERT INTO {table_name} (
            BRIDGE_ID, JOB_UID, EXPERIENCE_ID, EXPERIENCE_SOURCE,
            EXPERIENCE_VALUE_NUMERIC, EXPERIENCE_VALUE_TEXT, TECHNOLOGY_CONTEXT,
            IS_MINIMUM_REQUIREMENT, EXTRACTION_CONFIDENCE, PROCESSING_METHOD
        )
        SELECT
            CONCAT('bridge_', ere.JOB_UID, '_', en.EXPERIENCE_ID, '_level') as BRIDGE_ID,
            ere.JOB_UID,
            en.EXPERIENCE_ID,
            CONCAT('llm_', ere.EXPERIENCE_SOURCE) as EXPERIENCE_SOURCE,
            NULL as EXPERIENCE_VALUE_NUMERIC,  -- Role levels don't have numeric values
            ere.ORIGINAL_TEXT as EXPERIENCE_VALUE_TEXT,
            NULL as TECHNOLOGY_CONTEXT,
            TRUE as IS_MINIMUM_REQUIREMENT,  -- Role levels are typically minimum requirements
            ere.EXTRACTION_CONFIDENCE,
            'llm_auto' as PROCESSING_METHOD
        FROM BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION ere
        JOIN BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED en
          ON ere.EXPERIENCE_TYPE = 'role_level'
          AND (
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%entry%' AND en.EXPERIENCE_ID = 'EXP_ENTRY') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%junior%' AND en.EXPERIENCE_ID = 'EXP_JUNIOR') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%mid%' AND en.EXPERIENCE_ID = 'EXP_MID') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%senior%' AND en.EXPERIENCE_ID = 'EXP_SENIOR') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%lead%' AND en.EXPERIENCE_ID = 'EXP_LEAD') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%staff%' AND en.EXPERIENCE_ID = 'EXP_STAFF') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%principal%' AND en.EXPERIENCE_ID = 'EXP_PRINCIPAL') OR
              (LOWER(ere.ORIGINAL_TEXT) LIKE '%director%' AND en.EXPERIENCE_ID = 'EXP_DIRECTOR') OR
              (LOWER(ere.ORIGINAL_TEXT) REGEXP '(vp|vice.president)' AND en.EXPERIENCE_ID = 'EXP_VP') OR
              (LOWER(ere.ORIGINAL_TEXT) REGEXP '(executive|c[etfo]o)' AND en.EXPERIENCE_ID = 'EXP_EXECUTIVE')
          )
        WHERE ere.EXPERIENCE_TYPE = 'role_level'
        """

        cursor.execute(role_mappings_sql)
        role_count = cursor.rowcount
        context.log.info(f"✅ Created {role_count} role-level experience mappings")

        # Get bridge statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_relationships,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN EXPERIENCE_SOURCE LIKE '%min_years%' OR EXPERIENCE_SOURCE LIKE '%max_years%' THEN 1 END) as general_mappings,
            COUNT(CASE WHEN EXPERIENCE_SOURCE = 'llm_specific_tech' THEN 1 END) as tech_mappings,
            COUNT(CASE WHEN EXPERIENCE_SOURCE LIKE '%level%' OR EXPERIENCE_SOURCE LIKE '%seniority%' THEN 1 END) as role_mappings,
            AVG(EXTRACTION_CONFIDENCE) as avg_confidence,
            COUNT(CASE WHEN IS_MINIMUM_REQUIREMENT = TRUE THEN 1 END) as minimum_requirements,
            COUNT(CASE WHEN IS_MINIMUM_REQUIREMENT = FALSE THEN 1 END) as preferred_requirements
        FROM {table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "job_experience_relationships": result[0],
                "jobs_with_experience_requirements": result[1],
                "general_experience_mappings": result[2],
                "technology_specific_mappings": result[3],
                "seniority_level_mappings": result[4],
                "average_extraction_confidence": round(result[5] or 0, 3),
                "minimum_requirements": result[6],
                "preferred_requirements": result[7]
            })

        # Sample bridge data for validation
        cursor.execute(f"""
        SELECT
            jeb.EXPERIENCE_SOURCE,
            en.EXPERIENCE_NAME,
            jeb.EXPERIENCE_VALUE_TEXT,
            jeb.TECHNOLOGY_CONTEXT,
            jeb.IS_MINIMUM_REQUIREMENT,
            COUNT(*) as job_count
        FROM {table_name} jeb
        JOIN BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED en ON jeb.EXPERIENCE_ID = en.EXPERIENCE_ID
        GROUP BY jeb.EXPERIENCE_SOURCE, en.EXPERIENCE_NAME, jeb.EXPERIENCE_VALUE_TEXT,
                 jeb.TECHNOLOGY_CONTEXT, jeb.IS_MINIMUM_REQUIREMENT
        ORDER BY job_count DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["bridge_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Job-Experience Bridge Complete:
        • Total Relationships: {stats['job_experience_relationships']:,}
        • Jobs with Experience Requirements: {stats['jobs_with_experience_requirements']:,}
        • General Experience Mappings: {stats['general_experience_mappings']:,}
        • Technology-Specific Mappings: {stats['technology_specific_mappings']:,}
        • Role-Level Mappings: {stats['seniority_level_mappings']:,}
        • Minimum Requirements: {stats.get('minimum_requirements', 0):,}
        • Preferred Requirements: {stats.get('preferred_requirements', 0):,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "job_experience_relationships": MetadataValue.int(stats["job_experience_relationships"]),
            "jobs_with_experience_requirements": MetadataValue.int(stats["jobs_with_experience_requirements"]),
            "general_experience_mappings": MetadataValue.int(stats["general_experience_mappings"]),
            "technology_specific_mappings": MetadataValue.int(stats["technology_specific_mappings"]),
            "seniority_level_mappings": MetadataValue.int(stats["seniority_level_mappings"]),
            "average_extraction_confidence": MetadataValue.float(stats.get("average_extraction_confidence", 0)),
            "bridge_sample": MetadataValue.json(stats.get("bridge_sample", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Job-experience bridge creation failed: {str(e)}")
        stats["bridge_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()