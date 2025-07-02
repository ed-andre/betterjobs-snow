"""
Skills Consolidation Asset for ENHANCEMENT-035

Dedicated asset for applying intelligent consolidation to standardized skills.
Separated from skills normalization to follow Single Responsibility Principle.

Dependencies:
- stage_skills_normalized: Source of standardized skills data

Output Tables:
- SKILLS_CONSOLIDATED: Consolidated skills with variant merging and metadata
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
from dagster_betterjobs.utils.skill_consolidation import (
    SkillData,
    ConsolidationConfig,
    consolidate_skill_variants,
    get_consolidation_summary
)
from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["stage_skills_normalized"],
    description="Apply intelligent consolidation to standardized skills using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_consolidated(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply intelligent consolidation to standardized skills.

    ENHANCEMENT-035: Dedicated asset for consolidation logic only.
    Takes standardized skills and applies variant consolidation.

    Features:
    - Singular/plural form consolidation
    - Abbreviation and alias handling
    - Frequency-weighted merging
    - Confidence score preservation
    - Consolidation audit trail

    Processing:
    - Load standardized skills from SKILLS_NORMALIZED
    - Apply consolidation using skill_consolidation.py utilities
    - Create SKILLS_CONSOLIDATED table with metadata
    - Generate consolidation statistics and examples
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "consolidation_timestamp": datetime.now().isoformat(),
        "original_skills_loaded": 0,
        "consolidated_skills_created": 0,
        "skills_merged": 0,
        "consolidation_ratio": 0.0,
        "high_frequency_consolidations": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔄 Starting dedicated skills consolidation process...")

        # 🔧 SCHEMA-AS-CODE: Ensure consolidated skills table exists using canonical SQL file
        consolidated_table_name = ensure_object_exists("tables/stage_skills_consolidated.sql", snowflake, context)
        context.log.info(f"✅ Skills consolidated table ready: {consolidated_table_name}")

        # Get source table names
        skills_normalized_table = ensure_object_exists("tables/stage_skills_normalized.sql", snowflake, context)

        # Clear existing data for fresh consolidation
        cursor.execute(f"DELETE FROM {consolidated_table_name}")
        context.log.info("🗑️ Cleared existing consolidated skills data")

        # Step 1: Load standardized skills for consolidation
        context.log.info("📊 Loading standardized skills for consolidation...")

        load_skills_sql = f"""
        SELECT
            SKILL_ID,
            SKILL_NAME,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_FAMILY,
            SKILL_TYPE,
            ORIGINAL_VARIANTS,
            FREQUENCY_COUNT,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            CONFIDENCE_SCORE,
            TREND_DIRECTION,
            MANUAL_REVIEW_FLAG
        FROM {skills_normalized_table}
        WHERE FREQUENCY_COUNT >= 1  -- Include all skills for consolidation consideration
        ORDER BY FREQUENCY_COUNT DESC
        """

        cursor.execute(load_skills_sql)
        normalized_results = cursor.fetchall()

        if not normalized_results:
            context.log.warning("⚠️ No standardized skills found for consolidation")
            return stats

        # Convert to SkillData objects for consolidation
        original_skills = {}
        for row in normalized_results:
            skill_id = row[0]
            skill_name = row[1]

            # Parse original variants if they exist
            original_variants = []
            if row[6]:  # ORIGINAL_VARIANTS
                try:
                    if isinstance(row[6], str):
                        original_variants = json.loads(row[6])
                    else:
                        original_variants = row[6]
                except (json.JSONDecodeError, TypeError):
                    original_variants = [skill_name]
            else:
                original_variants = [skill_name]

            skill_data = SkillData(
                skill_name=skill_name,
                skill_category=row[2] or 'uncategorized',
                skill_subcategory=row[3] or 'uncategorized',
                frequency_count=row[7] or 0,
                confidence_score=float(row[10]) if row[10] else 0.5,
                original_variants=original_variants,
                first_seen_date=str(row[8]) if row[8] else None,
                last_seen_date=str(row[9]) if row[9] else None
            )

            # Store with skill_id as key for tracking original sources
            original_skills[skill_id] = skill_data

        stats["original_skills_loaded"] = len(original_skills)
        context.log.info(f"📊 Loaded {len(original_skills)} standardized skills for consolidation")

        # Step 2: Apply consolidation with enhanced configuration
        consolidation_config = ConsolidationConfig(
            enabled=True,
            preferred_form="singular",  # Prefer singular forms
            min_frequency_threshold=2   # Only consolidate skills appearing 2+ times
        )

        # Convert to name-based mapping for consolidation utility
        skills_by_name = {skill_data.skill_name: skill_data for skill_data in original_skills.values()}
        consolidated_skills = consolidate_skill_variants(skills_by_name, consolidation_config)

        # Step 3: Generate consolidation summary and statistics
        consolidation_summary = get_consolidation_summary(skills_by_name, consolidated_skills)

        stats.update({
            "consolidated_skills_created": len(consolidated_skills),
            "skills_merged": consolidation_summary['skills_merged'],
            "consolidation_ratio": consolidation_summary['consolidation_ratio'],
            "merge_examples": consolidation_summary['merge_examples']
        })

        context.log.info(f"""
        ✅ Skills Consolidation Analysis Complete:
        • Original Skills: {stats['original_skills_loaded']:,}
        • Consolidated Skills: {stats['consolidated_skills_created']:,}
        • Skills Merged: {stats['skills_merged']:,}
        • Consolidation Ratio: {stats['consolidation_ratio']:.2%}
        """)

        # Step 4: Prepare consolidated data for database insertion
        context.log.info("🔄 Preparing consolidated skills for database insertion...")

        # Create temporary staging table for bulk insert
        cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE SKILLS_CONSOLIDATED_STAGING (
            CONSOLIDATED_SKILL_ID STRING,
            CANONICAL_SKILL_NAME STRING,
            SKILL_CATEGORY STRING,
            SKILL_SUBCATEGORY STRING,
            SKILL_FAMILY STRING,
            SKILL_TYPE STRING,
            ORIGINAL_SKILL_IDS_JSON STRING,
            ORIGINAL_SKILL_NAMES_JSON STRING,
            CONSOLIDATION_METHOD STRING,
            TOTAL_FREQUENCY_COUNT INTEGER,
            CONSOLIDATED_CONFIDENCE_SCORE FLOAT,
            FIRST_SEEN_DATE DATE,
            LAST_SEEN_DATE DATE,
            TREND_DIRECTION STRING,
            MANUAL_REVIEW_FLAG BOOLEAN
        )
        """)

        # Prepare staging data with consolidation metadata
        staging_data = []
        for canonical_name, consolidated_skill in consolidated_skills.items():
            # Generate unique consolidated skill ID
            consolidated_skill_id = f"consolidated_skill_{hash(canonical_name) % 100000:05d}"

            # Determine consolidation method
            original_variant_count = len(consolidated_skill.original_variants)
            if original_variant_count > 1:
                # Check for plural/singular patterns
                has_plural_singular = any(
                    s.lower().endswith('s') for s in consolidated_skill.original_variants
                ) and any(
                    not s.lower().endswith('s') for s in consolidated_skill.original_variants
                )
                consolidation_method = "singular_plural" if has_plural_singular else "variant_merge"
            else:
                consolidation_method = "none"

            # Find original skill IDs that contributed to this consolidated skill
            contributing_skill_ids = []
            for skill_id, skill_data in original_skills.items():
                if skill_data.skill_name in consolidated_skill.original_variants:
                    contributing_skill_ids.append(skill_id)

            staging_record = (
                consolidated_skill_id,
                canonical_name,
                consolidated_skill.skill_category,
                consolidated_skill.skill_subcategory,
                'general',  # Default skill family
                'technical' if consolidated_skill.skill_category not in ['soft'] else 'soft',
                json.dumps(contributing_skill_ids),  # Original skill IDs as JSON
                json.dumps(consolidated_skill.original_variants),  # Original skill names as JSON
                consolidation_method,
                consolidated_skill.frequency_count,
                consolidated_skill.confidence_score,
                consolidated_skill.first_seen_date,
                consolidated_skill.last_seen_date,
                'stable',  # Default trend direction
                consolidated_skill.confidence_score < 0.5  # Manual review flag for low confidence
            )
            staging_data.append(staging_record)

        # Bulk insert into staging table
        staging_insert_sql = """
        INSERT INTO SKILLS_CONSOLIDATED_STAGING VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(staging_insert_sql, staging_data)
        context.log.info(f"✅ Bulk inserted {len(staging_data)} consolidated skills into staging table")

        # Step 5: Insert into final table with VARIANT conversion
        final_insert_sql = f"""
        INSERT INTO {consolidated_table_name} (
            CONSOLIDATED_SKILL_ID,
            CANONICAL_SKILL_NAME,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_FAMILY,
            SKILL_TYPE,
            ORIGINAL_SKILL_IDS,
            ORIGINAL_SKILL_NAMES,
            CONSOLIDATION_METHOD,
            TOTAL_FREQUENCY_COUNT,
            CONSOLIDATED_CONFIDENCE_SCORE,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            TREND_DIRECTION,
            MANUAL_REVIEW_FLAG
        )
        SELECT
            CONSOLIDATED_SKILL_ID,
            CANONICAL_SKILL_NAME,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_FAMILY,
            SKILL_TYPE,
            PARSE_JSON(ORIGINAL_SKILL_IDS_JSON) as ORIGINAL_SKILL_IDS,
            PARSE_JSON(ORIGINAL_SKILL_NAMES_JSON) as ORIGINAL_SKILL_NAMES,
            CONSOLIDATION_METHOD,
            TOTAL_FREQUENCY_COUNT,
            CONSOLIDATED_CONFIDENCE_SCORE,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            TREND_DIRECTION,
            MANUAL_REVIEW_FLAG
        FROM SKILLS_CONSOLIDATED_STAGING
        """

        cursor.execute(final_insert_sql)
        context.log.info("✅ Successfully inserted all consolidated skills into final table")

        # Step 6: Get final consolidation statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_consolidated,
            COUNT(CASE WHEN CONSOLIDATION_METHOD != 'none' THEN 1 END) as actually_consolidated,
            COUNT(CASE WHEN CONSOLIDATED_CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN TOTAL_FREQUENCY_COUNT >= 100 THEN 1 END) as high_frequency,
            AVG(CONSOLIDATED_CONFIDENCE_SCORE) as avg_confidence,
            AVG(TOTAL_FREQUENCY_COUNT) as avg_frequency
        FROM {consolidated_table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "total_consolidated": result[0],
                "actually_consolidated": result[1],
                "high_confidence_consolidated": result[2],
                "high_frequency_consolidations": result[3],
                "avg_consolidated_confidence": result[4],
                "avg_consolidated_frequency": result[5]
            })

        # Get consolidation method breakdown
        cursor.execute(f"""
        SELECT CONSOLIDATION_METHOD, COUNT(*) as count
        FROM {consolidated_table_name}
        GROUP BY CONSOLIDATION_METHOD
        ORDER BY count DESC
        """)

        method_results = cursor.fetchall()
        if method_results:
            columns = [desc[0] for desc in cursor.description]
            stats["consolidation_method_breakdown"] = [dict(zip(columns, row)) for row in method_results]

        context.log.info(f"""
        🎯 Skills Consolidation Complete (Schema-as-Code):
        • Total Consolidated Skills: {stats.get('total_consolidated', 0):,}
        • Actually Consolidated: {stats.get('actually_consolidated', 0):,}
        • High Confidence: {stats.get('high_confidence_consolidated', 0):,}
        • High Frequency Consolidations: {stats.get('high_frequency_consolidations', 0):,}
        • Average Confidence: {stats.get('avg_consolidated_confidence', 0):.3f}
        • Consolidation Table: {consolidated_table_name}
        """)

        # Add comprehensive metadata for Dagster UI
        metadata = {
            "original_skills_loaded": MetadataValue.int(stats["original_skills_loaded"]),
            "consolidated_skills_created": MetadataValue.int(stats["consolidated_skills_created"]),
            "skills_merged": MetadataValue.int(stats["skills_merged"]),
            "consolidation_ratio": MetadataValue.float(stats["consolidation_ratio"]),
            "high_frequency_consolidations": MetadataValue.int(stats.get("high_frequency_consolidations", 0)),
            "avg_consolidated_confidence": MetadataValue.float(stats.get("avg_consolidated_confidence", 0)),
            "consolidation_method_breakdown": MetadataValue.json(stats.get("consolidation_method_breakdown", [])),
            "merge_examples": MetadataValue.json(stats.get("merge_examples", [])),
            "schema_as_code": MetadataValue.bool(True),
            "consolidated_table_name": MetadataValue.text(consolidated_table_name)
        }

        context.add_output_metadata(metadata)

        return stats

    except Exception as e:
        context.log.error(f"❌ Skills consolidation failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()