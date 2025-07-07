from typing import Dict, Any
from dagster import asset, AssetExecutionContext, MetadataValue
from dagster_snowflake import SnowflakeResource

from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["analytics_dim_skills"],
    description="Denormalised skills lookup table for UI autocomplete/search (category, subcategory, skills_csv). Incremental MERGE against SERVE.DENORM_SKILLS.",
    group_name="4_serve_layer",
    kinds={"snowflake", "SQL"},

)
def serve_denorm_skills(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Builds/updates the SERVE.DENORM_SKILLS table from ANALYTICS.DIM_SKILLS.

    The asset aggregates distinct skill names per (category, subcategory), produces a comma-separated
    list (`skills_csv`) and merges the results into the SERVE layer table so that UI components can
    fetch autocomplete options quickly. The MERGE only updates rows when the aggregated CSV changed
    (detected via string comparison) and inserts new category/subcategory groups, providing an
    incremental update pattern without full reloads.
    """

    table_name = ensure_object_exists("tables/serve_denorm_skills.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        try:
            context.log.info("Aggregating skills from ANALYTICS.DIM_SKILLS …")

            # Create/replace temp table with current aggregated state
            cur.execute(
                f"""
                CREATE OR REPLACE TEMPORARY TABLE denorm_skills_current AS
                SELECT
                    SKILL_CATEGORY,
                    SKILL_SUBCATEGORY,
                    LISTAGG(DISTINCT SKILL_NAME, ', ') WITHIN GROUP (ORDER BY SKILL_NAME) AS SKILLS_CSV,
                    COUNT(DISTINCT SKILL_NAME) AS SKILL_COUNT
                FROM BETTERJOBS_DB.ANALYTICS.DIM_SKILLS
                GROUP BY SKILL_CATEGORY, SKILL_SUBCATEGORY
                """
            )

            # MERGE incremental changes into SERVE table
            context.log.info("Merging aggregated results into SERVE.DENORM_SKILLS …")
            merge_sql = f"""
            MERGE INTO {table_name} AS tgt
            USING (
                SELECT
                    SKILL_CATEGORY,
                    SKILL_SUBCATEGORY,
                    SKILLS_CSV,
                    SKILL_COUNT,
                    CURRENT_TIMESTAMP AS UPDATED_AT
                FROM denorm_skills_current
            ) AS src
            ON tgt.SKILL_CATEGORY = src.SKILL_CATEGORY AND tgt.SKILL_SUBCATEGORY = src.SKILL_SUBCATEGORY

            WHEN MATCHED AND tgt.SKILLS_CSV <> src.SKILLS_CSV THEN
                UPDATE SET
                    SKILLS_CSV    = src.SKILLS_CSV,
                    SKILL_COUNT   = src.SKILL_COUNT,
                    UPDATED_AT    = src.UPDATED_AT

            WHEN NOT MATCHED THEN
                INSERT (SKILL_CATEGORY, SKILL_SUBCATEGORY, SKILLS_CSV, SKILL_COUNT, UPDATED_AT)
                VALUES (src.SKILL_CATEGORY, src.SKILL_SUBCATEGORY, src.SKILLS_CSV, src.SKILL_COUNT, src.UPDATED_AT);
            """
            cur.execute(merge_sql)
            affected_rows = cur.rowcount

            # Optionally prune categories that disappeared (rare). Delete rows not in current snapshot.
            context.log.info("Pruning obsolete category/subcategory combinations …")
            cur.execute(
                f"""
                DELETE FROM {table_name}
                WHERE (SKILL_CATEGORY, SKILL_SUBCATEGORY) NOT IN (
                    SELECT SKILL_CATEGORY, SKILL_SUBCATEGORY FROM denorm_skills_current
                )
                """
            )
            deleted_rows = cur.rowcount

            # Stats for metadata
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cur.fetchone()[0]

            context.add_output_metadata({
                "rows_upserted": MetadataValue.int(affected_rows),
                "rows_deleted": MetadataValue.int(deleted_rows),
                "total_rows": MetadataValue.int(total_rows)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_upserted": affected_rows,
                "rows_deleted": deleted_rows,
                "total_rows": total_rows
            }

        finally:
            cur.close()


@asset(
    deps=["analytics_dim_keywords"],
    description="Denormalised keywords lookup table for UI autocomplete/search (flat list). Incremental MERGE against SERVE.DENORM_KEYWORDS.",
    group_name="4_serve_layer",
    kinds={"snowflake", "SQL"},
)

def serve_denorm_keywords(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Builds/updates the SERVE.DENORM_KEYWORDS table from ANALYTICS.DIM_KEYWORDS.

    The asset extracts the distinct set of keywords with their keyword_type from the analytics
    dimension and merges them incrementally into the SERVE layer table. Only new keywords or
    those whose type changed are upserted, and obsolete keywords are pruned to keep the table
    fully in sync without a full reload.
    """

    table_name = ensure_object_exists("tables/serve_denorm_keywords.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        try:
            context.log.info("Aggregating keywords from ANALYTICS.DIM_KEYWORDS …")

            # Current snapshot of distinct keywords
            cur.execute(
                """
                CREATE OR REPLACE TEMPORARY TABLE denorm_keywords_current AS
                SELECT
                    LOWER(TRIM(KEYWORD_TEXT)) AS KEYWORD,
                    MIN(KEYWORD_TYPE) AS KEYWORD_TYPE
                FROM BETTERJOBS_DB.ANALYTICS.DIM_KEYWORDS
                WHERE KEYWORD_TEXT IS NOT NULL AND TRIM(KEYWORD_TEXT) <> ''
                GROUP BY LOWER(TRIM(KEYWORD_TEXT))
                """
            )

            # MERGE incremental changes into SERVE table
            context.log.info("Merging aggregated results into SERVE.DENORM_KEYWORDS …")
            merge_sql = f"""
            MERGE INTO {table_name} AS tgt
            USING (
                SELECT
                    KEYWORD,
                    KEYWORD_TYPE,
                    CURRENT_TIMESTAMP AS UPDATED_AT
                FROM denorm_keywords_current
            ) AS src
            ON tgt.KEYWORD = src.KEYWORD

            WHEN MATCHED AND tgt.KEYWORD_TYPE <> src.KEYWORD_TYPE THEN
                UPDATE SET
                    KEYWORD_TYPE = src.KEYWORD_TYPE,
                    UPDATED_AT   = src.UPDATED_AT

            WHEN NOT MATCHED THEN
                INSERT (KEYWORD, KEYWORD_TYPE, UPDATED_AT)
                VALUES (src.KEYWORD, src.KEYWORD_TYPE, src.UPDATED_AT);
            """
            cur.execute(merge_sql)
            affected_rows = cur.rowcount

            # Remove keywords that no longer exist
            context.log.info("Pruning obsolete keywords …")
            cur.execute(
                f"""
                DELETE FROM {table_name}
                WHERE KEYWORD NOT IN (
                    SELECT KEYWORD FROM denorm_keywords_current
                )
                """
            )
            deleted_rows = cur.rowcount

            # Stats for metadata
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cur.fetchone()[0]

            context.add_output_metadata({
                "rows_upserted": MetadataValue.int(affected_rows),
                "rows_deleted": MetadataValue.int(deleted_rows),
                "total_rows": MetadataValue.int(total_rows)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_upserted": affected_rows,
                "rows_deleted": deleted_rows,
                "total_rows": total_rows
            }

        finally:
            cur.close()