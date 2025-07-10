from typing import Dict, Any
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult
from dagster_snowflake import SnowflakeResource
import os
from datetime import datetime

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


@asset(
    deps=[
        "analytics_fact_job_postings",
        "analytics_dim_company",
        "analytics_dim_location",
        "analytics_dim_job_description",
        "analytics_job_skills_bridge",
        "analytics_dim_skills",
        "analytics_job_keywords_bridge",
        "analytics_dim_keywords",
    ],
    description="Denormalised job postings table for UI/API search (flattened skills & keywords). Incremental MERGE against SERVE.DENORM_JOB_POSTINGS.",
    group_name="4_serve_layer",
    kinds={"snowflake", "SQL"},
)

def serve_denorm_job_postings(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Builds/updates the SERVE.DENORM_JOB_POSTINGS table from Analytics layer.

    The asset joins fact and dimension tables to produce a flattened record per job posting with
    aggregated lists of skills and keywords. Uses MERGE for incremental upsert and deletes
    obsolete job_uids that disappeared from the analytics fact.
    """

    table_name = ensure_object_exists("tables/serve_denorm_job_postings.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        try:
            context.log.info("Preparing denormalised job postings snapshot …")

            cur.execute(
                """
                CREATE OR REPLACE TEMPORARY TABLE denorm_job_postings_current AS
                WITH base AS (
                    SELECT
                        fp.JOB_UID,
                        dp.PLATFORM_NAME            AS PLATFORM,
                        fp.JOB_POSTING_KEY          AS JOB_ID,
                        dc.COMPANY_ID,
                        dc.COMPANY_NAME,
                        fp.JOB_TITLE                AS JOB_TITLE,
                        dj.DESCRIPTION_CLEAN        AS JOB_DESCRIPTION,
                        fp.POSTING_URL              AS JOB_URL,
                        fp.FIRST_POSTED_DATE        AS DATE_POSTED,
                        fp.DATE_RETRIEVED,
                        fp.IS_ACTIVE_POSTING        AS IS_ACTIVE,
                        fp.DATA_QUALITY_SCORE,
                        fp.UPDATED_TIMESTAMP        AS TRANSFORMATION_TIMESTAMP
                    FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fp
                    JOIN BETTERJOBS_DB.ANALYTICS.DIM_COMPANY dc        ON fp.COMPANY_KEY = dc.COMPANY_KEY
                    JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_DESCRIPTION dj ON dj.JOB_UID    = fp.JOB_UID
                    LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_PLATFORM dp ON dp.PLATFORM_KEY = fp.PLATFORM_KEY
                ),
                enriched AS (
                    SELECT
                        jle.JOB_UID,
                        jle.SALARY_MIN                 AS ENRICHED_SALARY_MIN,
                        jle.SALARY_MAX                 AS ENRICHED_SALARY_MAX,
                        jle.SALARY_CURRENCY            AS ENRICHED_SALARY_CURRENCY,
                        jle.SALARY_PERIOD              AS ENRICHED_SALARY_PERIOD,
                        jle.SALARY_TYPE                AS ENRICHED_SALARY_TYPE,
                        jle.MIN_YEARS_EXPERIENCE       AS ENRICHED_MIN_YEARS_EXPERIENCE,
                        jle.MAX_YEARS_EXPERIENCE       AS ENRICHED_MAX_YEARS_EXPERIENCE,
                        jle.EXPERIENCE_LEVEL           AS ENRICHED_EXPERIENCE_LEVEL,
                        jle.PRIMARY_KEYWORDS           AS ENRICHED_PRIMARY_KEYWORDS,
                        jle.INDUSTRY_KEYWORDS          AS ENRICHED_INDUSTRY_KEYWORDS,
                        jle.WORK_TYPE                  AS ENRICHED_WORK_TYPE,
                        jle.OFFICE_LOCATIONS           AS ENRICHED_OFFICE_LOCATIONS,
                        jle.ROLE_TYPE                  AS ENRICHED_ROLE_TYPE,
                        jle.TEAM_SIZE                  AS ENRICHED_TEAM_SIZE,
                        jle.LLM_OVERALL_CONFIDENCE     AS ENRICHED_OVERALL_CONFIDENCE,
                        jle.SALARY_CONFIDENCE          AS ENRICHED_SALARY_CONFIDENCE,
                        jle.EXPERIENCE_CONFIDENCE      AS ENRICHED_EXPERIENCE_CONFIDENCE,
                        jle.SKILLS_CONFIDENCE          AS ENRICHED_SKILLS_CONFIDENCE,
                        jle.WORK_ARRANGEMENT_CONFIDENCE AS ENRICHED_WORK_ARRANGEMENT_CONFIDENCE,
                        jle.CLASSIFICATION_CONFIDENCE  AS ENRICHED_CLASSIFICATION_CONFIDENCE
                    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle
                ),
                skills AS (
                    SELECT
                        fp.JOB_UID,
                        LISTAGG(DISTINCT ds.SKILL_NAME, ', ') AS SKILLS_CSV,
                        LISTAGG(DISTINCT CASE WHEN ds.SKILL_TYPE = 'technical' THEN ds.SKILL_NAME END, ', ') AS TECHNICAL_SKILLS_CSV,
                        LISTAGG(DISTINCT CASE WHEN ds.SKILL_TYPE = 'soft' THEN ds.SKILL_NAME END, ', ')       AS SOFT_SKILLS_CSV
                    FROM BETTERJOBS_DB.ANALYTICS.JOB_SKILLS_BRIDGE jsb
                    JOIN BETTERJOBS_DB.ANALYTICS.DIM_SKILLS ds ON ds.SKILL_KEY = jsb.SKILL_KEY
                    JOIN BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fp ON fp.JOB_POSTING_KEY = jsb.JOB_POSTING_KEY
                    GROUP BY fp.JOB_UID
                ),
                keywords AS (
                    SELECT
                        fp.JOB_UID,
                        LISTAGG(DISTINCT dk.KEYWORD_TEXT_CLEAN, ', ') AS KEYWORDS_CSV
                    FROM BETTERJOBS_DB.ANALYTICS.JOB_KEYWORDS_BRIDGE jkb
                    JOIN BETTERJOBS_DB.ANALYTICS.DIM_KEYWORDS dk ON dk.KEYWORD_KEY = jkb.KEYWORD_KEY
                    JOIN BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fp ON fp.JOB_POSTING_KEY = jkb.JOB_POSTING_KEY
                    GROUP BY fp.JOB_UID
                )
                SELECT
                    b.JOB_UID,
                    b.JOB_ID,
                    b.PLATFORM,
                    b.COMPANY_ID,
                    b.COMPANY_NAME,
                    b.JOB_TITLE,
                    b.JOB_DESCRIPTION,
                    b.JOB_URL,
                    b.DATE_POSTED,
                    b.DATE_RETRIEVED,
                    b.IS_ACTIVE,
                    b.DATA_QUALITY_SCORE,
                    b.TRANSFORMATION_TIMESTAMP,
                    e.ENRICHED_SALARY_MIN,
                    e.ENRICHED_SALARY_MAX,
                    e.ENRICHED_SALARY_CURRENCY,
                    e.ENRICHED_SALARY_PERIOD,
                    e.ENRICHED_SALARY_TYPE,
                    e.ENRICHED_MIN_YEARS_EXPERIENCE,
                    e.ENRICHED_MAX_YEARS_EXPERIENCE,
                    e.ENRICHED_EXPERIENCE_LEVEL,
                    sk.TECHNICAL_SKILLS_CSV,
                    sk.SOFT_SKILLS_CSV,
                    e.ENRICHED_PRIMARY_KEYWORDS,
                    e.ENRICHED_INDUSTRY_KEYWORDS,
                    e.ENRICHED_WORK_TYPE,
                    e.ENRICHED_OFFICE_LOCATIONS,
                    e.ENRICHED_ROLE_TYPE,
                    e.ENRICHED_TEAM_SIZE,
                    e.ENRICHED_OVERALL_CONFIDENCE,
                    e.ENRICHED_SALARY_CONFIDENCE,
                    e.ENRICHED_EXPERIENCE_CONFIDENCE,
                    e.ENRICHED_SKILLS_CONFIDENCE,
                    e.ENRICHED_WORK_ARRANGEMENT_CONFIDENCE,
                    e.ENRICHED_CLASSIFICATION_CONFIDENCE,
                    COALESCE(sk.SKILLS_CSV, '')   AS SKILLS_CSV,
                    COALESCE(kw.KEYWORDS_CSV, '') AS KEYWORDS_CSV,
                    CURRENT_TIMESTAMP             AS UPDATED_TIMESTAMP,
                    DATE_TRUNC('MONTH', b.DATE_POSTED) AS PARTITION_DATE
                FROM base b
                LEFT JOIN enriched e ON e.JOB_UID = b.JOB_UID
                LEFT JOIN skills sk   ON sk.JOB_UID = b.JOB_UID
                LEFT JOIN keywords kw ON kw.JOB_UID = b.JOB_UID
                """
            )

            # MERGE incremental changes
            context.log.info("Merging snapshot into SERVE.DENORM_JOB_POSTINGS …")
            merge_sql = f"""
            MERGE INTO {table_name} AS tgt
            USING denorm_job_postings_current AS src
            ON tgt.JOB_UID = src.JOB_UID

            WHEN MATCHED AND (
                   tgt.JOB_TITLE      <> src.JOB_TITLE OR
                   tgt.JOB_DESCRIPTION<> src.JOB_DESCRIPTION OR
                   tgt.DATE_POSTED    <> src.DATE_POSTED OR
                   tgt.IS_ACTIVE      <> src.IS_ACTIVE OR
                   tgt.SKILLS_CSV     <> src.SKILLS_CSV OR
                   tgt.KEYWORDS_CSV   <> src.KEYWORDS_CSV
            ) THEN
                UPDATE SET
                    JOB_ID         = src.JOB_ID,
                    PLATFORM       = src.PLATFORM,
                    COMPANY_ID     = src.COMPANY_ID,
                    COMPANY_NAME   = src.COMPANY_NAME,
                    JOB_TITLE      = src.JOB_TITLE,
                    JOB_DESCRIPTION= src.JOB_DESCRIPTION,
                    JOB_URL        = src.JOB_URL,
                    DATE_POSTED    = src.DATE_POSTED,
                    DATE_RETRIEVED = src.DATE_RETRIEVED,
                    IS_ACTIVE      = src.IS_ACTIVE,
                    DATA_QUALITY_SCORE = src.DATA_QUALITY_SCORE,
                    TRANSFORMATION_TIMESTAMP = src.TRANSFORMATION_TIMESTAMP,
                    ENRICHED_SALARY_MIN = src.ENRICHED_SALARY_MIN,
                    ENRICHED_SALARY_MAX = src.ENRICHED_SALARY_MAX,
                    ENRICHED_SALARY_CURRENCY = src.ENRICHED_SALARY_CURRENCY,
                    ENRICHED_SALARY_PERIOD = src.ENRICHED_SALARY_PERIOD,
                    ENRICHED_SALARY_TYPE = src.ENRICHED_SALARY_TYPE,
                    ENRICHED_MIN_YEARS_EXPERIENCE = src.ENRICHED_MIN_YEARS_EXPERIENCE,
                    ENRICHED_MAX_YEARS_EXPERIENCE = src.ENRICHED_MAX_YEARS_EXPERIENCE,
                    ENRICHED_EXPERIENCE_LEVEL = src.ENRICHED_EXPERIENCE_LEVEL,
                    TECHNICAL_SKILLS_CSV = src.TECHNICAL_SKILLS_CSV,
                    SOFT_SKILLS_CSV = src.SOFT_SKILLS_CSV,
                    ENRICHED_PRIMARY_KEYWORDS = src.ENRICHED_PRIMARY_KEYWORDS,
                    ENRICHED_INDUSTRY_KEYWORDS = src.ENRICHED_INDUSTRY_KEYWORDS,
                    ENRICHED_WORK_TYPE = src.ENRICHED_WORK_TYPE,
                    ENRICHED_OFFICE_LOCATIONS = src.ENRICHED_OFFICE_LOCATIONS,
                    ENRICHED_ROLE_TYPE = src.ENRICHED_ROLE_TYPE,
                    ENRICHED_TEAM_SIZE = src.ENRICHED_TEAM_SIZE,
                    ENRICHED_OVERALL_CONFIDENCE = src.ENRICHED_OVERALL_CONFIDENCE,
                    ENRICHED_SALARY_CONFIDENCE = src.ENRICHED_SALARY_CONFIDENCE,
                    ENRICHED_EXPERIENCE_CONFIDENCE = src.ENRICHED_EXPERIENCE_CONFIDENCE,
                    ENRICHED_SKILLS_CONFIDENCE = src.ENRICHED_SKILLS_CONFIDENCE,
                    ENRICHED_WORK_ARRANGEMENT_CONFIDENCE = src.ENRICHED_WORK_ARRANGEMENT_CONFIDENCE,
                    ENRICHED_CLASSIFICATION_CONFIDENCE = src.ENRICHED_CLASSIFICATION_CONFIDENCE,
                    SKILLS_CSV     = src.SKILLS_CSV,
                    KEYWORDS_CSV   = src.KEYWORDS_CSV,
                    UPDATED_TIMESTAMP = src.UPDATED_TIMESTAMP,
                    PARTITION_DATE = src.PARTITION_DATE

            WHEN NOT MATCHED THEN
                INSERT (
                    JOB_UID, JOB_ID, PLATFORM, COMPANY_ID, COMPANY_NAME, JOB_TITLE,
                    JOB_DESCRIPTION, JOB_URL, DATE_POSTED, DATE_RETRIEVED, IS_ACTIVE,
                    DATA_QUALITY_SCORE, TRANSFORMATION_TIMESTAMP,
                    ENRICHED_SALARY_MIN, ENRICHED_SALARY_MAX, ENRICHED_SALARY_CURRENCY, ENRICHED_SALARY_PERIOD, ENRICHED_SALARY_TYPE,
                    ENRICHED_MIN_YEARS_EXPERIENCE, ENRICHED_MAX_YEARS_EXPERIENCE, ENRICHED_EXPERIENCE_LEVEL,
                    TECHNICAL_SKILLS_CSV, SOFT_SKILLS_CSV, ENRICHED_PRIMARY_KEYWORDS, ENRICHED_INDUSTRY_KEYWORDS,
                    ENRICHED_WORK_TYPE, ENRICHED_OFFICE_LOCATIONS, ENRICHED_ROLE_TYPE, ENRICHED_TEAM_SIZE,
                    ENRICHED_OVERALL_CONFIDENCE, ENRICHED_SALARY_CONFIDENCE, ENRICHED_EXPERIENCE_CONFIDENCE,
                    ENRICHED_SKILLS_CONFIDENCE, ENRICHED_WORK_ARRANGEMENT_CONFIDENCE, ENRICHED_CLASSIFICATION_CONFIDENCE,
                    SKILLS_CSV, KEYWORDS_CSV, UPDATED_TIMESTAMP, PARTITION_DATE
                ) VALUES (
                    src.JOB_UID, src.JOB_ID, src.PLATFORM, src.COMPANY_ID, src.COMPANY_NAME, src.JOB_TITLE,
                    src.JOB_DESCRIPTION, src.JOB_URL, src.DATE_POSTED, src.DATE_RETRIEVED, src.IS_ACTIVE,
                    src.DATA_QUALITY_SCORE, src.TRANSFORMATION_TIMESTAMP,
                    src.ENRICHED_SALARY_MIN, src.ENRICHED_SALARY_MAX, src.ENRICHED_SALARY_CURRENCY, src.ENRICHED_SALARY_PERIOD, src.ENRICHED_SALARY_TYPE,
                    src.ENRICHED_MIN_YEARS_EXPERIENCE, src.ENRICHED_MAX_YEARS_EXPERIENCE, src.ENRICHED_EXPERIENCE_LEVEL,
                    src.TECHNICAL_SKILLS_CSV, src.SOFT_SKILLS_CSV, src.ENRICHED_PRIMARY_KEYWORDS, src.ENRICHED_INDUSTRY_KEYWORDS,
                    src.ENRICHED_WORK_TYPE, src.ENRICHED_OFFICE_LOCATIONS, src.ENRICHED_ROLE_TYPE, src.ENRICHED_TEAM_SIZE,
                    src.ENRICHED_OVERALL_CONFIDENCE, src.ENRICHED_SALARY_CONFIDENCE, src.ENRICHED_EXPERIENCE_CONFIDENCE,
                    src.ENRICHED_SKILLS_CONFIDENCE, src.ENRICHED_WORK_ARRANGEMENT_CONFIDENCE, src.ENRICHED_CLASSIFICATION_CONFIDENCE,
                    src.SKILLS_CSV, src.KEYWORDS_CSV, src.UPDATED_TIMESTAMP, src.PARTITION_DATE
                );
            """
            cur.execute(merge_sql)
            upserts = cur.rowcount

            # Delete obsolete job_uids
            context.log.info("Pruning obsolete job_uids …")
            cur.execute(
                f"""
                DELETE FROM {table_name}
                WHERE JOB_UID NOT IN (SELECT JOB_UID FROM denorm_job_postings_current)
                """
            )
            deletes = cur.rowcount

            # Stats
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cur.fetchone()[0]

            context.add_output_metadata({
                "rows_upserted": MetadataValue.int(upserts),
                "rows_deleted": MetadataValue.int(deletes),
                "total_rows": MetadataValue.int(total_rows)
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_upserted": upserts,
                "rows_deleted": deletes,
                "total_rows": total_rows
            }

        finally:
            cur.close()

