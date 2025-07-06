-- Fix missing COMPANY_NAME_CLEAN values in STAGE.JOBS_UNIFIED
-- Joins STAGE.JOBS_UNIFIED with RAW.MASTER_COMPANY_URLS on COMPANY_ID
-- and fills in COMPANY_NAME_CLEAN where it is NULL or empty.
--
-- Run this script in Snowflake as a one-off data correction task.
-- Safe to re-run; only rows with missing/blank COMPANY_NAME_CLEAN
-- are updated.

MERGE INTO BETTERJOBS_DB.STAGE.JOBS_UNIFIED AS target
USING (
    SELECT COMPANY_ID,
           TRIM(COMPANY_NAME) AS COMPANY_NAME_CLEAN_SRC
    FROM   BETTERJOBS_DB.RAW.MASTER_COMPANY_URLS
    WHERE  COMPANY_NAME IS NOT NULL
      AND  TRIM(COMPANY_NAME) <> ''
) AS source
ON target.COMPANY_ID = source.COMPANY_ID
WHEN MATCHED AND (target.COMPANY_NAME_CLEAN IS NULL OR TRIM(target.COMPANY_NAME_CLEAN) = '')
    THEN UPDATE SET
        COMPANY_NAME_CLEAN = source.COMPANY_NAME_CLEAN_SRC,
        TRANSFORMATION_TIMESTAMP = CURRENT_TIMESTAMP();

-- Optional: report how many rows were affected
-- SELECT ROW_COUNT() AS rows_updated;