-- Migration: Add PARTITION_KEY column to JOBS_LLM_ENRICHED table
-- Date: 2025-06-28
-- Purpose: Support partitioned LLM enrichment processing (ENHANCEMENT-031)

-- Step 1: Add PARTITION_KEY column to existing table
ALTER TABLE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
ADD COLUMN PARTITION_KEY VARCHAR(16777216);

-- Step 2: Backfill existing records with partition keys based on company name
-- This derives the partition key from the company name using the same logic as discovery assets
UPDATE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED llm
SET PARTITION_KEY = (
    CASE
        -- Handle numeric company names (0-9)
        WHEN SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9' THEN '0-9'

        -- Handle alphabetical company names (A-Z, case insensitive)
        WHEN UPPER(SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1)) BETWEEN 'A' AND 'Z' THEN UPPER(SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1))

        -- Handle special characters and other cases
        ELSE 'other'
    END
)
FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED j
WHERE llm.JOB_UID = j.JOB_UID
AND llm.PARTITION_KEY IS NULL;

-- Step 3: Verify backfill results
SELECT
    PARTITION_KEY,
    COUNT(*) as record_count,
    MIN(LLM_PROCESSING_TIMESTAMP) as earliest_processed,
    MAX(LLM_PROCESSING_TIMESTAMP) as latest_processed
FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
WHERE PARTITION_KEY IS NOT NULL
GROUP BY PARTITION_KEY
ORDER BY PARTITION_KEY;

-- Step 4: Check for any remaining NULL partition keys (should be 0)
SELECT COUNT(*) as null_partition_keys
FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
WHERE PARTITION_KEY IS NULL;

-- Step 5: Add comment to document the migration
COMMENT ON COLUMN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED.PARTITION_KEY IS 'Partition key (A-Z, 0-9, other) used for partitioned LLM processing. Added 2025-06-28 for ENHANCEMENT-031.';