-- Rollback: Remove PARTITION_KEY column from JOBS_LLM_ENRICHED table
-- Date: 2025-06-28
-- Purpose: Rollback partitioned LLM enrichment changes if needed (ENHANCEMENT-031)

-- Step 1: Remove comment from column (for clean rollback)
COMMENT ON COLUMN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED.PARTITION_KEY IS NULL;

-- Step 2: Drop the PARTITION_KEY column
-- Note: This will remove all partition tracking data
ALTER TABLE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
DROP COLUMN PARTITION_KEY;

-- Step 3: Verify rollback
DESCRIBE TABLE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED;