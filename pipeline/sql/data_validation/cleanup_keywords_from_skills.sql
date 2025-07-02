-- ENHANCEMENT-034: Cleanup Keywords from Skills Pipeline
-- Remove any existing keyword relationships from skills pipeline tables
-- This script should be run after implementing the enhancement

-- Remove any existing keyword relationships from bridge table
DELETE FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
WHERE SKILL_SOURCE = 'primary_keywords';

-- Remove any keyword skills from normalized table
DELETE FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
WHERE SKILL_CATEGORY = 'keyword';

-- Verification queries
-- Verify no keywords remain in skills pipeline
SELECT COUNT(*) as remaining_keywords
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
WHERE SKILL_SOURCE = 'primary_keywords';

-- Verify skills pipeline only contains actual skills
SELECT SKILL_SOURCE, COUNT(*) as count
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
GROUP BY SKILL_SOURCE
ORDER BY count DESC;

-- Verify keywords are properly handled in keyword pipeline
SELECT COUNT(*) as keyword_relationships
FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE;