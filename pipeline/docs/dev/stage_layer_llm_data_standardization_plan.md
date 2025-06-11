# STAGE Layer LLM Data Standardization Plan

This document outlines the comprehensive plan for standardizing and normalizing the LLM-extracted VARIANT/JSON data in the STAGE layer of the BetterJobs-Snow pipeline. This is a critical follow-up to the main STAGE layer transformation that enables proper analytics and dimensional modeling in the ANALYTICS layer.

## Overview

The STAGE layer currently contains LLM-extracted data in VARIANT/JSON format that is difficult to query, aggregate, and analyze. This plan addresses the normalization of this data into proper relational structures while maintaining data quality and enabling downstream analytics.

## Problem Statement

### Current LLM Data Structure (Problematic)

The LLM enrichment process produces data in VARIANT columns that are not analytics-friendly:

```sql
-- Current STAGE.jobs_llm_enriched structure
SELECT
    job_uid,
    technical_skills,  -- {"languages": ["Python", "JavaScript"], "databases": ["PostgreSQL"], "cloud": ["AWS"], "frameworks": ["React"], "tools": ["Docker"]}
    soft_skills,       -- ["Communication", "Leadership", "Problem Solving", "Team Collaboration"]
    primary_keywords,  -- ["Full Stack", "API Development", "Microservices", "Cloud Native"]
    industry_keywords, -- ["FinTech", "B2B SaaS", "High Growth"]
    role_type_keywords -- ["Individual Contributor", "Senior Level", "Backend Focus"]
FROM STAGE.jobs_llm_enriched;
```

### Analytics Challenges

**Querying Difficulties**:
- ❌ Complex FLATTEN operations required for basic skill counts
- ❌ Slow performance on VARIANT column aggregations
- ❌ Difficult to join with other relational data
- ❌ No referential integrity or data validation

**Business Intelligence Limitations**:
- ❌ Cannot easily answer "How many jobs require Python?"
- ❌ Cannot calculate salary premiums by technology
- ❌ Cannot build skill trend analysis
- ❌ Cannot create normalized skill taxonomies

**Data Quality Issues**:
- ❌ No deduplication of skill variations ("Python" vs "python" vs "Python3")
- ❌ No standardization across job postings
- ❌ No validation of skill taxonomies
- ❌ No frequency analysis or trend tracking

## Solution Architecture

### Normalized Data Structure

Transform VARIANT data into properly normalized relational tables:

```
jobs_llm_enriched (existing)
    ↓
skill_extraction_and_normalization
    ↓
stage_skills_normalized (master skills)
stage_job_skills_bridge (many-to-many)
stage_keywords_normalized (master keywords)
stage_job_keywords_bridge (many-to-many)
```

## Implementation Plan

### Phase 1: Skills Normalization Infrastructure

#### 1.1 Create Skills Master Table

```sql
CREATE TABLE STAGE.skills_normalized (
    skill_id STRING PRIMARY KEY,
    skill_name STRING NOT NULL,                    -- Standardized skill name
    skill_name_clean STRING NOT NULL,              -- Cleaned version for matching
    skill_name_original STRING,                    -- Most common original variant

    -- Skill Classification
    skill_category STRING NOT NULL,                -- languages, databases, cloud, frameworks, tools, soft
    skill_subcategory STRING,                      -- backend_language, nosql_database, public_cloud, etc.
    skill_family STRING,                           -- development, data, devops, etc.
    skill_type STRING DEFAULT 'technical',         -- technical, soft, business, certification

    -- Standardization & Deduplication
    original_variants VARIANT,                     -- JSON array of all variations found
    common_aliases VARIANT,                        -- JSON array of known aliases
    canonical_form STRING,                         -- Preferred canonical name

    -- Market Data
    frequency_count INTEGER DEFAULT 0,             -- How often this skill appears
    first_seen_date DATE,                          -- When first detected
    last_seen_date DATE,                           -- Most recent occurrence
    trend_direction STRING,                        -- rising, stable, declining

    -- Quality & Confidence
    confidence_score FLOAT DEFAULT 1.0,            -- Confidence in standardization
    manual_review_flag BOOLEAN DEFAULT FALSE,      -- Needs human review
    approved_by_admin BOOLEAN DEFAULT FALSE,       -- Admin approved

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    created_by STRING DEFAULT 'system',

    -- Indexing
    INDEX idx_category_name (skill_category, skill_name),
    INDEX idx_frequency (frequency_count DESC),
    INDEX idx_trend (trend_direction, frequency_count DESC)
) CLUSTER BY (skill_category, skill_name);
```

#### 1.2 Create Job-Skills Bridge Table

```sql
CREATE TABLE STAGE.job_skills_bridge (
    bridge_id STRING PRIMARY KEY,
    job_uid STRING NOT NULL,                       -- FK to jobs_unified/jobs_llm_enriched
    skill_id STRING NOT NULL,                      -- FK to skills_normalized

    -- Source Information
    skill_source STRING NOT NULL,                  -- 'technical_skills', 'soft_skills', 'primary_keywords'
    skill_category STRING NOT NULL,                -- Denormalized for performance
    original_text STRING,                          -- Original text from LLM

    -- Confidence & Quality
    extraction_confidence FLOAT,                   -- LLM extraction confidence
    standardization_confidence FLOAT,              -- Skill matching confidence
    overall_confidence FLOAT,                      -- Combined confidence score

    -- Context
    skill_context STRING,                          -- required, preferred, nice-to-have
    experience_level_context STRING,               -- entry, mid, senior (if mentioned)

    -- Processing Metadata
    processing_method STRING DEFAULT 'llm_auto',   -- llm_auto, manual_override, admin_correction
    needs_review BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    created_by STRING DEFAULT 'system',

    -- Foreign Keys
    FOREIGN KEY (job_uid) REFERENCES STAGE.jobs_unified(job_uid),
    FOREIGN KEY (skill_id) REFERENCES STAGE.skills_normalized(skill_id),

    -- Indexing
    INDEX idx_job_skill (job_uid, skill_id),
    INDEX idx_skill_jobs (skill_id, job_uid),
    INDEX idx_category_confidence (skill_category, overall_confidence DESC),
    INDEX idx_source_category (skill_source, skill_category)
) CLUSTER BY (job_uid, skill_category);
```

### Phase 2: Keywords and Classification Normalization

#### 2.1 Create Keywords Master Table

```sql
CREATE TABLE STAGE.keywords_normalized (
    keyword_id STRING PRIMARY KEY,
    keyword_text STRING NOT NULL,
    keyword_text_clean STRING NOT NULL,

    -- Classification
    keyword_type STRING NOT NULL,                  -- primary, industry, role_type, company_stage, technology
    keyword_category STRING,                       -- specific category within type

    -- Standardization
    original_variants VARIANT,                     -- All variations found
    canonical_form STRING,                         -- Standardized form

    -- Market Data
    frequency_count INTEGER DEFAULT 0,
    trend_score FLOAT DEFAULT 0.0,                 -- Trending indicator

    -- Quality
    confidence_score FLOAT DEFAULT 1.0,
    approved_by_admin BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_type_frequency (keyword_type, frequency_count DESC),
    INDEX idx_category_trend (keyword_category, trend_score DESC)
) CLUSTER BY (keyword_type, keyword_text);
```

#### 2.2 Create Job-Keywords Bridge Table

```sql
CREATE TABLE STAGE.job_keywords_bridge (
    bridge_id STRING PRIMARY KEY,
    job_uid STRING NOT NULL,
    keyword_id STRING NOT NULL,

    -- Source Information
    keyword_source STRING NOT NULL,                -- primary_keywords, industry_keywords, role_type_keywords
    original_text STRING,

    -- Confidence
    extraction_confidence FLOAT,
    standardization_confidence FLOAT,
    overall_confidence FLOAT,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    FOREIGN KEY (job_uid) REFERENCES STAGE.jobs_unified(job_uid),
    FOREIGN KEY (keyword_id) REFERENCES STAGE.keywords_normalized(keyword_id),

    INDEX idx_job_keyword (job_uid, keyword_id),
    INDEX idx_keyword_jobs (keyword_id, job_uid)
) CLUSTER BY (job_uid, keyword_source);
```

### Phase 3: Data Extraction and Normalization Process

#### 3.1 Skills Extraction Logic

```sql
-- Step 1: Extract all skills from VARIANT columns
CREATE OR REPLACE VIEW stage_skills_raw_extraction AS
WITH technical_skills_exploded AS (
    -- Extract technical skills by category
    SELECT
        job_uid,
        'technical_skills' as skill_source,
        skill_category.key::STRING as skill_category,
        TRIM(LOWER(skill_name.value::STRING)) as skill_name_raw,
        skill_name.value::STRING as skill_name_original
    FROM STAGE.jobs_llm_enriched,
    LATERAL FLATTEN(input => technical_skills) skill_category,
    LATERAL FLATTEN(input => skill_category.value) skill_name
    WHERE technical_skills IS NOT NULL
      AND skill_category.value IS NOT NULL
      AND ARRAY_SIZE(skill_category.value) > 0
),

soft_skills_exploded AS (
    -- Extract soft skills
    SELECT
        job_uid,
        'soft_skills' as skill_source,
        'soft' as skill_category,
        TRIM(LOWER(skill.value::STRING)) as skill_name_raw,
        skill.value::STRING as skill_name_original
    FROM STAGE.jobs_llm_enriched,
    LATERAL FLATTEN(input => soft_skills) skill
    WHERE soft_skills IS NOT NULL
      AND skill.value IS NOT NULL
      AND LENGTH(TRIM(skill.value::STRING)) > 0
),

primary_keywords_exploded AS (
    -- Extract primary keywords as skills
    SELECT
        job_uid,
        'primary_keywords' as skill_source,
        'keyword' as skill_category,
        TRIM(LOWER(keyword.value::STRING)) as skill_name_raw,
        keyword.value::STRING as skill_name_original
    FROM STAGE.jobs_llm_enriched,
    LATERAL FLATTEN(input => primary_keywords) keyword
    WHERE primary_keywords IS NOT NULL
      AND keyword.value IS NOT NULL
      AND LENGTH(TRIM(keyword.value::STRING)) > 0
)

SELECT * FROM technical_skills_exploded
UNION ALL
SELECT * FROM soft_skills_exploded
UNION ALL
SELECT * FROM primary_keywords_exploded;
```

#### 3.2 Skill Standardization Rules

```sql
-- Step 2: Create skill standardization mapping
CREATE OR REPLACE TABLE STAGE.skill_standardization_rules (
    rule_id STRING PRIMARY KEY,
    pattern STRING,                                 -- Pattern to match (regex or exact)
    standardized_name STRING,                       -- Standard form
    skill_category STRING,                          -- Correct category
    skill_subcategory STRING,                       -- Correct subcategory
    confidence_score FLOAT DEFAULT 1.0,
    rule_type STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern, fuzzy_match
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Example standardization rules
INSERT INTO STAGE.skill_standardization_rules VALUES
('rule_001', 'python', 'Python', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_002', 'python3', 'Python', 'languages', 'backend_language', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_003', 'py', 'Python', 'languages', 'backend_language', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_004', 'javascript', 'JavaScript', 'languages', 'frontend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_005', 'js', 'JavaScript', 'languages', 'frontend_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_006', 'react.js', 'React', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_007', 'reactjs', 'React', 'frameworks', 'frontend_framework', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_008', 'postgresql', 'PostgreSQL', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_009', 'postgres', 'PostgreSQL', 'databases', 'relational_database', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_010', 'aws', 'Amazon Web Services', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP);
```

#### 3.3 Automated Standardization Process

```sql
-- Step 3: Apply standardization rules and populate normalized tables
CREATE OR REPLACE PROCEDURE standardize_and_populate_skills()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    DELETE FROM STAGE.job_skills_bridge;
    DELETE FROM STAGE.skills_normalized;

    -- Populate skills_normalized with standardized skills
    INSERT INTO STAGE.skills_normalized (
        skill_id,
        skill_name,
        skill_name_clean,
        skill_name_original,
        skill_category,
        skill_subcategory,
        original_variants,
        frequency_count,
        first_seen_date,
        last_seen_date,
        confidence_score
    )
    WITH skill_aggregation AS (
        SELECT
            COALESCE(sr.standardized_name, sre.skill_name_original) as skill_name,
            COALESCE(sr.skill_category, sre.skill_category) as skill_category,
            COALESCE(sr.skill_subcategory, 'uncategorized') as skill_subcategory,
            ARRAY_AGG(DISTINCT sre.skill_name_original) as original_variants,
            COUNT(*) as frequency_count,
            MIN(ju.date_retrieved::DATE) as first_seen_date,
            MAX(ju.date_retrieved::DATE) as last_seen_date,
            AVG(COALESCE(sr.confidence_score, 0.5)) as confidence_score
        FROM stage_skills_raw_extraction sre
        LEFT JOIN STAGE.skill_standardization_rules sr
            ON LOWER(sre.skill_name_raw) = LOWER(sr.pattern)
        JOIN STAGE.jobs_unified ju ON sre.job_uid = ju.job_uid
        WHERE LENGTH(sre.skill_name_raw) >= 2  -- Filter out single characters
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 2  -- Only include skills appearing at least twice
    )
    SELECT
        CONCAT('skill_', ROW_NUMBER() OVER (ORDER BY frequency_count DESC)) as skill_id,
        skill_name,
        LOWER(TRIM(skill_name)) as skill_name_clean,
        original_variants[0]::STRING as skill_name_original,
        skill_category,
        skill_subcategory,
        original_variants,
        frequency_count,
        first_seen_date,
        last_seen_date,
        confidence_score,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'system'
    FROM skill_aggregation;

    -- Populate job_skills_bridge
    INSERT INTO STAGE.job_skills_bridge (
        bridge_id,
        job_uid,
        skill_id,
        skill_source,
        skill_category,
        original_text,
        standardization_confidence,
        overall_confidence
    )
    SELECT
        CONCAT('bridge_', job_uid, '_', skill_id, '_', ROW_NUMBER() OVER ()) as bridge_id,
        sre.job_uid,
        sn.skill_id,
        sre.skill_source,
        sn.skill_category,
        sre.skill_name_original,
        CASE
            WHEN sr.standardized_name IS NOT NULL THEN sr.confidence_score
            ELSE 0.5
        END as standardization_confidence,
        CASE
            WHEN sr.standardized_name IS NOT NULL THEN sr.confidence_score
            ELSE 0.5
        END as overall_confidence,
        CURRENT_TIMESTAMP,
        'system'
    FROM stage_skills_raw_extraction sre
    JOIN STAGE.skills_normalized sn
        ON (COALESCE(sr.standardized_name, sre.skill_name_original) = sn.skill_name)
    LEFT JOIN STAGE.skill_standardization_rules sr
        ON LOWER(sre.skill_name_raw) = LOWER(sr.pattern);

    RETURN 'Skills standardization completed successfully';
END;
$$;
```

### Phase 4: Data Quality and Validation

#### 4.1 Quality Monitoring Views

```sql
-- Skills quality monitoring
CREATE VIEW stage_skills_quality_report AS
SELECT
    skill_category,
    COUNT(*) as total_skills,
    COUNT(CASE WHEN confidence_score >= 0.8 THEN 1 END) as high_confidence_skills,
    COUNT(CASE WHEN confidence_score < 0.5 THEN 1 END) as low_confidence_skills,
    COUNT(CASE WHEN manual_review_flag THEN 1 END) as needs_review,
    AVG(confidence_score) as avg_confidence,
    AVG(frequency_count) as avg_frequency
FROM STAGE.skills_normalized
GROUP BY skill_category
ORDER BY total_skills DESC;

-- Job skills coverage analysis
CREATE VIEW stage_job_skills_coverage AS
SELECT
    COUNT(DISTINCT ju.job_uid) as total_jobs,
    COUNT(DISTINCT jsb.job_uid) as jobs_with_skills,
    ROUND((COUNT(DISTINCT jsb.job_uid)::FLOAT / COUNT(DISTINCT ju.job_uid)) * 100, 2) as coverage_percentage,
    AVG(skills_per_job.skill_count) as avg_skills_per_job
FROM STAGE.jobs_unified ju
LEFT JOIN STAGE.job_skills_bridge jsb ON ju.job_uid = jsb.job_uid
LEFT JOIN (
    SELECT job_uid, COUNT(*) as skill_count
    FROM STAGE.job_skills_bridge
    GROUP BY job_uid
) skills_per_job ON ju.job_uid = skills_per_job.job_uid;
```

#### 4.2 Data Validation Rules

```sql
-- Validation checks
CREATE OR REPLACE VIEW stage_data_validation_report AS
WITH validation_checks AS (
    -- Check 1: Skills without jobs
    SELECT 'orphaned_skills' as check_name, COUNT(*) as issue_count
    FROM STAGE.skills_normalized sn
    LEFT JOIN STAGE.job_skills_bridge jsb ON sn.skill_id = jsb.skill_id
    WHERE jsb.skill_id IS NULL

    UNION ALL

    -- Check 2: Jobs without skills
    SELECT 'jobs_without_skills' as check_name, COUNT(*) as issue_count
    FROM STAGE.jobs_unified ju
    LEFT JOIN STAGE.job_skills_bridge jsb ON ju.job_uid = jsb.job_uid
    WHERE jsb.job_uid IS NULL

    UNION ALL

    -- Check 3: Low confidence standardizations
    SELECT 'low_confidence_skills' as check_name, COUNT(*) as issue_count
    FROM STAGE.skills_normalized
    WHERE confidence_score < 0.5

    UNION ALL

    -- Check 4: Duplicate skill names
    SELECT 'duplicate_skill_names' as check_name, COUNT(*) - COUNT(DISTINCT skill_name) as issue_count
    FROM STAGE.skills_normalized
)
SELECT * FROM validation_checks
WHERE issue_count > 0;
```

### Phase 5: Performance Optimization

#### 5.1 Indexing Strategy

```sql
-- Optimize skills table for analytics queries
CREATE INDEX IF NOT EXISTS idx_skills_category_frequency
ON STAGE.skills_normalized (skill_category, frequency_count DESC);

CREATE INDEX IF NOT EXISTS idx_skills_trend_confidence
ON STAGE.skills_normalized (trend_direction, confidence_score DESC);

-- Optimize bridge table for join performance
CREATE INDEX IF NOT EXISTS idx_bridge_job_category
ON STAGE.job_skills_bridge (job_uid, skill_category);

CREATE INDEX IF NOT EXISTS idx_bridge_skill_confidence
ON STAGE.job_skills_bridge (skill_id, overall_confidence DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_bridge_category_source_confidence
ON STAGE.job_skills_bridge (skill_category, skill_source, overall_confidence DESC);
```

#### 5.2 Materialized Views for Analytics

```sql
-- Pre-computed skill popularity by job family
CREATE MATERIALIZED VIEW stage_skill_popularity_by_job_family AS
SELECT
    jf.job_family,
    jf.seniority_level,
    sn.skill_category,
    sn.skill_name,
    COUNT(DISTINCT jsb.job_uid) as job_count,
    COUNT(DISTINCT jsb.job_uid)::FLOAT /
        COUNT(DISTINCT ju.job_uid) OVER (PARTITION BY jf.job_family, jf.seniority_level) as penetration_rate,
    AVG(ju.salary_midpoint) as avg_salary_with_skill,
    CURRENT_TIMESTAMP as refreshed_at
FROM STAGE.job_skills_bridge jsb
JOIN STAGE.skills_normalized sn ON jsb.skill_id = sn.skill_id
JOIN STAGE.jobs_unified ju ON jsb.job_uid = ju.job_uid
-- Note: Assuming job_family classification exists or can be derived
LEFT JOIN (
    SELECT job_uid,
           CASE
               WHEN CONTAINS(LOWER(job_title_clean), 'engineer') THEN 'Engineering'
               WHEN CONTAINS(LOWER(job_title_clean), 'data') THEN 'Data'
               WHEN CONTAINS(LOWER(job_title_clean), 'product') THEN 'Product'
               ELSE 'Other'
           END as job_family,
           CASE
               WHEN CONTAINS(LOWER(job_title_clean), 'senior') THEN 'Senior'
               WHEN CONTAINS(LOWER(job_title_clean), 'lead') THEN 'Senior'
               WHEN CONTAINS(LOWER(job_title_clean), 'staff') THEN 'Senior'
               WHEN CONTAINS(LOWER(job_title_clean), 'junior') THEN 'Entry'
               ELSE 'Mid'
           END as seniority_level
    FROM STAGE.jobs_unified
) jf ON ju.job_uid = jf.job_uid
WHERE jsb.overall_confidence >= 0.6
GROUP BY jf.job_family, jf.seniority_level, sn.skill_category, sn.skill_name
HAVING job_count >= 5;  -- Minimum threshold for statistical relevance
```

## Implementation Timeline

### Week 1: Infrastructure Setup
- ✅ Create all normalized tables and indexes
- ✅ Implement basic extraction views
- ✅ Set up initial standardization rules

### Week 2: Data Processing Pipeline
- ✅ Implement and test standardization procedures
- ✅ Run initial data normalization
- ✅ Create quality monitoring views

### Week 3: Validation and Quality Assurance
- ✅ Validate data quality and completeness
- ✅ Manual review of low-confidence skills
- ✅ Refine standardization rules

### Week 4: Performance Optimization
- ✅ Implement performance indexes
- ✅ Create materialized views
- ✅ Optimize for analytics queries

## Success Criteria

### Data Quality Metrics
- **Coverage**: >95% of jobs have at least one skill extracted
- **Confidence**: >80% of skills have confidence score ≥0.8
- **Standardization**: <5% of skills require manual review
- **Performance**: Skills queries respond in <2 seconds

### Business Value Metrics
- **Analytics Enablement**: Dimensional model can leverage normalized skills
- **Query Performance**: 10x improvement in skill-based aggregations
- **Data Consistency**: Standardized skill taxonomies across all platforms
- **Trend Analysis**: Historical skill demand tracking enabled

### Technical Metrics
- **Data Integrity**: 100% referential integrity maintained
- **Processing Speed**: Full re-normalization completes in <30 minutes
- **Storage Efficiency**: Normalized structure reduces storage requirements
- **Maintenance**: Automated daily updates with minimal manual intervention

## Post-Implementation: Analytics Layer Integration

Once the LLM data standardization is complete, the ANALYTICS layer can leverage:

```sql
-- Example: Now possible analytics queries
SELECT
    sn.skill_name,
    COUNT(DISTINCT jsb.job_uid) as job_demand,
    AVG(ju.salary_midpoint) as avg_salary_premium
FROM STAGE.skills_normalized sn
JOIN STAGE.job_skills_bridge jsb ON sn.skill_id = jsb.skill_id
JOIN STAGE.jobs_unified ju ON jsb.job_uid = ju.job_uid
WHERE sn.skill_category = 'languages'
  AND ju.date_posted >= CURRENT_DATE - 90
GROUP BY sn.skill_name
ORDER BY job_demand DESC;
```

This standardization enables all the HIGH priority analytics features:
- ✅ Skills demand trend analysis
- ✅ Technology popularity tracking
- ✅ Salary premiums by skill
- ✅ Company technology preferences
- ✅ Skills-based market intelligence

The ANALYTICS layer can now focus purely on dimensional modeling and business intelligence, with clean, normalized skill data as input.