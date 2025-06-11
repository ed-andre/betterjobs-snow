# STAGE Schema Setup for Silver Layer Transformations

This document provides instructions for setting up the STAGE schema and `jobs_unified` table in Snowflake for the Silver layer transformations.

## Overview

The STAGE schema serves as the Silver layer in our medallion architecture, providing:
- Unified job data from all ATS platforms (Workday, Greenhouse, BambooHR, SmartRecruiters)
- Cleaned and standardized text fields
- Language detection and filtering
- AI-powered data extraction (salary, skills, keywords)
- **Normalized skills and keywords data** for analytics-ready querying
- Data quality validation and scoring
- Platform-specific data preservation via VARIANT columns

**Implementation Phases**:
- **Phase 1-2**: Basic unified job data and LLM enrichment (`jobs_unified`, `jobs_llm_enriched`)
- **Phase 3**: LLM data normalization for analytics readiness (`SKILLS_NORMALIZED`, bridge tables)
- **Phase 4+**: Analytics Layer preparation and dimensional modeling

## Prerequisites

- Snowflake environment with RAW schema already configured
- `BETTERJOBS_DB` database created
- Appropriate permissions for schema and table creation

## Schema Creation

### 1. Create STAGE Schema

```sql
-- Connect to the database
USE DATABASE BETTERJOBS_DB;

-- Create STAGE schema
CREATE SCHEMA IF NOT EXISTS STAGE;

-- Grant permissions to role (adjust role name as needed)
GRANT USAGE ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT CREATE TABLE ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
```

### 2. Create Main Unified Jobs Table

```sql
-- Switch to STAGE schema
USE SCHEMA STAGE;

-- Create the unified jobs table (Phase 1 - Basic Transformation with UID)
CREATE TABLE IF NOT EXISTS jobs_unified (
    -- Generated unique identifier (replaces composite primary key)
    -- Increased from 16 to 32 characters to prevent hash collisions
    job_uid STRING(32) PRIMARY KEY,

    -- Core identifiers
    job_id STRING,
    company_id STRING,
    platform STRING,  -- 'workday', 'greenhouse', 'bamboohr', 'smartrecruiters'

    -- Standardized core fields
    job_title_clean STRING,
    job_description_clean STRING,
    company_name_clean STRING,
    location_standardized STRING,
    job_url STRING,

    -- Date fields
    date_posted DATE,
    date_retrieved TIMESTAMP_NTZ,

    -- Status and classification
    is_active BOOLEAN DEFAULT TRUE,
    employment_status STRING,  -- Full-time, Part-time, Contract, Internship
    department STRING,

    -- Language detection
    detected_language STRING,
    language_confidence FLOAT,
    is_english BOOLEAN,
    language_detection_method STRING,

    -- Platform-specific data (preserved as JSON)
    platform_specific_data VARIANT,

    -- Quality and metadata
    transformation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    data_quality_score FLOAT,

    -- Source tracking
    source_raw_table STRING,  -- Original table name for lineage
    raw_data VARIANT,  -- Compressed original raw data for reference

    -- Partitioning
    partition_date DATE
);
```

### 2.1 LLM Enrichment Schema (Phase 2 - Separate Table Approach)

**RECOMMENDED**: Create a separate `jobs_llm_enriched` table instead of altering `jobs_unified`.

**Benefits of Separate Table**:
- ✅ Keep `jobs_unified` schema stable for existing analytics
- ✅ Independent LLM processing without affecting base data
- ✅ Better performance for queries not needing LLM data
- ✅ Easier management of confidence scores and reprocessing
- ✅ Clean separation of base data vs AI-extracted data

```sql
-- Create LLM enrichment table (Phase 2)
CREATE TABLE IF NOT EXISTS jobs_llm_enriched (
    -- Foreign key to jobs_unified (match exact data type)
    job_uid STRING PRIMARY KEY,

    -- Salary information (from LLM)
    salary_min NUMBER,
    salary_max NUMBER,
    salary_currency STRING DEFAULT 'USD',
    salary_period STRING,  -- hourly, annually, monthly
    salary_type STRING,    -- base, total, contract
    equity_mentioned BOOLEAN DEFAULT FALSE,
    bonus_mentioned BOOLEAN DEFAULT FALSE,
    salary_confidence FLOAT,

    -- Experience requirements (from LLM)
    min_years_experience NUMBER,
    max_years_experience NUMBER,
    experience_level STRING,  -- Entry, Mid, Senior, Executive
    specific_technologies_years VARIANT, -- JSON object: {"Python": 3, "React": 2}
    education_requirements VARIANT,      -- JSON array: ["Bachelor's degree", "Master's preferred"]
    certifications VARIANT,              -- JSON array: ["AWS Certified", "PMP"]
    experience_confidence FLOAT,

    -- Technical skills (from LLM, consolidated as JSON objects)
    technical_skills VARIANT,            -- {"languages": ["Python", "JavaScript"], "databases": ["PostgreSQL"], "cloud": ["AWS", "Azure"], "frameworks": ["React", "Django"], "tools": ["Docker", "Git"]}
    soft_skills VARIANT,                 -- ["Communication", "Leadership", "Problem Solving", "Team Collaboration"]
    skills_confidence FLOAT,

    -- Work arrangement (from LLM)
    work_type STRING,                    -- Remote, Hybrid, On-site
    remote_flexibility STRING,           -- "3 days remote, 2 days office"
    travel_requirements STRING,          -- "10% travel required"
    office_locations VARIANT,            -- ["San Francisco, CA", "New York, NY"]
    timezone_requirements STRING,        -- "Pacific Time preferred"
    work_arrangement_confidence FLOAT,

    -- Job classification (from LLM)
    job_family STRING,                   -- Engineering, Data, Product, Sales, Marketing
    job_sub_family STRING,               -- Backend Engineering, Data Science, etc.
    seniority_level STRING,              -- Entry, Mid, Senior, Executive
    primary_keywords VARIANT,            -- ["Full Stack", "API Development", "Microservices"]
    industry_keywords VARIANT,           -- ["FinTech", "B2B SaaS", "High Growth"]
    role_type STRING,                    -- IC, Manager, Director, VP
    team_size STRING,                    -- "5-10 engineers"
    classification_confidence FLOAT,

    -- LLM processing metadata
    llm_processed BOOLEAN DEFAULT TRUE,
    llm_processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    llm_model_version STRING DEFAULT 'gemini-2.5-flash-preview-05-20',
    llm_overall_confidence FLOAT,
    llm_needs_manual_review BOOLEAN DEFAULT FALSE,

    -- Quality validation
    extraction_confidence_avg FLOAT,     -- Average of all confidence scores
    keyword_quality_score FLOAT,         -- SQL-based keyword validation score
    validation_status STRING DEFAULT 'pending', -- pending, validated, needs_review
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Add foreign key constraint after table creation (more reliable approach)
ALTER TABLE jobs_llm_enriched
ADD CONSTRAINT fk_jobs_llm_job_uid
FOREIGN KEY (job_uid) REFERENCES jobs_unified(job_uid);

-- Add clustering for performance
ALTER TABLE jobs_llm_enriched CLUSTER BY (llm_processing_timestamp);

-- Create index on confidence for quality queries
CREATE INDEX IF NOT EXISTS idx_llm_confidence
ON jobs_llm_enriched (llm_overall_confidence);

-- Create index on validation status
CREATE INDEX IF NOT EXISTS idx_validation_status
ON jobs_llm_enriched (validation_status);
```

### 3. Add Clustering and Indexes

```sql
-- Add clustering for optimal query performance
ALTER TABLE jobs_unified CLUSTER BY (platform, date_posted);

-- Add search optimization for text fields (optional, for production)
-- ALTER TABLE jobs_unified ADD SEARCH OPTIMIZATION;
```

### 4. Create Supporting Tables

```sql
-- Company profiles enrichment table
CREATE TABLE IF NOT EXISTS company_profiles (
    company_id STRING PRIMARY KEY,
    company_name_standardized STRING,
    company_industry_standardized STRING,
    company_size_category STRING,
    employee_count_range STRING,
    funding_stage STRING,
    headquarters_location STRING,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Location mapping table
CREATE TABLE IF NOT EXISTS location_mapping (
    location_raw STRING,
    location_standardized STRING,
    city STRING,
    state STRING,
    country STRING,
    metro_area STRING,
    cost_of_living_index FLOAT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Skills taxonomy table
CREATE TABLE IF NOT EXISTS skills_taxonomy (
    skill_name STRING PRIMARY KEY,
    skill_category STRING,  -- programming_language, database, cloud_platform, framework, tool
    skill_aliases VARIANT,  -- Array of alternative names
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Transformation logs for audit trail
CREATE TABLE IF NOT EXISTS transformation_logs (
    log_id STRING PRIMARY KEY,
    job_id STRING,
    transformation_step STRING,
    status STRING,  -- success, failed, skipped
    error_message STRING,
    processing_time_seconds FLOAT,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 5. LLM Data Standardization Tables

**Phase 3**: Create normalized tables for LLM-extracted VARIANT data to enable proper analytics and dimensional modeling. These tables transform the JSON/VARIANT skills and keywords data into relational structures.

```sql
-- Skills normalization master table
CREATE TABLE IF NOT EXISTS SKILLS_NORMALIZED (
    SKILL_ID STRING PRIMARY KEY,
    SKILL_NAME STRING NOT NULL,                    -- Standardized skill name
    SKILL_NAME_CLEAN STRING NOT NULL,              -- Cleaned version for matching
    SKILL_NAME_ORIGINAL STRING,                    -- Most common original variant

    -- Skill Classification
    SKILL_CATEGORY STRING NOT NULL,                -- languages, databases, cloud, frameworks, tools, soft
    SKILL_SUBCATEGORY STRING,                      -- backend_language, nosql_database, public_cloud, etc.
    SKILL_FAMILY STRING,                           -- development, data, devops, etc.
    SKILL_TYPE STRING DEFAULT 'technical',         -- technical, soft, business, certification

    -- Standardization & Deduplication
    ORIGINAL_VARIANTS VARIANT,                     -- JSON array of all variations found
    COMMON_ALIASES VARIANT,                        -- JSON array of known aliases
    CANONICAL_FORM STRING,                         -- Preferred canonical name

    -- Market Data
    FREQUENCY_COUNT INTEGER DEFAULT 0,             -- How often this skill appears
    FIRST_SEEN_DATE DATE,                          -- When first detected
    LAST_SEEN_DATE DATE,                           -- Most recent occurrence
    TREND_DIRECTION STRING,                        -- rising, stable, declining

    -- Quality & Confidence
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,            -- Confidence in standardization
    MANUAL_REVIEW_FLAG BOOLEAN DEFAULT FALSE,      -- Needs human review
    APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,       -- Admin approved

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (SKILL_CATEGORY, SKILL_NAME);

-- Optimize for performance (clustering is defined in table creation)
-- Note: Regular Snowflake tables use clustering keys instead of indexes
-- Search optimization can be added for text-based queries if needed:
-- ALTER TABLE SKILLS_NORMALIZED ADD SEARCH OPTIMIZATION;

-- Job-Skills bridge table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS JOB_SKILLS_BRIDGE (
    BRIDGE_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,                       -- FK to JOBS_UNIFIED
    SKILL_ID STRING NOT NULL,                      -- FK to SKILLS_NORMALIZED

    -- Source Information
    SKILL_SOURCE STRING NOT NULL,                  -- 'technical_skills', 'soft_skills', 'primary_keywords'
    SKILL_CATEGORY STRING NOT NULL,                -- Denormalized for performance
    ORIGINAL_TEXT STRING,                          -- Original text from LLM

    -- Confidence & Quality
    EXTRACTION_CONFIDENCE FLOAT,                   -- LLM extraction confidence
    STANDARDIZATION_CONFIDENCE FLOAT,              -- Skill matching confidence
    OVERALL_CONFIDENCE FLOAT,                      -- Combined confidence score

    -- Context
    SKILL_CONTEXT STRING,                          -- required, preferred, nice-to-have
    EXPERIENCE_LEVEL_CONTEXT STRING,               -- entry, mid, senior (if mentioned)

    -- Processing Metadata
    PROCESSING_METHOD STRING DEFAULT 'llm_auto',   -- llm_auto, manual_override, admin_correction
    NEEDS_REVIEW BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (JOB_UID, SKILL_CATEGORY);

-- Add foreign key constraints
ALTER TABLE JOB_SKILLS_BRIDGE
ADD CONSTRAINT FK_JOB_SKILLS_JOB_UID
FOREIGN KEY (JOB_UID) REFERENCES JOBS_UNIFIED(JOB_UID);

ALTER TABLE JOB_SKILLS_BRIDGE
ADD CONSTRAINT FK_JOB_SKILLS_SKILL_ID
FOREIGN KEY (SKILL_ID) REFERENCES SKILLS_NORMALIZED(SKILL_ID);

-- Performance optimization (clustering is defined in table creation)
-- Note: Regular Snowflake tables use clustering keys instead of indexes
-- Additional clustering can be added if needed:
-- ALTER TABLE JOB_SKILLS_BRIDGE CLUSTER BY (SKILL_ID, JOB_UID);

-- Skill family mapping table
 CREATE TABLE IF NOT EXISTS SKILL_FAMILY_MAPPING (
            MAPPING_ID STRING PRIMARY KEY,
            SKILL_CATEGORY STRING NOT NULL,             -- Category to map from
            SKILL_FAMILY STRING NOT NULL,               -- Family to map to
            FAMILY_DESCRIPTION STRING,                  -- Description of the family
            PRIORITY INTEGER DEFAULT 1,                 -- Priority for overlapping mappings
            IS_ACTIVE BOOLEAN DEFAULT TRUE,             -- Enable/disable mapping
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        ) CLUSTER BY (SKILL_CATEGORY, IS_ACTIVE)


-- Keywords normalization master table
CREATE TABLE IF NOT EXISTS KEYWORDS_NORMALIZED (
    KEYWORD_ID STRING PRIMARY KEY,
    KEYWORD_TEXT STRING NOT NULL,
    KEYWORD_TEXT_CLEAN STRING NOT NULL,

    -- Classification
    KEYWORD_TYPE STRING NOT NULL,                  -- primary, industry, role_type, company_stage, technology
    KEYWORD_CATEGORY STRING,                       -- specific category within type

    -- Standardization
    ORIGINAL_VARIANTS VARIANT,                     -- All variations found
    CANONICAL_FORM STRING,                         -- Standardized form

    -- Market Data
    FREQUENCY_COUNT INTEGER DEFAULT 0,
    TREND_SCORE FLOAT DEFAULT 0.0,                 -- Trending indicator

    -- Quality
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (KEYWORD_TYPE, KEYWORD_TEXT);

-- Locations normalization master table (Phase 3 - LLM Data Standardization)
CREATE TABLE IF NOT EXISTS LOCATIONS_NORMALIZED (
    LOCATION_ID STRING PRIMARY KEY,
    LOCATION_NAME STRING NOT NULL,                 -- Standardized location name
    LOCATION_NAME_CLEAN STRING NOT NULL,           -- Cleaned version for matching
    LOCATION_NAME_ORIGINAL STRING,                 -- Most common original variant

    -- Geographic Classification
    CITY STRING,
    STATE_PROVINCE STRING,
    COUNTRY STRING,
    METRO_AREA STRING,
    REGION STRING,                                 -- Northeast, West Coast, etc.

    -- Location Type
    LOCATION_TYPE STRING,                          -- office, headquarters, remote, hybrid
    IS_REMOTE_FRIENDLY BOOLEAN DEFAULT FALSE,      -- Supports remote work
    IS_MAJOR_TECH_HUB BOOLEAN DEFAULT FALSE,       -- Silicon Valley, Seattle, etc.

    -- Economic Data
    COST_OF_LIVING_INDEX FLOAT,
    AVERAGE_SALARY_ADJUSTMENT FLOAT,               -- Regional salary multiplier

    -- Standardization Metadata
    ORIGINAL_VARIANTS VARIANT,                     -- All variations found
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    FREQUENCY_COUNT INTEGER DEFAULT 0,
    FIRST_SEEN_DATE DATE,
    LAST_SEEN_DATE DATE,

    -- Quality & Confidence
    MANUAL_REVIEW_FLAG BOOLEAN DEFAULT FALSE,      -- Needs human review
    APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,       -- Admin approved

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (COUNTRY, STATE_PROVINCE, CITY);

-- Performance optimization (clustering is defined in table creation)
-- Note: Regular Snowflake tables use clustering keys instead of indexes
-- Search optimization can be added for text-based queries if needed:
-- ALTER TABLE KEYWORDS_NORMALIZED ADD SEARCH OPTIMIZATION;

-- Job-Keywords bridge table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS JOB_KEYWORDS_BRIDGE (
    BRIDGE_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,
    KEYWORD_ID STRING NOT NULL,

    -- Source Information
    KEYWORD_SOURCE STRING NOT NULL,                -- primary_keywords, industry_keywords, role_type_keywords
    ORIGINAL_TEXT STRING,

    -- Confidence
    EXTRACTION_CONFIDENCE FLOAT,
    STANDARDIZATION_CONFIDENCE FLOAT,
    OVERALL_CONFIDENCE FLOAT,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (JOB_UID, KEYWORD_SOURCE);

-- Add foreign key constraints
ALTER TABLE JOB_KEYWORDS_BRIDGE
ADD CONSTRAINT FK_JOB_KEYWORDS_JOB_UID
FOREIGN KEY (JOB_UID) REFERENCES JOBS_UNIFIED(JOB_UID);

ALTER TABLE JOB_KEYWORDS_BRIDGE
ADD CONSTRAINT FK_JOB_KEYWORDS_KEYWORD_ID
FOREIGN KEY (KEYWORD_ID) REFERENCES KEYWORDS_NORMALIZED(KEYWORD_ID);

-- Job-Locations bridge table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS JOB_LOCATIONS_BRIDGE (
    BRIDGE_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,                       -- FK to JOBS_UNIFIED
    LOCATION_ID STRING NOT NULL,                   -- FK to LOCATIONS_NORMALIZED

    -- Source Information
    LOCATION_SOURCE STRING NOT NULL,               -- 'office_locations', 'location_standardized', 'headquarters'
    ORIGINAL_TEXT STRING,                          -- Original text from LLM/source

    -- Location Context
    LOCATION_CONTEXT STRING,                       -- primary, secondary, remote_option
    WORK_ARRANGEMENT STRING,                       -- on_site, hybrid, remote

    -- Confidence & Quality
    EXTRACTION_CONFIDENCE FLOAT,                   -- LLM extraction confidence
    STANDARDIZATION_CONFIDENCE FLOAT,              -- Location matching confidence
    OVERALL_CONFIDENCE FLOAT,                      -- Combined confidence score

    -- Processing Metadata
    PROCESSING_METHOD STRING DEFAULT 'llm_auto',   -- llm_auto, manual_override, admin_correction
    NEEDS_REVIEW BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (JOB_UID, LOCATION_SOURCE);

-- Add foreign key constraints for locations
ALTER TABLE JOB_LOCATIONS_BRIDGE
ADD CONSTRAINT FK_JOB_LOCATIONS_JOB_UID
FOREIGN KEY (JOB_UID) REFERENCES JOBS_UNIFIED(JOB_UID);

ALTER TABLE JOB_LOCATIONS_BRIDGE
ADD CONSTRAINT FK_JOB_LOCATIONS_LOCATION_ID
FOREIGN KEY (LOCATION_ID) REFERENCES LOCATIONS_NORMALIZED(LOCATION_ID);

-- Performance optimization (clustering is defined in table creation)
-- Note: Regular Snowflake tables use clustering keys instead of indexes
-- Additional clustering can be added if needed:
-- ALTER TABLE JOB_KEYWORDS_BRIDGE CLUSTER BY (KEYWORD_ID, JOB_UID);

-- Skill standardization rules table
CREATE TABLE IF NOT EXISTS SKILL_STANDARDIZATION_RULES (
    RULE_ID STRING PRIMARY KEY,
    PATTERN STRING,                                 -- Pattern to match (regex or exact)
    STANDARDIZED_NAME STRING,                       -- Standard form
    SKILL_CATEGORY STRING,                          -- Correct category
    SKILL_SUBCATEGORY STRING,                       -- Correct subcategory
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    RULE_TYPE STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern, fuzzy_match
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial standardization rules
INSERT INTO SKILL_STANDARDIZATION_RULES VALUES
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

-- Skill category detection patterns table
CREATE TABLE IF NOT EXISTS SKILL_CATEGORY_PATTERNS (
    PATTERN_ID STRING PRIMARY KEY,
    SKILL_CATEGORY STRING NOT NULL,                 -- languages, frameworks, databases, cloud, tools
    PATTERN STRING NOT NULL,                        -- Regex pattern for category detection
    PATTERN_TYPE STRING DEFAULT 'regex',            -- regex, exact_match, contains
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,             -- Confidence in category assignment
    DESCRIPTION STRING,                             -- Human-readable description of pattern
    IS_ACTIVE BOOLEAN DEFAULT TRUE,                 -- Enable/disable pattern
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (SKILL_CATEGORY, IS_ACTIVE);

-- Insert skill category detection patterns
INSERT INTO SKILL_CATEGORY_PATTERNS VALUES
-- Languages patterns
('cat_001', 'languages', '\\bpython\\b', 'regex', 1.0, 'Python programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_002', 'languages', '\\bjavascript\\b', 'regex', 1.0, 'JavaScript programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_003', 'languages', '\\bjava\\b', 'regex', 1.0, 'Java programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_004', 'languages', '\\bc\\+\\+\\b', 'regex', 1.0, 'C++ programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_005', 'languages', '\\bc#\\b', 'regex', 1.0, 'C# programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_006', 'languages', '\\bruntime\\b', 'regex', 0.8, 'Runtime environments', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_007', 'languages', '\\bprogramming\\b', 'regex', 0.7, 'General programming references', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_008', 'languages', '\\blanguage\\b', 'regex', 0.6, 'Language keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Frameworks patterns
('cat_009', 'frameworks', '\\bframework\\b', 'regex', 1.0, 'Framework keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_010', 'frameworks', '\\breact\\b', 'regex', 1.0, 'React framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_011', 'frameworks', '\\bangular\\b', 'regex', 1.0, 'Angular framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_012', 'frameworks', '\\bvue\\b', 'regex', 1.0, 'Vue.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_013', 'frameworks', '\\bdjango\\b', 'regex', 1.0, 'Django framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_014', 'frameworks', '\\bspring\\b', 'regex', 1.0, 'Spring framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_015', 'frameworks', '\\bexpress\\b', 'regex', 1.0, 'Express.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_016', 'frameworks', '\\.js$', 'regex', 0.8, 'JavaScript framework extensions', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_017', 'frameworks', '\\.ts$', 'regex', 0.8, 'TypeScript framework extensions', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Databases patterns
('cat_018', 'databases', '\\bdatabase\\b', 'regex', 1.0, 'Database keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_019', 'databases', '\\bsql\\b', 'regex', 1.0, 'SQL databases', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_020', 'databases', '\\bmysql\\b', 'regex', 1.0, 'MySQL database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_021', 'databases', '\\bpostgresql\\b', 'regex', 1.0, 'PostgreSQL database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_022', 'databases', '\\bmongodb\\b', 'regex', 1.0, 'MongoDB database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_023', 'databases', '\\bredis\\b', 'regex', 1.0, 'Redis database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_024', 'databases', '\\boracle\\b', 'regex', 1.0, 'Oracle database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Cloud platforms patterns
('cat_025', 'cloud', '\\baws\\b', 'regex', 1.0, 'Amazon Web Services', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_026', 'cloud', '\\bazure\\b', 'regex', 1.0, 'Microsoft Azure', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_027', 'cloud', '\\bgcp\\b', 'regex', 1.0, 'Google Cloud Platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_028', 'cloud', '\\bcloud\\b', 'regex', 1.0, 'Cloud keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_029', 'cloud', '\\bkubernetes\\b', 'regex', 1.0, 'Kubernetes orchestration', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_030', 'cloud', '\\bcontainer\\b', 'regex', 0.8, 'Container technologies', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Tools patterns
('cat_031', 'tools', '\\btool\\b', 'regex', 0.7, 'Tool keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_032', 'tools', '\\bgit\\b', 'regex', 1.0, 'Git version control', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_033', 'tools', '\\bdocker\\b', 'regex', 1.0, 'Docker containerization', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_034', 'tools', '\\bjenkins\\b', 'regex', 1.0, 'Jenkins CI/CD', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_035', 'tools', '\\bslack\\b', 'regex', 0.9, 'Slack communication tool', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_036', 'tools', '\\bjira\\b', 'regex', 1.0, 'JIRA project management', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


-- Location standardization rules
CREATE OR REPLACE TABLE LOCATION_STANDARDIZATION_RULES (
    RULE_ID STRING PRIMARY KEY,
    PATTERN STRING,                                 -- Pattern to match
    STANDARDIZED_NAME STRING,                       -- Standard form
    CITY STRING,
    STATE_PROVINCE STRING,
    COUNTRY STRING,
    LOCATION_TYPE STRING,                           -- office, remote, hybrid
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    RULE_TYPE STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern, fuzzy_match
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Example location standardization rules
INSERT INTO LOCATION_STANDARDIZATION_RULES VALUES
('loc_001', 'san francisco', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_002', 'sf', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_003', 'san francisco, ca', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_004', 'new york', 'New York, NY', 'New York', 'New York', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_005', 'nyc', 'New York, NY', 'New York', 'New York', 'United States', 'office', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('loc_006', 'remote', 'Remote', NULL, NULL, 'Global', 'remote', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_007', 'work from home', 'Remote', NULL, NULL, 'Global', 'remote', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_008', 'seattle', 'Seattle, WA', 'Seattle', 'Washington', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_009', 'austin', 'Austin, TX', 'Austin', 'Texas', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_010', 'boston', 'Boston, MA', 'Boston', 'Massachusetts', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP);
```

### 6. Create Views for Easy Querying

```sql
-- View for English jobs only
CREATE OR REPLACE VIEW jobs_english AS
SELECT * FROM jobs_unified
WHERE is_english = TRUE;

-- View for recent jobs (last 30 days)
CREATE OR REPLACE VIEW jobs_recent AS
SELECT * FROM jobs_unified
WHERE date_posted >= CURRENT_DATE - 30
AND is_active = TRUE;

-- View for jobs with complete LLM enrichment
CREATE OR REPLACE VIEW jobs_fully_enriched AS
SELECT
    j.*,
    llm.*
FROM jobs_unified j
INNER JOIN jobs_llm_enriched llm ON j.job_uid = llm.job_uid
WHERE j.is_english = TRUE;

-- View for jobs with salary information (from LLM extraction)
CREATE OR REPLACE VIEW jobs_with_salary AS
SELECT
    j.*,
    llm.salary_min,
    llm.salary_max,
    llm.salary_currency,
    llm.salary_period,
    llm.salary_confidence
FROM jobs_unified j
INNER JOIN jobs_llm_enriched llm ON j.job_uid = llm.job_uid
WHERE llm.salary_min IS NOT NULL OR llm.salary_max IS NOT NULL;

-- View for high-confidence LLM extractions
CREATE OR REPLACE VIEW jobs_high_confidence_llm AS
SELECT
    j.*,
    llm.*
FROM jobs_unified j
INNER JOIN jobs_llm_enriched llm ON j.job_uid = llm.job_uid
WHERE llm.llm_overall_confidence >= 0.8;

-- Platform summary view (updated for LLM data)
CREATE OR REPLACE VIEW platform_summary AS
SELECT
    j.platform,
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN j.is_english = TRUE THEN 1 END) as english_jobs,
    COUNT(llm.job_uid) as jobs_with_llm_data,
    COUNT(CASE WHEN llm.salary_min IS NOT NULL THEN 1 END) as jobs_with_salary,
    COUNT(CASE WHEN llm.llm_overall_confidence >= 0.8 THEN 1 END) as high_confidence_extractions,
    AVG(j.data_quality_score) as avg_base_quality_score,
    AVG(llm.llm_overall_confidence) as avg_llm_confidence,
    MIN(j.date_posted) as earliest_job,
    MAX(j.date_posted) as latest_job
FROM jobs_unified j
LEFT JOIN jobs_llm_enriched llm ON j.job_uid = llm.job_uid
GROUP BY j.platform;

-- LLM processing status view
CREATE OR REPLACE VIEW llm_processing_status AS
SELECT
    COUNT(j.job_uid) as total_jobs,
    COUNT(llm.job_uid) as processed_jobs,
    COUNT(j.job_uid) - COUNT(llm.job_uid) as pending_jobs,
    ROUND((COUNT(llm.job_uid)::FLOAT / COUNT(j.job_uid)) * 100, 1) as processing_percentage,
    COUNT(CASE WHEN llm.llm_overall_confidence >= 0.8 THEN 1 END) as high_confidence_extractions,
    COUNT(CASE WHEN llm.llm_needs_manual_review = TRUE THEN 1 END) as needs_manual_review,
    AVG(llm.llm_overall_confidence) as avg_confidence_score
FROM jobs_unified j
LEFT JOIN jobs_llm_enriched llm ON j.job_uid = llm.job_uid
WHERE j.is_english = TRUE;

-- Skills analysis view (using normalized skills tables)
CREATE OR REPLACE VIEW SKILLS_ANALYSIS AS
SELECT
    sn.SKILL_NAME,
    sn.SKILL_CATEGORY,
    sn.SKILL_SUBCATEGORY,
    COUNT(DISTINCT jsb.JOB_UID) as FREQUENCY,
    ARRAY_AGG(DISTINCT j.PLATFORM) as PLATFORMS,
    AVG(jsb.OVERALL_CONFIDENCE) as AVG_CONFIDENCE,
    sn.TREND_DIRECTION,
    sn.FREQUENCY_COUNT as TOTAL_MARKET_FREQUENCY
FROM SKILLS_NORMALIZED sn
LEFT JOIN JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
LEFT JOIN JOBS_UNIFIED j ON jsb.JOB_UID = j.JOB_UID
WHERE j.IS_ENGLISH = TRUE OR j.IS_ENGLISH IS NULL
GROUP BY sn.SKILL_NAME, sn.SKILL_CATEGORY, sn.SKILL_SUBCATEGORY, sn.TREND_DIRECTION, sn.FREQUENCY_COUNT
ORDER BY FREQUENCY DESC;

-- Top skills by category view
CREATE OR REPLACE VIEW TOP_SKILLS_BY_CATEGORY AS
SELECT
    SKILL_CATEGORY,
    SKILL_NAME,
    FREQUENCY_COUNT,
    TREND_DIRECTION,
    CONFIDENCE_SCORE,
    ROW_NUMBER() OVER (PARTITION BY SKILL_CATEGORY ORDER BY FREQUENCY_COUNT DESC) as RANK_IN_CATEGORY
FROM SKILLS_NORMALIZED
WHERE CONFIDENCE_SCORE >= 0.8
ORDER BY SKILL_CATEGORY, RANK_IN_CATEGORY;

-- Skills quality monitoring view
CREATE OR REPLACE VIEW SKILLS_QUALITY_REPORT AS
SELECT
    SKILL_CATEGORY,
    COUNT(*) as TOTAL_SKILLS,
    COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as HIGH_CONFIDENCE_SKILLS,
    COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as LOW_CONFIDENCE_SKILLS,
    COUNT(CASE WHEN MANUAL_REVIEW_FLAG THEN 1 END) as NEEDS_REVIEW,
    AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE,
    AVG(FREQUENCY_COUNT) as AVG_FREQUENCY
FROM SKILLS_NORMALIZED
GROUP BY SKILL_CATEGORY
ORDER BY TOTAL_SKILLS DESC;

-- Job skills coverage analysis view
CREATE OR REPLACE VIEW JOB_SKILLS_COVERAGE AS
SELECT
    COUNT(DISTINCT ju.JOB_UID) as TOTAL_JOBS,
    COUNT(DISTINCT jsb.JOB_UID) as JOBS_WITH_SKILLS,
    ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as COVERAGE_PERCENTAGE,
    AVG(skills_per_job.SKILL_COUNT) as AVG_SKILLS_PER_JOB
FROM JOBS_UNIFIED ju
LEFT JOIN JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
LEFT JOIN (
    SELECT JOB_UID, COUNT(*) as SKILL_COUNT
    FROM JOB_SKILLS_BRIDGE
    GROUP BY JOB_UID
) skills_per_job ON ju.JOB_UID = skills_per_job.JOB_UID;

-- Locations analysis view (using normalized locations tables)
CREATE OR REPLACE VIEW LOCATIONS_ANALYSIS AS
SELECT
    ln.LOCATION_NAME,
    ln.CITY,
    ln.STATE_PROVINCE,
    ln.COUNTRY,
    ln.LOCATION_TYPE,
    COUNT(DISTINCT jlb.JOB_UID) as FREQUENCY,
    ARRAY_AGG(DISTINCT j.PLATFORM) as PLATFORMS,
    AVG(jlb.OVERALL_CONFIDENCE) as AVG_CONFIDENCE,
    ln.IS_REMOTE_FRIENDLY,
    ln.IS_MAJOR_TECH_HUB,
    ln.FREQUENCY_COUNT as TOTAL_MARKET_FREQUENCY
FROM LOCATIONS_NORMALIZED ln
LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ln.LOCATION_ID = jlb.LOCATION_ID
LEFT JOIN JOBS_UNIFIED j ON jlb.JOB_UID = j.JOB_UID
WHERE j.IS_ENGLISH = TRUE OR j.IS_ENGLISH IS NULL
GROUP BY ln.LOCATION_NAME, ln.CITY, ln.STATE_PROVINCE, ln.COUNTRY, ln.LOCATION_TYPE,
         ln.IS_REMOTE_FRIENDLY, ln.IS_MAJOR_TECH_HUB, ln.FREQUENCY_COUNT
ORDER BY FREQUENCY DESC;

-- Top locations by type view
CREATE OR REPLACE VIEW TOP_LOCATIONS_BY_TYPE AS
SELECT
    LOCATION_TYPE,
    LOCATION_NAME,
    CITY,
    STATE_PROVINCE,
    COUNTRY,
    FREQUENCY_COUNT,
    IS_MAJOR_TECH_HUB,
    CONFIDENCE_SCORE,
    ROW_NUMBER() OVER (PARTITION BY LOCATION_TYPE ORDER BY FREQUENCY_COUNT DESC) as RANK_IN_TYPE
FROM LOCATIONS_NORMALIZED
WHERE CONFIDENCE_SCORE >= 0.8
ORDER BY LOCATION_TYPE, RANK_IN_TYPE;

-- Job locations coverage analysis view
CREATE OR REPLACE VIEW JOB_LOCATIONS_COVERAGE AS
SELECT
    COUNT(DISTINCT ju.JOB_UID) as TOTAL_JOBS,
    COUNT(DISTINCT jlb.JOB_UID) as JOBS_WITH_LOCATIONS,
    ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as COVERAGE_PERCENTAGE,
    AVG(locations_per_job.LOCATION_COUNT) as AVG_LOCATIONS_PER_JOB
FROM JOBS_UNIFIED ju
LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
LEFT JOIN (
    SELECT JOB_UID, COUNT(*) as LOCATION_COUNT
    FROM JOB_LOCATIONS_BRIDGE
    GROUP BY JOB_UID
) locations_per_job ON ju.JOB_UID = locations_per_job.JOB_UID;

-- Skill category patterns analysis view
CREATE OR REPLACE VIEW SKILL_CATEGORY_PATTERNS_ANALYSIS AS
SELECT
    SKILL_CATEGORY,
    COUNT(*) as TOTAL_PATTERNS,
    COUNT(CASE WHEN IS_ACTIVE = TRUE THEN 1 END) as ACTIVE_PATTERNS,
    COUNT(CASE WHEN IS_ACTIVE = FALSE THEN 1 END) as INACTIVE_PATTERNS,
    AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE,
    MIN(CONFIDENCE_SCORE) as MIN_CONFIDENCE,
    MAX(CONFIDENCE_SCORE) as MAX_CONFIDENCE,
    ARRAY_AGG(CASE WHEN IS_ACTIVE = TRUE THEN PATTERN END) as ACTIVE_PATTERN_LIST
FROM SKILL_CATEGORY_PATTERNS
GROUP BY SKILL_CATEGORY
ORDER BY TOTAL_PATTERNS DESC;

-- Data validation report view
CREATE OR REPLACE VIEW DATA_VALIDATION_REPORT AS
WITH VALIDATION_CHECKS AS (
    -- Check 1: Skills without jobs
    SELECT 'orphaned_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED sn
    LEFT JOIN JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
    WHERE jsb.SKILL_ID IS NULL

    UNION ALL

    -- Check 2: Jobs without skills
    SELECT 'jobs_without_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM JOBS_UNIFIED ju
    LEFT JOIN JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
    WHERE jsb.JOB_UID IS NULL AND ju.IS_ENGLISH = TRUE

    UNION ALL

    -- Check 3: Low confidence skills
    SELECT 'low_confidence_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED
    WHERE CONFIDENCE_SCORE < 0.5

    UNION ALL

    -- Check 4: Duplicate skill names
    SELECT 'duplicate_skill_names' as CHECK_NAME, COUNT(*) - COUNT(DISTINCT SKILL_NAME) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED

    UNION ALL

    -- Check 5: Locations without jobs
    SELECT 'orphaned_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM LOCATIONS_NORMALIZED ln
    LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ln.LOCATION_ID = jlb.LOCATION_ID
    WHERE jlb.LOCATION_ID IS NULL

    UNION ALL

    -- Check 6: Jobs without locations
    SELECT 'jobs_without_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM JOBS_UNIFIED ju
    LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
    WHERE jlb.JOB_UID IS NULL AND ju.IS_ENGLISH = TRUE

    UNION ALL

    -- Check 7: Low confidence locations
    SELECT 'low_confidence_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM LOCATIONS_NORMALIZED
    WHERE CONFIDENCE_SCORE < 0.5
)
SELECT * FROM VALIDATION_CHECKS
WHERE ISSUE_COUNT > 0;
```

## Environment Variables

Add the STAGE schema environment variable to your configuration:

```bash
# Add to your .env file
export SNOWFLAKE_STAGE_SCHEMA="STAGE"
```

## Verification

After setup, verify the schema and tables were created correctly:

```sql
-- Check schema exists
SHOW SCHEMAS IN DATABASE BETTERJOBS_DB;

-- Check tables in STAGE schema
USE SCHEMA STAGE;
SHOW TABLES;

-- Verify main table structures
DESCRIBE TABLE jobs_unified;
DESCRIBE TABLE jobs_llm_enriched;

-- Verify normalization tables (Phase 3)
DESCRIBE TABLE SKILLS_NORMALIZED;
DESCRIBE TABLE JOB_SKILLS_BRIDGE;
DESCRIBE TABLE KEYWORDS_NORMALIZED;
DESCRIBE TABLE JOB_KEYWORDS_BRIDGE;
DESCRIBE TABLE LOCATIONS_NORMALIZED;
DESCRIBE TABLE JOB_LOCATIONS_BRIDGE;
DESCRIBE TABLE SKILL_CATEGORY_PATTERNS;

-- Check views
SHOW VIEWS;

-- Verify foreign key constraints
SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = 'STAGE'
AND CONSTRAINT_TYPE = 'FOREIGN KEY';

-- Test normalization views
SELECT * FROM SKILLS_QUALITY_REPORT;
SELECT * FROM JOB_SKILLS_COVERAGE;
SELECT * FROM LOCATIONS_ANALYSIS LIMIT 10;
SELECT * FROM JOB_LOCATIONS_COVERAGE;
SELECT * FROM DATA_VALIDATION_REPORT;

-- Test skill category patterns
SELECT SKILL_CATEGORY, COUNT(*) as PATTERN_COUNT
FROM SKILL_CATEGORY_PATTERNS
WHERE IS_ACTIVE = TRUE
GROUP BY SKILL_CATEGORY
ORDER BY PATTERN_COUNT DESC;

-- Test skill category patterns analysis view
SELECT * FROM SKILL_CATEGORY_PATTERNS_ANALYSIS;
```

## Next Steps

After completing the schema setup:

1. **Phase 1**: Create Dagster Assets for STAGE layer transformation assets
2. **Phase 2**: Implement Data Utils for data cleaning and standardization
3. **Phase 3**: LLM Integration - Add Gemini-powered data extraction to populate `jobs_llm_enriched`
4. **Phase 4**: LLM Data Normalization - Implement the standardization process:
   - Extract skills and keywords from VARIANT columns
   - Apply standardization rules and deduplication
   - Populate normalized tables (`SKILLS_NORMALIZED`, `JOB_SKILLS_BRIDGE`, etc.)
   - Validate data quality and implement monitoring
5. **Phase 5**: Quality Validation and monitoring across all tables
6. **Phase 6**: Analytics Layer preparation with normalized data structures

## Troubleshooting

### Common Issues

**Permission Errors:**
```sql
-- Grant additional permissions if needed
GRANT ALL PRIVILEGES ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
```

**Schema Already Exists:**
- The `CREATE SCHEMA IF NOT EXISTS` statement is safe to run multiple times
- If you need to recreate tables, use `DROP TABLE IF EXISTS` first

**VARIANT Column Issues:**
- VARIANT columns store JSON data natively in Snowflake
- Access nested data using dot notation: `column_name:key`
- For arrays, use `ARRAY_SIZE()` and `FLATTEN()` functions

## Performance Considerations

- **Clustering**: The table is clustered by `platform` and `date_posted` for optimal query performance
- **Partitioning**: Use `partition_date` for time-based queries
- **VARIANT Indexing**: Snowflake automatically indexes VARIANT columns
- **Search Optimization**: Consider enabling for text search in production

### Key Changes from Previous Schema:

1. **Primary Key Change**: `job_uid STRING PRIMARY KEY` replaces the composite key approach
2. **UID Generation**: Deterministic UIDs generated from `job_id + platform + company_id + date_posted`
3. **Simpler Uniqueness**: Single field uniqueness instead of complex composite key logic
4. **Performance**: Single indexed primary key for faster lookups and joins

### UID Benefits:

- **True Uniqueness**: Single field for unique identification
- **Deterministic**: Same job always gets same UID across processing runs
- **Performance**: Single indexed field vs composite key lookups
- **Future-proof**: Schema changes don't affect uniqueness logic
- **Simplified Analytics**: Easier downstream joins and references

### UID Implementation Details:

**Generation Algorithm**:
```python
# Deterministic UID generation (Updated to prevent collisions)
composite_key = f"JOB_ID:{job_id}|PLATFORM:{platform}|COMPANY:{company_id}|DATE:{date_posted}"
job_uid = hashlib.sha256(composite_key.encode()).hexdigest()[:32]  # 32 chars instead of 16
```

**Example UIDs**:
- Input: `("12345", "workday", "company_1", "2024-01-15")`
- Output: `"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"` (32-character hex string)

**Collision Prevention**:
- **Hash Length**: Increased from 16 to 32 characters (64-bit to 128-bit hash space)
- **Collision Probability**: Reduced from ~2^64 to ~2^128 possible values
- **Explicit Labels**: Added field labels in composite key to prevent ambiguity
- **Null Handling**: Explicit "NULL_*" values instead of empty strings

### UID Migration Considerations:

- Existing data will be automatically migrated when the asset runs
- UIDs are generated during data processing, not as database defaults
- All downstream references should use `job_uid` instead of composite keys