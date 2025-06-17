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
    role_type_keywords, -- ["Individual Contributor", "Senior Level", "Backend Focus"]
    office_locations   -- ["San Francisco, CA", "New York, NY", "Remote"]
FROM STAGE.jobs_llm_enriched;
```

### Analytics Challenges

**Querying Difficulties**:
- ❌ Complex FLATTEN operations required for basic skill counts and location analysis
- ❌ Slow performance on VARIANT column aggregations
- ❌ Difficult to join with other relational data
- ❌ No referential integrity or data validation

**Business Intelligence Limitations**:
- ❌ Cannot easily answer "How many jobs require Python?"
- ❌ Cannot calculate salary premiums by technology or location
- ❌ Cannot build skill trend analysis or geographic insights
- ❌ Cannot create normalized skill taxonomies or location hierarchies

**Data Quality Issues**:
- ❌ No deduplication of skill variations ("Python" vs "python" vs "Python3")
- ❌ No standardization of location formats ("SF" vs "San Francisco" vs "San Francisco, CA")
- ❌ No validation of skill taxonomies or geographic data
- ❌ No frequency analysis or trend tracking

## Solution Architecture

### Normalized Data Structure

Transform VARIANT data into properly normalized relational tables:

```
jobs_llm_enriched (existing)
    ↓
skill_extraction_and_normalization
    ↓
STAGE.SKILLS_NORMALIZED (master skills)
STAGE.JOB_SKILLS_BRIDGE (many-to-many)
STAGE.KEYWORDS_NORMALIZED (master keywords)
STAGE.JOB_KEYWORDS_BRIDGE (many-to-many)
STAGE.LOCATIONS_NORMALIZED (master locations)
STAGE.JOB_LOCATIONS_BRIDGE (many-to-many)
```

## Development Plan: Assets and Functions

### Dagster Assets Architecture

The LLM data standardization will be implemented as a series of interconnected Dagster assets, following the existing pipeline patterns. Each asset has a specific purpose and clear dependencies.

#### Asset Dependency Flow

```
jobs_llm_enriched (existing)
    ↓
stage_llm_raw_extractions (Phase 1)
    ↓
┌─ stage_skills_standardization (Phase 1)  ┬─ stage_skills_normalized (Phase 1)
├─ stage_keywords_standardization (Phase 2) ├─ stage_keywords_normalized (Phase 2)
└─ stage_locations_standardization (Phase 3)─ stage_locations_normalized (Phase 3)
    ↓
stage_llm_data_quality_validation (Phase 4)
    ↓
stage_llm_analytics_views (Phase 5)
```

### Phase 1: Skills Normalization Assets

#### 1.1 `stage_llm_skills_raw_extraction`
**Purpose**: Extract and flatten all skills data from VARIANT columns in `jobs_llm_enriched`
**Dependencies**: `jobs_llm_enriched`
**Output**: Temporary staging table with raw skills data

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py

@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten skills from LLM VARIANT columns",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_skills_raw_extraction(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all skills from VARIANT columns and flatten into workable format.

    Processes:
    - technical_skills: Flattens nested JSON by category
    - soft_skills: Extracts array values
    - primary_keywords: Treats as skills for standardization

    Output: Raw skills with source tracking and confidence scores
    """
    # Implementation details...
```

#### 1.2 `stage_skills_standardization_rules`
**Purpose**: Manage and update skill standardization rules and aliases
**Dependencies**: None (reference data)
**Output**: Updated standardization rules table

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py

@asset(
    description="Maintain skill standardization rules and aliases",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 7)  # Weekly updates
)
def stage_skills_standardization_rules(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain comprehensive skill standardization rules.

    Features:
    - Common skill aliases (JS -> JavaScript, ML -> Machine Learning)
    - Category classification rules
    - Confidence scoring based on pattern matching
    - Manual override support
    """
    # Implementation details...
```

#### 1.3 `stage_skills_normalized`
**Purpose**: Apply standardization rules and create normalized skills master table
**Dependencies**: `stage_llm_skills_raw_extraction`, `stage_skills_standardization_rules`
**Output**: Normalized skills with market intelligence

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py

@asset(
    deps=["stage_llm_skills_raw_extraction", "stage_skills_standardization_rules"],
    description="Create normalized skills master table with market intelligence",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_normalized(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create skills master table.

    Processing:
    - Apply standardization rules with confidence scoring
    - Deduplicate skill variations
    - Calculate frequency and trend metrics
    - Flag low-confidence items for manual review

    Output: Clean skills master table for analytics
    """
    # Implementation details...
```

#### 1.4 `stage_job_skills_bridge`
**Purpose**: Create many-to-many relationships between jobs and normalized skills
**Dependencies**: `stage_skills_normalized`, `jobs_unified`
**Output**: Job-skill relationships with context and confidence

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py

@asset(
    deps=["stage_skills_normalized", "stage_jobs_unified"],
    description="Create job-skill relationships with context tracking",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_skills_bridge(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized skills with rich context.

    Features:
    - Source tracking (technical_skills vs soft_skills vs keywords)
    - Context classification (required vs preferred vs nice-to-have)
    - Experience level inference
    - Confidence scoring for skill-job associations
    """
    # Implementation details...
```

### Phase 2: Keywords Standardization Assets

#### 2.1 `stage_keywords_normalized`
**Purpose**: Standardize and classify job posting keywords
**Dependencies**: `stage_llm_skills_raw_extraction`
**Output**: Normalized keywords master table

```python
@asset(
    deps=["stage_llm_skills_raw_extraction"],
    description="Standardize job posting keywords and classifications",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keywords_normalized(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Process and standardize keywords from LLM extractions.

    Categories:
    - Industry keywords: FinTech, B2B SaaS, High Growth
    - Role type keywords: Individual Contributor, Senior Level
    - Technology keywords: Cloud Native, Microservices
    - Company stage keywords: Series A, Public Company
    """
    # Implementation details...
```

#### 2.2 `stage_job_keywords_bridge`
**Purpose**: Create job-keyword relationships
**Dependencies**: `stage_keywords_normalized`
**Output**: Job-keyword bridge table

### Phase 3: Location Standardization Assets

#### 3.1 `stage_locations_normalized`
**Purpose**: Standardize and enrich location data with geographic intelligence
**Dependencies**: `jobs_llm_enriched`, `jobs_unified`
**Output**: Normalized locations with geographic hierarchy

```python
@asset(
    deps=["jobs_llm_enriched", "jobs_unified"],
    description="Standardize location data with geographic intelligence",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_locations_normalized(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Standardize location data from multiple sources.

    Sources:
    - office_locations: VARIANT array from LLM
    - location_standardized: Basic location from jobs_unified
    - headquarters: Company location data

    Enrichment:
    - Geographic hierarchy (city, state, country, region)
    - Tech hub classification
    - Remote work indicators
    - Cost of living index integration
    """
    # Implementation details...
```

#### 3.2 `stage_job_locations_bridge`
**Purpose**: Create job-location relationships with work arrangement context
**Dependencies**: `stage_locations_normalized`
**Output**: Job-location bridge with work type classification

### Phase 4: Data Quality and Validation Assets

#### 4.1 `stage_llm_data_quality_validation`
**Purpose**: Comprehensive data quality monitoring and validation
**Dependencies**: All normalization assets
**Output**: Data quality reports and flagged items

```python
@asset(
    deps=["stage_skills_normalized", "stage_keywords_normalized", "stage_locations_normalized"],
    description="Comprehensive data quality validation for LLM standardization",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_data_quality_validation(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Validate data quality across all normalized tables.

    Validations:
    - Orphaned records (skills without jobs, jobs without skills)
    - Low confidence items requiring manual review
    - Duplicate detection and resolution
    - Coverage analysis (percentage of jobs with normalized data)
    - Trend analysis and anomaly detection
    """
    # Implementation details...
```

#### 4.2 `stage_llm_quality_metrics`
**Purpose**: Generate quality metrics and monitoring dashboards
**Dependencies**: `stage_llm_data_quality_validation`
**Output**: Quality metrics for monitoring and alerting

### Phase 5: Analytics Enablement Assets

#### 5.1 `stage_llm_analytics_views`
**Purpose**: Create optimized views for downstream analytics
**Dependencies**: All bridge tables
**Output**: Pre-aggregated views for common analytics queries

```python
@asset(
    deps=["stage_job_skills_bridge", "stage_job_keywords_bridge", "stage_job_locations_bridge"],
    description="Create analytics-optimized views for downstream consumption",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_analytics_views(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create performant views for common analytics queries.

    Views:
    - skills_analysis: Top skills by category, trend, and location
    - location_insights: Geographic distribution and remote work trends
    - keyword_trends: Industry and role classification analytics
    - job_enrichment_summary: Overall enrichment quality and coverage
    """
    # Implementation details...
```

### Utility Functions and Classes

#### Core Processing Functions

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_standardization.py

class SkillStandardizer:
    """Handle skill standardization and deduplication logic"""

    def __init__(self, confidence_threshold: float = 0.7):
        self.confidence_threshold = confidence_threshold
        self.skill_aliases = self._load_skill_aliases()

    def standardize_skill(self, raw_skill: str, category: str) -> Dict[str, Any]:
        """Standardize a single skill with confidence scoring"""
        # Implementation...

    def detect_skill_category(self, skill: str) -> str:
        """Auto-detect skill category using pattern matching"""
        # Implementation...

    def calculate_confidence(self, raw_skill: str, standardized_skill: str) -> float:
        """Calculate standardization confidence score"""
        # Implementation...

class LocationStandardizer:
    """Handle location standardization and geographic enrichment"""

    def standardize_location(self, raw_location: str) -> Dict[str, Any]:
        """Standardize location with geographic hierarchy"""
        # Implementation...

    def detect_remote_indicators(self, location: str) -> bool:
        """Detect if location indicates remote work"""
        # Implementation...

    def classify_tech_hub(self, city: str, state: str) -> bool:
        """Classify if location is a major tech hub"""
        # Implementation...

class KeywordClassifier:
    """Handle keyword classification and standardization"""

    def classify_keyword_type(self, keyword: str) -> str:
        """Classify keyword into type (industry, role, technology, etc.)"""
        # Implementation...

    def standardize_keyword(self, raw_keyword: str) -> Dict[str, Any]:
        """Standardize keyword with confidence scoring"""
        # Implementation...

# Data quality and validation functions
def validate_normalization_completeness(snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Validate that normalization process completed successfully"""
    # Implementation...

def calculate_coverage_metrics(snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Calculate coverage metrics for normalized data"""
    # Implementation...

def detect_data_quality_issues(snowflake: SnowflakeResource) -> List[Dict[str, Any]]:
    """Detect and report data quality issues"""
    # Implementation...
```

#### Configuration Management

```python
# config/llm_standardization_config.py

@dataclass
class LLMStandardizationConfig:
    """Configuration for LLM data standardization process"""

    # Confidence thresholds
    skill_confidence_threshold: float = 0.7
    location_confidence_threshold: float = 0.8
    keyword_confidence_threshold: float = 0.6

    # Processing parameters
    min_skill_frequency: int = 2  # Minimum appearances to include skill
    max_skill_variants: int = 10  # Maximum variants to track per skill

    # Quality thresholds
    min_coverage_percentage: float = 85.0  # Minimum coverage for quality validation
    max_low_confidence_percentage: float = 15.0  # Maximum low confidence items

    # Batch processing
    batch_size: int = 1000
    parallel_processing: bool = True
```

## Implementation Plan

### Phase 1: Skills Normalization Infrastructure

#### 1.1 Create Skills Master Table

```sql
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
```

#### 1.2 Create Job-Skills Bridge Table

```sql
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
```

### Phase 2: Keywords and Classification Normalization

#### 2.1 Create Keywords Master Table

```sql
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
```

#### 2.2 Create Job-Keywords Bridge Table

```sql
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
```



### Phase 4: Data Extraction and Normalization Process

#### 4.1 Skills Extraction Logic

```sql
-- Step 1: Extract all skills from VARIANT columns
CREATE OR REPLACE VIEW SKILLS_RAW_EXTRACTION AS
WITH TECHNICAL_SKILLS_EXPLODED AS (
    -- Extract technical skills by category
    SELECT
        JOB_UID,
        'technical_skills' as SKILL_SOURCE,
        SKILL_CATEGORY.KEY::STRING as SKILL_CATEGORY,
        TRIM(LOWER(SKILL_NAME.VALUE::STRING)) as SKILL_NAME_RAW,
        SKILL_NAME.VALUE::STRING as SKILL_NAME_ORIGINAL
    FROM STAGE.JOBS_LLM_ENRICHED,
    LATERAL FLATTEN(input => TECHNICAL_SKILLS) SKILL_CATEGORY,
    LATERAL FLATTEN(input => SKILL_CATEGORY.VALUE) SKILL_NAME
    WHERE TECHNICAL_SKILLS IS NOT NULL
      AND SKILL_CATEGORY.VALUE IS NOT NULL
      AND ARRAY_SIZE(SKILL_CATEGORY.VALUE) > 0
),

SOFT_SKILLS_EXPLODED AS (
    -- Extract soft skills
    SELECT
        JOB_UID,
        'soft_skills' as SKILL_SOURCE,
        'soft' as SKILL_CATEGORY,
        TRIM(LOWER(SKILL.VALUE::STRING)) as SKILL_NAME_RAW,
        SKILL.VALUE::STRING as SKILL_NAME_ORIGINAL
    FROM STAGE.JOBS_LLM_ENRICHED,
    LATERAL FLATTEN(input => SOFT_SKILLS) SKILL
    WHERE SOFT_SKILLS IS NOT NULL
      AND SKILL.VALUE IS NOT NULL
      AND LENGTH(TRIM(SKILL.VALUE::STRING)) > 0
),

PRIMARY_KEYWORDS_EXPLODED AS (
    -- Extract primary keywords as skills
    SELECT
        JOB_UID,
        'primary_keywords' as SKILL_SOURCE,
        'keyword' as SKILL_CATEGORY,
        TRIM(LOWER(KEYWORD.VALUE::STRING)) as SKILL_NAME_RAW,
        KEYWORD.VALUE::STRING as SKILL_NAME_ORIGINAL
    FROM STAGE.JOBS_LLM_ENRICHED,
    LATERAL FLATTEN(input => PRIMARY_KEYWORDS) KEYWORD
    WHERE PRIMARY_KEYWORDS IS NOT NULL
      AND KEYWORD.VALUE IS NOT NULL
      AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 0
)

SELECT * FROM TECHNICAL_SKILLS_EXPLODED
UNION ALL
SELECT * FROM SOFT_SKILLS_EXPLODED
UNION ALL
SELECT * FROM PRIMARY_KEYWORDS_EXPLODED;
```

#### 4.2 Skill Standardization Rules

```sql
-- Step 2: Create skill standardization mapping
CREATE OR REPLACE TABLE SKILL_STANDARDIZATION_RULES (
    RULE_ID STRING PRIMARY KEY,
    PATTERN STRING,                                 -- Pattern to match (regex or exact)
    STANDARDIZED_NAME STRING,                       -- Standard form
    SKILL_CATEGORY STRING,                          -- Correct category
    SKILL_SUBCATEGORY STRING,                       -- Correct subcategory
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    RULE_TYPE STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern,
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Example standardization rules
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
```

#### 4.3 Automated Standardization Process

```sql
-- Step 3: Apply standardization rules and populate normalized tables
CREATE OR REPLACE PROCEDURE STANDARDIZE_AND_POPULATE_SKILLS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    DELETE FROM STAGE.JOB_SKILLS_BRIDGE;
    DELETE FROM STAGE.SKILLS_NORMALIZED;

    -- Populate SKILLS_NORMALIZED with standardized skills
    INSERT INTO STAGE.SKILLS_NORMALIZED (
        SKILL_ID,
        SKILL_NAME,
        SKILL_NAME_CLEAN,
        SKILL_NAME_ORIGINAL,
        SKILL_CATEGORY,
        SKILL_SUBCATEGORY,
        ORIGINAL_VARIANTS,
        FREQUENCY_COUNT,
        FIRST_SEEN_DATE,
        LAST_SEEN_DATE,
        CONFIDENCE_SCORE
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
        FROM SKILLS_RAW_EXTRACTION sre
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
    FROM SKILLS_RAW_EXTRACTION sre
    JOIN STAGE.skills_normalized sn
        ON (COALESCE(sr.standardized_name, sre.skill_name_original) = sn.skill_name)
    LEFT JOIN STAGE.skill_standardization_rules sr
        ON LOWER(sre.skill_name_raw) = LOWER(sr.pattern);

    RETURN 'Skills standardization completed successfully';
END;
$$;
```

#### 4.4 Quality Monitoring Views

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

#### 4.5 Data Validation Rules

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
-- Performance optimization using clustering keys (instead of indexes)
-- Note: Regular Snowflake tables use clustering keys instead of secondary indexes

-- Skills table is clustered by (skill_category, skill_name) in table definition
-- Additional clustering can be added if needed:
-- ALTER TABLE STAGE.skills_normalized CLUSTER BY (skill_category, frequency_count);

-- Bridge table is clustered by (job_uid, skill_id) in table definition
-- Additional clustering options:
-- ALTER TABLE STAGE.job_skills_bridge CLUSTER BY (skill_category, overall_confidence);

-- For text-based searches, search optimization can be enabled:
-- ALTER TABLE STAGE.skills_normalized ADD SEARCH OPTIMIZATION;
-- ALTER TABLE STAGE.job_skills_bridge ADD SEARCH OPTIMIZATION;
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

## Implementation Status

### ✅ Phase 1: Skills Normalization Assets - **COMPLETED** (June 2025)

**Assets Implemented:**
- `stage_llm_skills_raw_extraction`: Extracts and flattens skills from VARIANT columns
- `stage_skills_standardization_rules`: Manages skill standardization rules
- `stage_skills_normalized`: Creates normalized skills master table with family classification
- `stage_job_skills_bridge`: Creates job-skill relationships with confidence scoring

**Key Features Delivered:**
- ✅ Skills extraction from technical_skills, soft_skills, and primary_keywords
- ✅ Lookup table-based skill family classification (development, data, devops, etc.)
- ✅ Confidence scoring and manual review flagging
- ✅ Comprehensive bridge table with source tracking and context
- ✅ Error handling and accurate logging

**SQL Configuration Files Created:**
- `pipeline/sql/llm_standardization/insert_skill_category_patterns.sql` - Category detection patterns
- `pipeline/sql/llm_standardization/insert_skill_standardization_rules.sql` - Skill standardization rules
- `pipeline/sql/llm_standardization/insert_skill_family_mappings.sql` - Family classification mappings
- `pipeline/sql/llm_standardization/insert_location_standardization_rules.sql` - Location standardization
- `pipeline/sql/llm_standardization/README.md` - Setup documentation

**Production Metrics Achieved:**
- Skills Normalized: 8,302 unique skills
- Job-Skill Relationships: 149,477 total relationships
- Coverage: 6,706 jobs with normalized skills
- Average Skills per Job: 22.3
- High Confidence Relationships: 10,572

**Technical Improvements:**
- Fixed Snowflake VARIANT to ARRAY casting issues using `IS_ARRAY()`
- Implemented lookup table approach instead of hardcoded CASE statements
- Added proper Decimal to float conversion for Dagster metadata
- Enhanced logging accuracy for table creation vs. existence checks

### ✅ Phase 2: Keywords Standardization Assets - **COMPLETED** (June 2025)

Phase 2 focused on standardizing business and contextual keywords that are distinct from technical skills. This phase successfully extracted and normalized `industry_keywords` and `role_type_keywords` from the LLM enriched data.

**Note**: `primary_keywords` were already processed in Phase 1 as they overlap with technical skills.

**Assets Implemented:**
- `stage_llm_keywords_raw_extraction`: Extracts and flattens keywords from VARIANT columns
- `stage_keywords_standardization_rules`: Manages keyword standardization rules and aliases
- `stage_keyword_type_mapping`: Manages keyword type and category classifications
- `stage_keywords_normalized`: Creates normalized keywords master table with market intelligence
- `stage_job_keywords_bridge`: Creates job-keyword relationships with context tracking

**Key Features Delivered:**
- ✅ Keywords extraction from industry_keywords and role_type_keywords VARIANT arrays
- ✅ Comprehensive standardization rules for industry and role type classifications
- ✅ Lookup table-based keyword type and category mapping
- ✅ Confidence scoring and manual review flagging for low-confidence relationships
- ✅ Bridge table with source tracking and business relevance context
- ✅ Error handling and accurate logging following Phase 1 patterns

**SQL Configuration Files Created:**
- `pipeline/sql/llm_standardization/insert_keyword_standardization_rules.sql` - Comprehensive keyword standardization rules
- All keyword type mapping and classification logic implemented via lookup tables

**Production Metrics Achieved:**
- Keywords Normalized: 2,847 unique keywords (estimated)
- Job-Keyword Relationships: 24,071 total relationships
- Industry Relationships: 17,383 (business domain classifications)
- Role Type Relationships: 6,688 (hierarchy and seniority classifications)
- Coverage: 6,729 jobs with normalized keywords (97.6% of all jobs)
- High Confidence Relationships: 9,494 (39.4% of total relationships)
- Average Confidence Score: 0.733

**Technical Improvements:**
- Applied successful Phase 1 patterns to avoid Snowflake VARIANT processing issues
- Used lookup table approach instead of complex VARIANT FLATTEN operations in bridge creation
- Implemented two-pass matching logic (standardized rules + direct matches)
- Fixed column name mapping between skills and keywords standardization tables
- Enhanced confidence scoring combining extraction and standardization confidence

#### Data Sources Available:
- **industry_keywords**: VARIANT array containing industry-specific terms (e.g., "FinTech", "B2B SaaS", "Healthcare")
- **role_type_keywords**: VARIANT array containing role classification terms (e.g., "Individual Contributor", "Senior Level", "Team Lead")

#### 2.1 `stage_llm_keywords_raw_extraction`
**Purpose**: Extract and flatten keywords from VARIANT columns
**Dependencies**: `stage_jobs_llm_enriched_unified`
**Output**: Raw keywords with source tracking

**Asset Implementation:**
```python
@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten keywords from LLM VARIANT columns",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_keywords_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all keywords from VARIANT columns and flatten into workable format.

    Processes:
    - industry_keywords: Business domain and industry classification terms
    - role_type_keywords: Role hierarchy and classification keywords

    Note: primary_keywords handled in Phase 1 skills extraction

    Output: Raw keywords with source tracking and frequency metrics
    """
```

**SQL Logic:**
```sql
CREATE OR REPLACE VIEW BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION AS
WITH INDUSTRY_KEYWORDS_EXPLODED AS (
    -- Extract industry classification keywords
    SELECT
        jle.JOB_UID,
        'industry_keywords' as KEYWORD_SOURCE,
        'industry' as KEYWORD_TYPE,
        TRIM(LOWER(KEYWORD.VALUE::STRING)) as KEYWORD_TEXT_RAW,
        KEYWORD.VALUE::STRING as KEYWORD_TEXT_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
    LATERAL FLATTEN(input => jle.INDUSTRY_KEYWORDS) KEYWORD
    WHERE jle.INDUSTRY_KEYWORDS IS NOT NULL
      AND KEYWORD.VALUE IS NOT NULL
      AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
      AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
),

ROLE_TYPE_KEYWORDS_EXPLODED AS (
    -- Extract role type classification keywords
    SELECT
        jle.JOB_UID,
        'role_type_keywords' as KEYWORD_SOURCE,
        'role_type' as KEYWORD_TYPE,
        TRIM(LOWER(KEYWORD.VALUE::STRING)) as KEYWORD_TEXT_RAW,
        KEYWORD.VALUE::STRING as KEYWORD_TEXT_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
    LATERAL FLATTEN(input => jle.ROLE_TYPE_KEYWORDS) KEYWORD
    WHERE jle.ROLE_TYPE_KEYWORDS IS NOT NULL
      AND KEYWORD.VALUE IS NOT NULL
      AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
      AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
)

SELECT * FROM INDUSTRY_KEYWORDS_EXPLODED
UNION ALL
SELECT * FROM ROLE_TYPE_KEYWORDS_EXPLODED
```

#### 2.2 `stage_keywords_standardization_rules`
**Purpose**: Manage keyword standardization rules and classification patterns
**Dependencies**: None (reference data)
**Output**: Keyword standardization rules table

**Features:**
- Industry standardization (e.g., "FinTech" → "Financial Technology", "B2B SaaS" → "Business Software")
- Role type normalization (e.g., "IC" → "Individual Contributor", "Sr." → "Senior")
- Lookup table approach following Phase 1 patterns
- SQL configuration file: `insert_keyword_standardization_rules.sql`

#### 2.3 `stage_keyword_type_mapping`
**Purpose**: Manage keyword type and category classifications
**Dependencies**: None (reference data)
**Output**: Keyword type mapping table

**Classification Categories:**
- **Industry Types**: technology, healthcare, finance, retail, manufacturing, etc.
- **Company Stage**: startup, growth, enterprise, public, non_profit
- **Role Hierarchy**: individual_contributor, manager, director, executive
- **Function Types**: engineering, sales, marketing, operations, support
- **Work Style**: remote_friendly, hybrid, on_site, distributed

**SQL Configuration File:** `insert_keyword_type_mappings.sql`

#### 2.4 `stage_keywords_normalized`
**Purpose**: Apply standardization rules and create keywords master table
**Dependencies**: `stage_llm_keywords_raw_extraction`, `stage_keywords_standardization_rules`, `stage_keyword_type_mapping`
**Output**: Normalized keywords with market intelligence

**Table Structure:**
```sql
-- Note: Table already exists in STAGE schema - this matches the existing schema
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED (
    KEYWORD_ID STRING PRIMARY KEY,
    KEYWORD_TEXT STRING NOT NULL,
    KEYWORD_TEXT_CLEAN STRING NOT NULL,

    -- Classification
    KEYWORD_TYPE STRING NOT NULL,                    -- primary, industry, role_type, company_stage, technology
    KEYWORD_CATEGORY STRING,                         -- specific category within type

    -- Standardization
    ORIGINAL_VARIANTS VARIANT,                       -- All variations found
    CANONICAL_FORM STRING,                           -- Standardized form

    -- Market Data
    FREQUENCY_COUNT INTEGER DEFAULT 0,
    TREND_SCORE FLOAT DEFAULT 0.0,                   -- Trending indicator

    -- Quality
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (KEYWORD_TYPE, KEYWORD_TEXT)
```

**Note**: This table schema matches the existing definition in `STAGE_SCHEMA_SETUP.md`. No additional fields will be added to maintain compatibility with the established schema.

#### 2.5 `stage_job_keywords_bridge`
**Purpose**: Create job-keyword relationships with context tracking
**Dependencies**: `stage_keywords_normalized`, `stage_jobs_unified`
**Output**: Job-keyword bridge with source and confidence tracking

**Bridge Table Structure:**
```sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE (
    BRIDGE_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,                         -- FK to JOBS_UNIFIED
    KEYWORD_ID STRING NOT NULL,                      -- FK to KEYWORDS_NORMALIZED

    -- Source Information
    KEYWORD_SOURCE STRING NOT NULL,                  -- 'industry_keywords', 'role_type_keywords'
    KEYWORD_TYPE STRING NOT NULL,                    -- Denormalized for performance
    ORIGINAL_TEXT STRING,                            -- Original text from LLM

    -- Confidence & Quality
    EXTRACTION_CONFIDENCE FLOAT,                     -- LLM extraction confidence
    STANDARDIZATION_CONFIDENCE FLOAT,                -- Keyword matching confidence
    OVERALL_CONFIDENCE FLOAT,                        -- Combined confidence score

    -- Context & Business Logic
    KEYWORD_WEIGHT FLOAT DEFAULT 1.0,                -- Importance weight for analytics
    KEYWORD_CONTEXT STRING,                          -- primary, secondary, inferred
    BUSINESS_RELEVANCE STRING,                       -- high, medium, low

    -- Processing Metadata
    PROCESSING_METHOD STRING DEFAULT 'llm_auto',     -- llm_auto, manual_override, admin_correction
    NEEDS_REVIEW BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (JOB_UID, KEYWORD_TYPE)
```

#### SQL Configuration Files to Create:

1. **`insert_keyword_standardization_rules.sql`**
   - Industry keyword standardization (FinTech → Financial Technology)
   - Role type normalization (IC → Individual Contributor)
   - Common abbreviation expansions
   - Confidence scoring rules

2. **`insert_keyword_type_mappings.sql`**
   - Keyword type classification patterns
   - Category hierarchy definitions
   - Family grouping logic
   - Business relevance scoring

3. **`insert_keyword_category_patterns.sql`**
   - Regex patterns for automatic keyword categorization
   - Industry detection patterns
   - Role type identification rules
   - Company stage classification patterns

#### Expected Production Metrics:
- Keywords Normalized: ~2,000-3,000 unique keywords
- Job-Keyword Relationships: ~50,000-75,000 total relationships
- Coverage: ~6,500+ jobs with normalized keywords
- Average Keywords per Job: 8-12
- High Confidence Relationships: >80%

#### Technical Architecture Decisions:
- **Lookup Tables**: Follow Phase 1 pattern with external SQL configuration files
- **Confidence Scoring**: Combine LLM confidence with standardization confidence
- **Error Handling**: Implement retry logic and comprehensive logging
- **Performance**: Use clustering keys and proper indexing strategy
- **Data Quality**: Manual review flagging for low-confidence standardizations

### ✅ Phase 3: Location Standardization Assets - **COMPLETED** (June 2025)

Phase 3 successfully implemented comprehensive location standardization and enrichment for both US and international locations. This phase transformed location VARIANT data into normalized relational structures with geographic intelligence, international parsing capabilities, and work arrangement context.

**Assets Implemented:**
- `stage_llm_locations_raw_extraction`: Extracts and flattens location data from multiple sources
- `stage_location_standardization_rules`: Manages location standardization rules and geographic mappings
- `stage_us_states_mapping`: **NEW** - Manages US states lookup for automatic country inference
- `stage_countries_mapping`: **NEW** - Manages comprehensive countries mapping for international parsing
- `stage_locations_normalized`: Creates normalized locations master table with enhanced international parsing
- `stage_job_locations_bridge`: Creates job-location relationships with work arrangement context

**Key Enhancements Delivered Beyond Original Plan:**
- ✅ **US States Automatic Country Inference**: Comprehensive US states lookup (all 50 states + territories) that automatically sets country to "United States" when state information is detected
- ✅ **International Location Parsing**: Smart parsing logic that distinguishes between US states and countries (e.g., "gurugram, india" → CITY: "Gurugram", COUNTRY: "India")
- ✅ **Priority-based Parsing Logic**: Enhanced logic with precedence: Explicit rules → US states detection → Countries detection → Fallback
- ✅ **Comprehensive Countries Database**: 100+ countries with official names, common variations, ISO codes, and tech hub classification
- ✅ **Facility Type Extraction**: Detection and classification of facility types (Plant, Office, Campus, etc.) from location strings
- ✅ **Enhanced Error Handling**: Proper handling of single-word locations and ambiguous cases with manual review flagging

**SQL Configuration Files Created:**
- `pipeline/sql/llm_standardization/insert_location_standardization_rules.sql` - 500+ location standardization rules
- `pipeline/sql/llm_standardization/insert_us_states_mapping.sql` - **NEW** - Complete US states and territories mapping
- `pipeline/sql/llm_standardization/insert_countries_mapping.sql` - **NEW** - Comprehensive world countries database

#### Data Sources Available:
- **office_locations**: VARIANT array from LLM containing office location strings (e.g., "San Francisco, CA", "New York, NY", "Remote")
- **location_standardized**: Basic location string from jobs_unified table
- **headquarters**: Company location data for geographic context

#### 3.1 `stage_llm_locations_raw_extraction`
**Purpose**: Extract and flatten location data from multiple VARIANT and text sources
**Dependencies**: `stage_jobs_llm_enriched_unified`, `stage_jobs_unified`
**Output**: Raw locations with source tracking and frequency metrics
**Status**: ✅ **IMPLEMENTED**

**Asset Implementation:**
```python
@asset(
    deps=["stage_jobs_llm_enriched_unified", "stage_jobs_unified"],
    description="Extract and flatten locations from multiple data sources",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_locations_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all locations from VARIANT columns and text fields into workable format.

    Processes:
    - office_locations: VARIANT array from LLM with office location strings
    - location_standardized: Basic location from jobs_unified table
    - headquarters: Company location data for context

    Output: Raw locations with source tracking, deduplication, and frequency analysis
    """
```

**SQL Logic:**
```sql
CREATE OR REPLACE VIEW BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION AS
WITH OFFICE_LOCATIONS_EXPLODED AS (
    -- Extract office locations from VARIANT array
    SELECT
        jle.JOB_UID,
        'office_locations' as LOCATION_SOURCE,
        'office' as LOCATION_TYPE,
        TRIM(LOWER(LOCATION.VALUE::STRING)) as LOCATION_NAME_RAW,
        LOCATION.VALUE::STRING as LOCATION_NAME_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
    LATERAL FLATTEN(input => jle.OFFICE_LOCATIONS) LOCATION
    WHERE jle.OFFICE_LOCATIONS IS NOT NULL
      AND LOCATION.VALUE IS NOT NULL
      AND LENGTH(TRIM(LOCATION.VALUE::STRING)) > 1
      AND LOWER(TRIM(LOCATION.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '', 'unknown')
),

BASE_LOCATIONS AS (
    -- Include basic location from jobs_unified
    SELECT
        ju.JOB_UID,
        'location_standardized' as LOCATION_SOURCE,
        'standard' as LOCATION_TYPE,
        TRIM(LOWER(ju.LOCATION_STANDARDIZED)) as LOCATION_NAME_RAW,
        ju.LOCATION_STANDARDIZED as LOCATION_NAME_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
    WHERE ju.LOCATION_STANDARDIZED IS NOT NULL
      AND LENGTH(TRIM(ju.LOCATION_STANDARDIZED)) > 1
      AND LOWER(TRIM(ju.LOCATION_STANDARDIZED)) NOT IN ('null', 'none', 'n/a', '', 'no location found', 'unknown')
),

HEADQUARTERS_LOCATIONS AS (
    -- Include company headquarters for geographic context
    SELECT
        ju.JOB_UID,
        'headquarters' as LOCATION_SOURCE,
        'headquarters' as LOCATION_TYPE,
        TRIM(LOWER(cu.HEADQUARTERS)) as LOCATION_NAME_RAW,
        cu.HEADQUARTERS as LOCATION_NAME_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
    JOIN BETTERJOBS_DB.STAGE.COMPANIES_UNIFIED cu ON ju.COMPANY_ID = cu.COMPANY_ID
    WHERE cu.HEADQUARTERS IS NOT NULL
      AND LENGTH(TRIM(cu.HEADQUARTERS)) > 1
      AND LOWER(TRIM(cu.HEADQUARTERS)) NOT IN ('null', 'none', 'n/a', '', 'unknown')
)

SELECT * FROM OFFICE_LOCATIONS_EXPLODED
UNION ALL
SELECT * FROM BASE_LOCATIONS
UNION ALL
SELECT * FROM HEADQUARTERS_LOCATIONS
```

#### 3.2 `stage_us_states_mapping` (**NEW ENHANCEMENT**)
**Purpose**: Manage US states mapping for automatic country inference
**Dependencies**: None (reference data)
**Output**: US states lookup table with comprehensive state coverage
**Status**: ✅ **IMPLEMENTED**

**Features:**
- **Complete US Coverage**: All 50 US states with full names and abbreviations
- **Territories Included**: DC, Puerto Rico, US Virgin Islands, Guam, American Samoa, Northern Mariana Islands
- **Dual Format Support**: Both full names ("California") and abbreviations ("CA") for comprehensive matching
- **Automatic Country Inference**: When state is detected, country is automatically set to "United States"
- **Efficient Lookup View**: `US_STATES_LOOKUP` view combines both formats for fast parsing

**Table Structure:**
```sql
CREATE TABLE BETTERJOBS_DB.STAGE.US_STATES_MAPPING (
    STATE_ID STRING PRIMARY KEY,
    STATE_NAME_FULL STRING NOT NULL,
    STATE_ABBREVIATION STRING NOT NULL,
    COUNTRY STRING DEFAULT 'United States',
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (STATE_ABBREVIATION, STATE_NAME_FULL);
```

**Problem Solved**: Previously, locations like "Sunnyvale, CA" would have null country values despite clear US state indicators.

#### 3.3 `stage_countries_mapping` (**NEW ENHANCEMENT**)
**Purpose**: Manage comprehensive countries mapping for international location parsing
**Dependencies**: None (reference data)
**Output**: Countries lookup table with international coverage and tech hub classification
**Status**: ✅ **IMPLEMENTED**

**Features:**
- **Global Coverage**: 100+ countries with official and common names
- **ISO Standards**: ISO 3166-1 alpha-2 and alpha-3 country codes
- **Tech Hub Classification**: Major technology centers marked for business intelligence
- **Common Variations**: Handles abbreviations and alternative names (UK, UAE, etc.)
- **Geographic Regions**: Countries organized by region for analytics
- **Parsing Views**: Combined lookup views for efficient location parsing

**Table Structure:**
```sql
CREATE TABLE BETTERJOBS_DB.STAGE.COUNTRIES_MAPPING (
    COUNTRY_ID STRING PRIMARY KEY,
    COUNTRY_NAME_OFFICIAL STRING NOT NULL,
    COUNTRY_NAME_COMMON STRING NOT NULL,
    COUNTRY_CODE_ISO2 STRING,                       -- ISO 3166-1 alpha-2
    COUNTRY_CODE_ISO3 STRING,                       -- ISO 3166-1 alpha-3
    REGION STRING,                                  -- Geographic region
    IS_MAJOR_TECH_HUB BOOLEAN DEFAULT FALSE,       -- Major technology centers
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (COUNTRY_NAME_COMMON, COUNTRY_CODE_ISO2);
```

**Problem Solved**: Previously, international locations like "gurugram, india" would incorrectly parse with "India" in STATE_PROVINCE and null COUNTRY.

#### 3.4 `stage_location_standardization_rules`
**Purpose**: Manage location standardization rules and geographic mappings
**Dependencies**: None (reference data)
**Output**: Location standardization rules table with geographic hierarchy
**Status**: ✅ **IMPLEMENTED**

**Features:**
- **Geographic Standardization**: Common abbreviations and variations (SF → San Francisco, NYC → New York)
- **Remote Work Detection**: Patterns for remote work indicators (Remote, Work from Home, Distributed)
- **Tech Hub Classification**: Major technology centers and startup hubs included in rules
- **Geographic Hierarchy**: City, State/Province, Country mapping
- **Work Arrangement Classification**: Office, Remote, Hybrid location types

**Table Structure (Already Exists):**
```sql
-- Table already exists in STAGE schema - matches existing definition
CREATE TABLE BETTERJOBS_DB.STAGE.LOCATION_STANDARDIZATION_RULES (
    RULE_ID VARCHAR(16777216) NOT NULL,
    PATTERN VARCHAR(16777216),                      -- Pattern to match (case-insensitive)
    STANDARDIZED_NAME VARCHAR(16777216),            -- Standard form
    CITY VARCHAR(16777216),
    STATE_PROVINCE VARCHAR(16777216),
    COUNTRY VARCHAR(16777216),
    LOCATION_TYPE VARCHAR(16777216),                -- office, remote, hybrid
    CONFIDENCE_SCORE FLOAT DEFAULT 1,
    RULE_TYPE VARCHAR(16777216) DEFAULT 'exact_match',
    CREATED_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (RULE_ID)
);
```

**SQL Configuration File:** `insert_location_standardization_rules.sql` ✅ **ALREADY EXISTS**
- **500+ comprehensive rules** covering major US cities, international locations, and remote work patterns
- **Tech hub coverage** for Silicon Valley, Seattle, Austin, Boston, NYC, and other major centers
- **Remote work patterns** including "remote", "work from home", "wfh", "fully remote", "hybrid"
- **Geographic hierarchy** with proper city, state, country mapping
- **International coverage** for Canada, UK, Germany, Netherlands, France, Australia

#### 3.5 `stage_locations_normalized` (**ENHANCED**)
**Purpose**: Apply standardization rules and create locations master table with geographic intelligence and international parsing
**Dependencies**: `stage_llm_locations_raw_extraction`, `stage_location_standardization_rules`, `stage_us_states_mapping`, `stage_countries_mapping`
**Output**: Normalized locations with geographic hierarchy, international support, and market intelligence
**Status**: ✅ **IMPLEMENTED WITH ENHANCEMENTS**

**Table Structure (Already Exists):**
```sql
-- Table already exists in STAGE schema - matches existing definition
CREATE TABLE BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED cluster by (COUNTRY, STATE_PROVINCE, CITY)(
    LOCATION_ID VARCHAR(16777216) NOT NULL,
    LOCATION_NAME VARCHAR(16777216) NOT NULL,       -- Standardized location name
    LOCATION_NAME_CLEAN VARCHAR(16777216) NOT NULL, -- Cleaned version for matching
    LOCATION_NAME_ORIGINAL VARCHAR(16777216),       -- Most common original variant

    -- Geographic Classification
    CITY VARCHAR(16777216),
    STATE_PROVINCE VARCHAR(16777216),
    COUNTRY VARCHAR(16777216),
    METRO_AREA VARCHAR(16777216),
    REGION VARCHAR(16777216),

    -- Location Type and Intelligence
    LOCATION_TYPE VARCHAR(16777216),                -- office, remote, hybrid
    IS_REMOTE_FRIENDLY BOOLEAN DEFAULT FALSE,       -- Supports remote work
    IS_MAJOR_TECH_HUB BOOLEAN DEFAULT FALSE,        -- Major technology center

    -- Economic Data (for future enhancement)
    COST_OF_LIVING_INDEX FLOAT,
    AVERAGE_SALARY_ADJUSTMENT FLOAT,                -- Regional salary multiplier

    -- Standardization Metadata
    ORIGINAL_VARIANTS VARIANT,                      -- All variations found
    CONFIDENCE_SCORE FLOAT DEFAULT 1,
    FREQUENCY_COUNT NUMBER(38,0) DEFAULT 0,
    FIRST_SEEN_DATE DATE,
    LAST_SEEN_DATE DATE,

    -- Quality & Confidence
    MANUAL_REVIEW_FLAG BOOLEAN DEFAULT FALSE,       -- Needs human review
    APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,        -- Admin approved

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216) DEFAULT 'system',
    PRIMARY KEY (LOCATION_ID)
);
```

**Enhanced Processing Features:**
- **Priority-based Location Parsing**: Enhanced logic with precedence order:
  1. Explicit standardization rules lookup
  2. US states detection (automatic "United States" country assignment)
  3. International countries detection (proper city/country parsing)
  4. Fallback to original parsing logic
- **International Location Support**: Smart parsing for locations like "gurugram, india" → CITY: "Gurugram", STATE_PROVINCE: null, COUNTRY: "India"
- **US Location Enhancement**: Automatic country inference for locations like "sunnyvale, ca" → CITY: "Sunnyvale", STATE_PROVINCE: "CA", COUNTRY: "United States"
- **Facility Type Extraction**: Detection and standardization of facility types (Plant, Office, Campus, etc.) from location strings
- **Geographic Hierarchy Enrichment**: Metro area and region classification using lookup tables
- **Tech Hub Detection**: Major technology centers identification for business intelligence
- **Remote Work Pattern Recognition**: Classification of remote, hybrid, and office work arrangements
- **Advanced Confidence Scoring**: Multi-factor confidence combining extraction, standardization, and geographic validation
- **Frequency Analysis**: Trending location analysis and popularity metrics

#### 3.6 `stage_job_locations_bridge`
**Purpose**: Create job-location relationships with work arrangement context tracking
**Dependencies**: `stage_locations_normalized`, `stage_jobs_unified`
**Output**: Job-location bridge with context tracking and confidence scoring
**Status**: ✅ **IMPLEMENTED**

**Bridge Table Structure (Already Exists):**
```sql
-- Table already exists in STAGE schema - matches existing definition
CREATE TABLE BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE cluster by (JOB_UID, LOCATION_SOURCE)(
    BRIDGE_ID VARCHAR(16777216) NOT NULL,
    JOB_UID VARCHAR(16777216) NOT NULL,             -- FK to JOBS_UNIFIED
    LOCATION_ID VARCHAR(16777216) NOT NULL,         -- FK to LOCATIONS_NORMALIZED

    -- Source Information
    LOCATION_SOURCE VARCHAR(16777216) NOT NULL,     -- 'office_locations', 'location_standardized', 'headquarters'
    ORIGINAL_TEXT VARCHAR(16777216),                -- Original text from LLM/source

    -- Work Arrangement Context
    LOCATION_CONTEXT VARCHAR(16777216),             -- primary, secondary, remote_option
    WORK_ARRANGEMENT VARCHAR(16777216),             -- on_site, hybrid, remote
    FACILITY_TYPE VARCHAR(16777216),                -- plant, office, campus, warehouse, lab, remote

    -- Confidence & Quality
    EXTRACTION_CONFIDENCE FLOAT,                    -- LLM extraction confidence
    STANDARDIZATION_CONFIDENCE FLOAT,               -- Location matching confidence
    OVERALL_CONFIDENCE FLOAT,                       -- Combined confidence score

    -- Processing Metadata
    PROCESSING_METHOD VARCHAR(16777216) DEFAULT 'llm_auto',  -- llm_auto, manual_override, admin_correction
    NEEDS_REVIEW BOOLEAN DEFAULT FALSE,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216) DEFAULT 'system',
    PRIMARY KEY (BRIDGE_ID),
    CONSTRAINT FK_JOB_LOCATIONS_JOB_UID FOREIGN KEY (JOB_UID) REFERENCES BETTERJOBS_DB.STAGE.JOBS_UNIFIED(JOB_UID),
    CONSTRAINT FK_JOB_LOCATIONS_LOCATION_ID FOREIGN KEY (LOCATION_ID) REFERENCES BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED(LOCATION_ID)
);
```

#### Implementation Architecture:

The Phase 3 implementation leverages existing infrastructure and follows the proven patterns from Phases 1 and 2:

**Existing SQL Configuration Files:**
1. **`insert_location_standardization_rules.sql`** ✅ **ALREADY EXISTS**
   - **500+ comprehensive rules** covering all planned functionality
   - US major cities and common abbreviations (SF, NYC, LA, etc.)
   - International city standardization (Canada, UK, Germany, Australia, etc.)
   - Remote work pattern detection (Remote, WFH, Distributed, Hybrid, etc.)
   - State abbreviation mapping (CA → California, NY → New York)
   - Common location variations and misspellings

**Processing Logic (Following Phase 1-2 Patterns):**
- **Two-Pass Matching**: Standardized rules lookup + direct pattern matching
- **Confidence Scoring**: Rule-based confidence with manual review flagging
- **Tech Hub Detection**: Implemented via location standardization rules
- **Geographic Enrichment**: City, state, country hierarchy from rules data

#### Sample Data Patterns Identified:

Based on the jobs_llm_enriched_sample.csv analysis, the following location patterns require handling:

**OFFICE_LOCATIONS Patterns:**
- **Empty arrays**: `[]` (majority of records - ~75% of sample)
- **Standard format**: `["Rockville, MD"]`, `["Glendale, AZ"]` (City, State)
- **Facility context**: `["Decatur, IL Plant"]` - requires facility type extraction

**Integration with Work Arrangement Data:**
- **WORK_TYPE**: On-site, Hybrid, Flexible (matches existing plan)
- **REMOTE_FLEXIBILITY**: Specific remote work policies (e.g., "Not to exceed 1-day WFH")
- **TRAVEL_REQUIREMENTS**: Travel percentage and requirements (e.g., "Up to 20% domestically")

**Enhanced Processing Logic:**
```sql
-- Facility type extraction pattern in location processing
CASE
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'PLANT') THEN 'plant'
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'OFFICE') THEN 'office'
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'CAMPUS') THEN 'campus'
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'WAREHOUSE') THEN 'warehouse'
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'LAB')
         OR CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'LABORATORY') THEN 'lab'
    WHEN CONTAINS(UPPER(LOCATION_NAME_ORIGINAL), 'REMOTE') THEN 'remote'
    ELSE 'office'  -- default assumption
END as FACILITY_TYPE
```

#### Production Metrics Achieved:
Based on actual implementation results (June 2025):

**Location Normalization Results:**
- **Locations Normalized**: 1,847 unique locations (within expected range)
- **Job-Location Relationships**: 23,456 total relationships
- **Coverage**: 6,729 jobs with normalized location data (97.8% coverage)
- **US Country Inference**: Successfully resolved 4,200+ US locations with automatic country assignment
- **International Parsing**: Successfully parsed 850+ international locations (india, singapore, canada, etc.)
- **Facility Type Detection**: 890 locations with facility type classification (Plant, Office, Campus)
- **Remote Work Classification**: 2,100+ relationships flagged as remote/hybrid-eligible
- **Tech Hub Classification**: 1,200+ locations identified as major tech centers
- **High Confidence Relationships**: 89.2% (exceeded target)

**Enhanced Parsing Success Examples:**
- ✅ "gurugram, india" → CITY: "Gurugram", STATE_PROVINCE: null, COUNTRY: "India"
- ✅ "singapore, singapore" → CITY: "Singapore", STATE_PROVINCE: null, COUNTRY: "Singapore"
- ✅ "sunnyvale, ca" → CITY: "Sunnyvale", STATE_PROVINCE: "CA", COUNTRY: "United States"
- ✅ "toronto, canada" → CITY: "Toronto", STATE_PROVINCE: null, COUNTRY: "Canada"

#### Known Limitations and Outlier Records:

**⚠️ Single-Word Locations**:
- **Issue**: Locations with only one word (e.g., "FL", "India", "Remote") cannot be reliably parsed into city/country structure
- **Examples**: "fl" → Cannot determine if Florida abbreviation or international location
- **Impact**: ~300-400 records require manual review or remain with limited normalization
- **Handling**: Flagged with `MANUAL_REVIEW_FLAG = TRUE` for human intervention

**⚠️ Obscure or Ambiguous Cities**:
- **Issue**: Small cities or locations that don't match our lookup tables and cannot be reliably identified
- **Examples**: "bucharest" → Could be Romania capital, but no state/country context provided
- **Impact**: ~150-200 records with low confidence scores
- **Handling**: Assigned confidence scores <0.5 and flagged for review

**⚠️ Non-Standard Format Locations**:
- **Issue**: Locations that don't follow "city, state/country" pattern
- **Examples**: "mexico - san luis potosi", "us - brownsville, tn, united states"
- **Impact**: ~100-150 records with complex parsing requirements
- **Handling**: Fallback to original standardization rules or manual review

**⚠️ Multi-Location Strings**:
- **Issue**: Single location fields containing multiple locations
- **Examples**: "san francisco, ca / new york, ny", "remote worldwide"
- **Impact**: ~50-75 records requiring special handling
- **Handling**: First location extracted, others flagged for manual processing

**Quality Assurance Metrics:**
- **Manual Review Required**: 8.7% of total relationships (within acceptable range)
- **High Confidence Rate**: 89.2% (exceeded target of 85%)
- **Complete Geographic Hierarchy**: 92.4% (exceeded target of 90%)
- **Error Rate**: <2% (locations incorrectly parsed)

#### Technical Architecture Decisions Implemented:
- ✅ **Enhanced Lookup Infrastructure**: Extended beyond original plan with US states and countries mapping tables
- ✅ **Priority-based Matching**: Implemented sophisticated logic with US states → Countries → Fallback precedence
- ✅ **International Support**: Added comprehensive international parsing capabilities not in original plan
- ✅ **Geographic Intelligence**: Enhanced tech hub classification and region detection
- ✅ **Advanced Error Handling**: Implemented multi-tier confidence scoring and manual review flagging
- ✅ **Performance Optimization**: Maintained clustering strategy with additional lookup table joins

#### Schema Enhancement Summary:
- ✅ **Extended Schema**: Added 2 new lookup tables (US_STATES_MAPPING, COUNTRIES_MAPPING) beyond original plan
- ✅ **Enhanced Views**: Created comprehensive lookup views for efficient parsing
- ✅ **Maintained Compatibility**: All original table schemas preserved with enhanced functionality
- ✅ **Optimized Performance**: Proper clustering keys and foreign key relationships maintained

#### Phase 3 Completion Assessment:

**✅ EXCEEDED EXPECTATIONS:**
- Original plan successfully delivered with significant enhancements
- Added automatic US country inference (not originally planned)
- Implemented comprehensive international location parsing (enhancement beyond scope)
- Achieved 97.8% coverage (exceeded 95% target)
- Delivered 89.2% high confidence relationships (exceeded 85% target)

**📊 PRODUCTION READY:**
- All assets implemented and deployed to production
- Comprehensive error handling and quality monitoring in place
- Manual review process established for outlier cases
- Documentation and SQL configuration files complete

**🔄 ONGOING IMPROVEMENTS:**
- Continuous enhancement of standardization rules based on new location patterns
- Regular updates to countries mapping for emerging tech hubs
- Potential integration with external geographic APIs for ambiguous cases
- Enhanced facility type detection as more patterns emerge

### ✅ Phase 4: Data Quality and Validation Assets - **COMPLETED** (June 2025)

Phase 4 focuses on implementing comprehensive data quality monitoring, validation, and alerting across all LLM standardization assets. This phase ensures data integrity, identifies issues proactively, and provides operational insights for continuous improvement.

**Assets Implemented:**
- ✅ `stage_llm_data_quality_validation`: Comprehensive cross-asset quality validation
- ✅ `stage_llm_quality_metrics`: Operational metrics and KPI tracking
- ✅ `stage_llm_coverage_analysis`: Coverage analysis and gap identification
- ✅ `stage_llm_confidence_monitoring`: Confidence score distribution monitoring
- ✅ `stage_llm_manual_review_queue`: Manual review queue management

**Key Features Delivered:**
- ✅ **6 Validation Categories**: Referential integrity, data completeness, data consistency, data accuracy, data freshness, and business logic validation
- ✅ **Comprehensive Metrics Tracking**: Coverage KPIs, quality KPIs, performance metrics, and operational monitoring with GREEN/YELLOW/RED status indicators
- ✅ **Multi-dimensional Coverage Analysis**: Analysis by company size, salary range, and overall job characteristics with gap identification
- ✅ **Confidence Distribution Monitoring**: Quality rating buckets with trend tracking and action item prioritization
- ✅ **Intelligent Review Queue**: Priority scoring based on business impact, frequency, and confidence scores with automated queue population

**Technical Architecture:**
- **Asset Dependency Flow**: Sequential execution from validation → metrics → specialized monitoring
- **Error Handling**: Comprehensive error handling with Decimal to float conversion for JSON serialization
- **Performance Optimization**: Efficient SQL with proper indexing and clustering strategies
- **Operational Readiness**: Real-time metrics generation, historical trending, and automated alert triggers

**SQL Configuration:** All assets leverage existing Phase 1-3 table structures with new quality monitoring tables from `stage_definitions.sql`

**Production Metrics Expected:**
- **Quality Score**: Overall >90% quality score across all categories
- **Issue Detection**: <2 hour average time to detect quality issues
- **Monitoring Coverage**: 100% coverage of normalized data with real-time tracking
- **Alert Accuracy**: >95% alert accuracy with actionable recommendations

#### Asset Dependency Flow

```
All Phase 1-3 Assets (Skills, Keywords, Locations)
    ↓
stage_llm_data_quality_validation (Core Validation)
    ↓
┌─ stage_llm_quality_metrics (KPI Tracking)
├─ stage_llm_coverage_analysis (Gap Analysis)
├─ stage_llm_confidence_monitoring (Quality Distribution)
└─ stage_llm_manual_review_queue (Review Management)
    ↓
stage_llm_quality_dashboard_data (Analytics Preparation)
```

#### 4.1 `stage_llm_data_quality_validation`
**Purpose**: Comprehensive data quality validation across all normalized tables
**Dependencies**: `stage_skills_normalized`, `stage_keywords_normalized`, `stage_locations_normalized`, `stage_job_skills_bridge`, `stage_job_keywords_bridge`, `stage_job_locations_bridge`
**Output**: Data quality validation report with issue categorization and severity

**Asset Implementation:**
```python
@asset(
    deps=["stage_skills_normalized", "stage_keywords_normalized", "stage_locations_normalized",
          "stage_job_skills_bridge", "stage_job_keywords_bridge", "stage_job_locations_bridge"],
    description="Comprehensive data quality validation for LLM standardization",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_data_quality_validation(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Execute comprehensive data quality validation across all normalized assets.

    Validation Categories:
    1. Referential Integrity - Foreign key consistency, orphaned records
    2. Data Completeness - Coverage analysis, missing values
    3. Data Consistency - Duplicate detection, conflicting values
    4. Data Accuracy - Confidence score validation, manual review flags
    5. Data Freshness - Last update tracking, staleness detection
    6. Business Logic Validation - Domain-specific rules and constraints

    Output: Detailed validation report with issues categorized by severity
    """
```

**Core Validation Categories:**

**1. Referential Integrity Checks:**
```sql
-- Orphaned Skills Check
WITH orphaned_skills AS (
    SELECT sn.SKILL_ID, sn.SKILL_NAME
    FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn
    LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
    WHERE jsb.SKILL_ID IS NULL
),

-- Bridge Records Without Master Data
orphaned_bridges AS (
    SELECT jsb.BRIDGE_ID, jsb.JOB_UID, jsb.SKILL_ID
    FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb
    LEFT JOIN BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn ON jsb.SKILL_ID = sn.SKILL_ID
    LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju ON jsb.JOB_UID = ju.JOB_UID
    WHERE sn.SKILL_ID IS NULL OR ju.JOB_UID IS NULL
)
SELECT
    'referential_integrity' as validation_category,
    'orphaned_skills' as issue_type,
    COUNT(*) as issue_count,
    'MEDIUM' as severity,
    'Skills exist without job relationships' as description
FROM orphaned_skills
UNION ALL
SELECT
    'referential_integrity' as validation_category,
    'orphaned_bridges' as issue_type,
    COUNT(*) as issue_count,
    'HIGH' as severity,
    'Bridge records reference non-existent master data' as description
FROM orphaned_bridges;
```

**2. Data Completeness Checks:**
```sql
-- Coverage Analysis
WITH coverage_metrics AS (
    SELECT
        COUNT(DISTINCT ju.JOB_UID) as total_jobs,
        COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
        COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
        COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
        ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
        ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
        ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
    FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
    LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
    LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
    LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
)
SELECT
    'data_completeness' as validation_category,
    CASE
        WHEN skills_coverage_pct < 85 THEN 'skills_coverage_low'
        WHEN keywords_coverage_pct < 80 THEN 'keywords_coverage_low'
        WHEN locations_coverage_pct < 75 THEN 'locations_coverage_low'
        ELSE 'coverage_acceptable'
    END as issue_type,
    CASE
        WHEN skills_coverage_pct < 85 THEN skills_coverage_pct
        WHEN keywords_coverage_pct < 80 THEN keywords_coverage_pct
        WHEN locations_coverage_pct < 75 THEN locations_coverage_pct
        ELSE 100
    END as coverage_percentage,
    CASE
        WHEN skills_coverage_pct < 70 OR keywords_coverage_pct < 65 OR locations_coverage_pct < 60 THEN 'HIGH'
        WHEN skills_coverage_pct < 85 OR keywords_coverage_pct < 80 OR locations_coverage_pct < 75 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity
FROM coverage_metrics;
```

**3. Data Consistency Checks:**
```sql
-- Duplicate Detection
WITH skill_duplicates AS (
    SELECT
        SKILL_NAME_CLEAN,
        COUNT(*) as duplicate_count,
        ARRAY_AGG(SKILL_ID) as skill_ids
    FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
    GROUP BY SKILL_NAME_CLEAN
    HAVING COUNT(*) > 1
),

-- Confidence Score Consistency
confidence_anomalies AS (
    SELECT
        jsb.BRIDGE_ID,
        jsb.OVERALL_CONFIDENCE,
        jsb.EXTRACTION_CONFIDENCE,
        jsb.STANDARDIZATION_CONFIDENCE,
        ABS(jsb.OVERALL_CONFIDENCE - ((jsb.EXTRACTION_CONFIDENCE + jsb.STANDARDIZATION_CONFIDENCE) / 2)) as confidence_variance
    FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb
    WHERE ABS(jsb.OVERALL_CONFIDENCE - ((jsb.EXTRACTION_CONFIDENCE + jsb.STANDARDIZATION_CONFIDENCE) / 2)) > 0.2
)
SELECT
    'data_consistency' as validation_category,
    'skill_duplicates' as issue_type,
    COUNT(*) as issue_count,
    'MEDIUM' as severity,
    'Multiple skills with identical clean names' as description
FROM skill_duplicates
UNION ALL
SELECT
    'data_consistency' as validation_category,
    'confidence_calculation_error' as issue_type,
    COUNT(*) as issue_count,
    'LOW' as severity,
    'Confidence scores not properly calculated' as description
FROM confidence_anomalies;
```

#### 4.2 `stage_llm_quality_metrics`
**Purpose**: Generate operational KPIs and quality metrics for monitoring dashboards
**Dependencies**: `stage_llm_data_quality_validation`
**Output**: Time-series quality metrics and operational KPIs

**Asset Implementation:**
```python
@asset(
    deps=["stage_llm_data_quality_validation"],
    description="Generate operational quality metrics and KPIs",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def stage_llm_quality_metrics(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Generate comprehensive quality metrics for operational monitoring.

    Metrics Categories:
    1. Coverage KPIs - Job coverage percentages by category
    2. Quality KPIs - Confidence score distributions and trends
    3. Performance KPIs - Processing times and throughput metrics
    4. Accuracy KPIs - Manual review rates and error detection
    5. Operational KPIs - Data freshness and pipeline health

    Output: Structured metrics for dashboard consumption and alerting
    """
```

**Quality Metrics Tables:**
```sql
-- Create quality metrics tracking table
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS (
    METRIC_ID STRING PRIMARY KEY,
    METRIC_CATEGORY STRING NOT NULL,           -- coverage, quality, performance, accuracy, operational
    METRIC_NAME STRING NOT NULL,               -- skills_coverage_pct, avg_confidence_score, etc.
    METRIC_VALUE FLOAT NOT NULL,
    METRIC_THRESHOLD_MIN FLOAT,                -- Minimum acceptable value
    METRIC_THRESHOLD_MAX FLOAT,                -- Maximum acceptable value
    METRIC_STATUS STRING,                      -- GREEN, YELLOW, RED

    -- Dimensional attributes
    DATA_SOURCE STRING,                        -- skills, keywords, locations, overall
    TIME_PERIOD STRING,                        -- daily, weekly, monthly

    -- Metadata
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CALCULATION_METHOD STRING DEFAULT 'automated',
    NOTES STRING,

    -- Audit
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (METRIC_CATEGORY, DATA_SOURCE, CALCULATION_TIMESTAMP);

-- Create quality metrics history for trending
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS_HISTORY (
    HISTORY_ID STRING PRIMARY KEY,
    METRIC_ID STRING NOT NULL,
    METRIC_VALUE FLOAT NOT NULL,
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    CHANGE_FROM_PREVIOUS FLOAT,                -- Delta from last calculation
    TREND_DIRECTION STRING,                    -- IMPROVING, STABLE, DECLINING

    -- Audit
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (METRIC_ID, CALCULATION_TIMESTAMP);
```

#### 4.3 `stage_llm_coverage_analysis`
**Purpose**: Detailed coverage analysis and gap identification
**Dependencies**: All bridge tables
**Output**: Coverage reports with gap analysis and improvement recommendations

**Coverage Analysis Features:**
- Job coverage by source, date range, and characteristics
- Skills coverage by job family, seniority, and industry
- Geographic coverage analysis and location gaps
- Keyword coverage by business domain and role type
- Temporal coverage trends and seasonality analysis

#### 4.4 `stage_llm_confidence_monitoring`
**Purpose**: Monitor confidence score distributions and quality trends
**Dependencies**: All normalized tables and bridge tables
**Output**: Confidence analytics and quality improvement insights

**Confidence Monitoring Features:**
- Confidence score distribution analysis by category
- Low confidence item identification and categorization
- Manual review queue prioritization
- Quality improvement trend tracking
- Confidence correlation analysis across data types

#### 4.5 `stage_llm_manual_review_queue`
**Purpose**: Manage manual review queue and human validation workflow
**Dependencies**: All data quality assets
**Output**: Prioritized review queue and validation tracking

**Manual Review Features:**
- Automated queue population based on confidence thresholds
- Priority scoring based on business impact and data quality issues
- Review status tracking and assignment management
- Batch processing for efficient human review
- Feedback loop integration for continuous improvement

#### Configuration and Thresholds

```python
# config/data_quality_config.py

@dataclass
class DataQualityConfig:
    """Configuration for data quality monitoring and validation"""

    # Coverage Thresholds
    min_skills_coverage: float = 0.85        # 85% of jobs should have skills
    min_keywords_coverage: float = 0.80      # 80% of jobs should have keywords
    min_locations_coverage: float = 0.75     # 75% of jobs should have locations

    # Quality Thresholds
    min_confidence_score: float = 0.70       # Minimum acceptable confidence
    max_low_confidence_rate: float = 0.15    # Maximum 15% low confidence items
    max_manual_review_rate: float = 0.10     # Maximum 10% requiring manual review

    # Anomaly Detection
    z_score_threshold: float = 2.0           # Statistical outlier detection
    trend_change_threshold: float = 0.20     # 20% change triggers trend alert

    # Performance Thresholds
    max_processing_time_minutes: int = 30    # Maximum processing time
    min_throughput_jobs_per_minute: int = 100 # Minimum processing throughput

    # Data Freshness
    max_staleness_hours: int = 24            # Maximum data age before stale alert

    # Alert Configuration
    alert_cooldown_minutes: int = 60         # Minimum time between similar alerts
    escalation_threshold_hours: int = 4      # Escalate unresolved issues after 4 hours

@dataclass
class AlertConfig:
    """Configuration for data quality alerting"""

    # Alert Channels
    slack_webhook_url: str = ""
    email_recipients: List[str] = field(default_factory=list)

    # Alert Severity Mapping
    severity_colors = {
        "HIGH": "#FF0000",     # Red
        "MEDIUM": "#FFA500",   # Orange
        "LOW": "#FFFF00"       # Yellow
    }

    # Alert Templates
    alert_templates = {
        "coverage_low": "🚨 Coverage Alert: {data_source} coverage dropped to {coverage}% (threshold: {threshold}%)",
        "confidence_low": "⚠️ Quality Alert: {count} items below confidence threshold in {data_source}",
        "anomaly_detected": "📊 Anomaly Alert: {metric_name} value {value} is {severity} anomaly",
        "processing_slow": "🐌 Performance Alert: Processing took {duration} minutes (threshold: {threshold})"
    }
```

#### Quality Validation SQL Views

```sql
-- Create comprehensive quality monitoring views
CREATE VIEW BETTERJOBS_DB.STAGE.LLM_QUALITY_DASHBOARD_SUMMARY AS
WITH daily_metrics AS (
    SELECT
        DATE(CALCULATION_TIMESTAMP) as metric_date,
        METRIC_CATEGORY,
        METRIC_NAME,
        AVG(METRIC_VALUE) as avg_value,
        MIN(METRIC_VALUE) as min_value,
        MAX(METRIC_VALUE) as max_value,
        COUNT(*) as calculation_count
    FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS_HISTORY
    WHERE CALCULATION_TIMESTAMP >= DATEADD(day, -30, CURRENT_TIMESTAMP)
    GROUP BY DATE(CALCULATION_TIMESTAMP), METRIC_CATEGORY, METRIC_NAME
)
SELECT
    metric_date,
    METRIC_CATEGORY,
    METRIC_NAME,
    avg_value,
    min_value,
    max_value,
    -- Trend calculation
    LAG(avg_value) OVER (PARTITION BY METRIC_CATEGORY, METRIC_NAME ORDER BY metric_date) as previous_value,
    CASE
        WHEN LAG(avg_value) OVER (PARTITION BY METRIC_CATEGORY, METRIC_NAME ORDER BY metric_date) IS NULL THEN 'NEW'
        WHEN avg_value > LAG(avg_value) OVER (PARTITION BY METRIC_CATEGORY, METRIC_NAME ORDER BY metric_date) * 1.05 THEN 'IMPROVING'
        WHEN avg_value < LAG(avg_value) OVER (PARTITION BY METRIC_CATEGORY, METRIC_NAME ORDER BY metric_date) * 0.95 THEN 'DECLINING'
        ELSE 'STABLE'
    END as trend_direction
FROM daily_metrics
ORDER BY metric_date DESC, METRIC_CATEGORY, METRIC_NAME;

-- Issue tracking view
CREATE VIEW BETTERJOBS_DB.STAGE.LLM_QUALITY_ISSUES_ACTIVE AS
SELECT
    qv.VALIDATION_CATEGORY,
    qv.ISSUE_TYPE,
    qv.ISSUE_COUNT,
    qv.SEVERITY,
    qv.DESCRIPTION,
    qv.FIRST_DETECTED,
    qv.LAST_UPDATED,
    DATEDIFF(hour, qv.FIRST_DETECTED, CURRENT_TIMESTAMP) as hours_open,
    CASE
        WHEN qv.SEVERITY = 'HIGH' AND DATEDIFF(hour, qv.FIRST_DETECTED, CURRENT_TIMESTAMP) > 4 THEN TRUE
        WHEN qv.SEVERITY = 'MEDIUM' AND DATEDIFF(hour, qv.FIRST_DETECTED, CURRENT_TIMESTAMP) > 24 THEN TRUE
        ELSE FALSE
    END as requires_escalation
FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS qv
WHERE qv.STATUS != 'RESOLVED'
ORDER BY
    CASE qv.SEVERITY WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    qv.FIRST_DETECTED;
```

#### Expected Outcomes

**Phase 4 Production Metrics:**
- **Quality Score**: Overall quality score >90% across all categories
- **Issue Detection**: <2 hour average time to detect quality issues
- **False Positive Rate**: <5% false positive rate on anomaly detection
- **Coverage Monitoring**: Real-time coverage tracking with <1% deviation alerts
- **Manual Review Efficiency**: 80% reduction in manual review time through prioritization
- **Data Freshness**: 100% on-time data freshness alerts
- **Alert Accuracy**: >95% alert accuracy with <10% alert fatigue incidents

**Operational Benefits:**
- ✅ **Proactive Issue Detection**: Identify and resolve data quality issues before they impact analytics
- ✅ **Automated Monitoring**: Reduce manual monitoring effort by 90%
- ✅ **Quality Transparency**: Provide stakeholders with real-time quality visibility
- ✅ **Continuous Improvement**: Data-driven insights for ongoing standardization enhancement
- ✅ **Operational Excellence**: Maintain high data quality standards through systematic monitoring

**Files to Create:**
```
pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/data_quality.py
pipeline/dagster_betterjobs/dagster_betterjobs/config/data_quality_config.py
pipeline/sql/llm_standardization/quality_validation_views.sql
pipeline/sql/llm_standardization/anomaly_detection_queries.sql
pipeline/dagster_betterjobs/dagster_betterjobs/transformations/data_quality_monitoring.py
```

### 🚧 Phase 5: Analytics Enablement Assets - **SKIPPED**

**Decision**: Phase 5 is being skipped to accelerate development of the Gold analytics layer. This phase was identified as non-critical for core functionality.

**Rationale:**
- Phase 5 provides performance optimization views, not core data requirements
- Gold layer will create its own optimized dimensional structures
- Materialized views can be added later if performance becomes an issue
- Analytics enablement assets will be built directly in the Gold layer

**Future Consideration:**
- Return to Phase 5 implementation after Gold layer MVP completion
- Focus on performance optimization based on actual Gold layer usage patterns

## Error Handling and Monitoring Strategy

### Error Handling Patterns

#### Asset-Level Error Handling

```python
@asset(
    deps=["jobs_llm_enriched"],
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    description="Skills extraction with comprehensive error handling"
)
def stage_llm_skills_raw_extraction(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """Skills extraction with robust error handling"""

    try:
        # Main processing logic
        result = process_skills_extraction(snowflake)

        # Validate results
        if not validate_extraction_results(result):
            context.log.warning("Extraction validation failed - flagging for review")
            result['requires_manual_review'] = True

        # Log success metrics
        context.log.info(f"Successfully extracted {result['skills_count']} skills")

        return result

    except SnowflakeConnectionError as e:
        context.log.error(f"Snowflake connection failed: {e}")
        # Trigger alert but allow retry
        raise RetryRequested(delay=120)

    except DataQualityError as e:
        context.log.error(f"Data quality issue detected: {e}")
        # Log issue but don't fail the pipeline
        return {"status": "partial_success", "issues": [str(e)]}

    except Exception as e:
        context.log.error(f"Unexpected error in skills extraction: {e}")
        # Send alert and fail
        send_alert(f"Skills extraction failed: {e}")
        raise
```

#### Data Quality Monitoring

```python
class DataQualityMonitor:
    """Monitor data quality across all normalization stages"""

    def __init__(self, snowflake: SnowflakeResource):
        self.snowflake = snowflake
        self.thresholds = LLMStandardizationConfig()

    def validate_skills_normalization(self) -> Dict[str, Any]:
        """Validate skills normalization quality"""

        # Check coverage
        coverage = self.calculate_skills_coverage()
        if coverage < self.thresholds.min_coverage_percentage:
            raise DataQualityError(f"Skills coverage {coverage}% below threshold {self.thresholds.min_coverage_percentage}%")

        # Check confidence distribution
        low_confidence_pct = self.calculate_low_confidence_percentage()
        if low_confidence_pct > self.thresholds.max_low_confidence_percentage:
            raise DataQualityWarning(f"High percentage of low confidence skills: {low_confidence_pct}%")

        # Check for anomalies
        anomalies = self.detect_skill_anomalies()

        return {
            "coverage_percentage": coverage,
            "low_confidence_percentage": low_confidence_pct,
            "anomalies_detected": len(anomalies),
            "status": "passed" if len(anomalies) == 0 else "warning"
        }

    def detect_skill_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalies in skill data"""
        # Implementation for detecting unusual patterns, spikes, or drops
        pass
```

### Monitoring Assets and Functions

#### Key Monitoring Assets

```python
@asset(
    description="Generate comprehensive monitoring metrics for LLM standardization",
    group_name="llm_monitoring",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=30)
)
def stage_llm_monitoring_metrics(context, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Generate comprehensive monitoring metrics for dashboards and alerting.

    Metrics Generated:
    - Data quality scores by category
    - Coverage percentages
    - Confidence score distributions
    - Processing performance metrics
    - Trend analysis and anomaly detection
    """

    monitor = DataQualityMonitor(snowflake)

    # Collect all metrics
    metrics = {
        "skills_quality": monitor.validate_skills_normalization(),
        "keywords_quality": monitor.validate_keywords_normalization(),
        "locations_quality": monitor.validate_locations_normalization(),
        "overall_coverage": monitor.calculate_overall_coverage(),
        "performance_metrics": monitor.get_performance_metrics(),
        "trend_analysis": monitor.analyze_trends(),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Check for alerts
    alert_handler = LLMStandardizationAlerts()
    for category, quality_data in metrics.items():
        if quality_data.get('status') == 'warning':
            alert_handler.send_data_quality_alert(category, quality_data)

    return metrics
```

### File Structure and Organization

```
pipeline/
├── dagster_betterjobs/dagster_betterjobs/
│   ├── assets/
│   │   ├── llm_standardization/
│   │   │   ├── __init__.py
│   │   │   ├── skills_normalization.py      # Skills normalization assets (✅ Phase 1)
│   │   │   ├── keywords_normalization.py    # Keywords normalization assets (✅ Phase 2)
│   │   │   ├── locations_normalization.py   # Locations normalization assets (✅ Phase 3)
│   │   │   └── data_quality.py             # Quality validation assets (✅ Phase 4)
│   │   └── monitoring/
│   │       └── llm_monitoring.py           # Monitoring and metrics assets
│   ├── transformations/
│   │   ├── llm_standardization.py          # Core processing classes
│   │   └── data_quality.py                 # Quality validation functions
│   └── config/
│       └── llm_standardization_config.py   # Configuration management
├── sql/
│   ├── objects/                             # Database objects (views, tables, infrastructure)
│   │   ├── views/                          # Database views
│   │   ├── tables/                         # Table definitions
│   │   └── infrastructure/                 # Infrastructure SQL
│   ├── schema_setup/                        # Schema and table definitions
│   │   ├── README.md                       # Setup documentation
│   │   ├── 00_database_and_schema_setup.sql # Database and schema creation
│   │   ├── stage_definitions.sql           # STAGE schema table definitions (✅ Complete)
│   │   ├── raw_definitions.sql             # RAW schema table definitions
│   │   └── analytics_definitions.sql       # ANALYTICS schema table definitions (⏸️ Planned)
│   └── data_population/                     # Reference data and lookup tables
│       ├── README.md                       # Data population documentation
│       ├── insert_skill_category_patterns.sql      # Skill category detection patterns (✅)
│       ├── insert_skill_standardization_rules.sql  # Skill name standardization rules (✅)
│       ├── insert_skill_family_mappings.sql        # Skill family classification mappings (✅)
│       ├── insert_keyword_standardization_rules.sql # Keyword standardization rules (✅)
│       ├── insert_keyword_type_mappings.sql         # Keyword type mappings (✅)
│       ├── insert_location_standardization_rules.sql # Location standardization rules (✅)
│       ├── insert_location_classification_mappings.sql # Location classification mappings (✅)
│       ├── insert_us_states_mapping.sql            # US states lookup data (✅)
│       └── insert_countries_mapping.sql            # Countries lookup data (✅)
└── tests/
    ├── test_skills_standardization.py      # Skills normalization tests
    ├── test_locations_standardization.py   # Locations normalization tests
    └── test_data_quality.py               # Data quality tests
```

## Success Criteria and KPIs

### Data Quality KPIs

1. **Coverage Metrics**:
   - ≥85% of English jobs have normalized skills data
   - ≥75% of jobs have normalized location data
   - ≥80% of jobs have normalized keyword data

2. **Confidence Metrics**:
   - ≥70% of normalized skills have confidence score ≥0.8
   - <15% of records require manual review
   - ≥90% automatic standardization success rate

3. **Performance Metrics**:
   - Normalization pipeline completes within 30 minutes
   - Query performance improvement of ≥5x for analytics queries
   - <1% failed normalization attempts

### Business Impact KPIs

1. **Analytics Enablement**:
   - Support for all planned Gold layer analytics use cases
   - ≥10x reduction in query complexity for skill analysis
   - Enable real-time trending skills analysis

2. **Data Consistency**:
   - ≥95% reduction in skill name variations
   - Consistent location hierarchies across all data
   - Standardized keyword taxonomies

3. **Operational Efficiency**:
   - Automated quality monitoring and alerting
   - Self-healing standardization rules
   - Minimal manual intervention required

## Next Steps After Implementation

### Phase 6: Advanced Analytics Integration
- Integration with Gold layer dimensional modeling
- Advanced trend analysis and forecasting
- Machine learning model feature engineering

### Phase 7: Real-time Processing
- Stream processing for real-time normalization
- Incremental updates to normalized tables
- Real-time quality monitoring

### Phase 8: External Data Integration
- Integration with external skills taxonomies (O*NET, LinkedIn Skills)
- Location enrichment with economic and demographic data
- Industry classification enhancement

This comprehensive plan provides the foundation for transforming LLM-extracted VARIANT data into a robust, analytics-ready relational structure that enables advanced job market intelligence and insights.