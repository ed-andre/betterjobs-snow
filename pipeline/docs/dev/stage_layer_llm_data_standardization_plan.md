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

### Phase 3: Location Normalization Infrastructure

#### 3.1 Create Locations Master Table

```sql
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
```

#### 3.2 Create Job-Locations Bridge Table

```sql
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
```

#### 3.3 Location Extraction and Standardization

```sql
-- Location extraction view
CREATE OR REPLACE VIEW STAGE_LOCATIONS_RAW_EXTRACTION AS
WITH OFFICE_LOCATIONS_EXPLODED AS (
    -- Extract office locations from VARIANT array
    SELECT
        JOB_UID,
        'office_locations' as LOCATION_SOURCE,
        TRIM(LOCATION.VALUE::STRING) as LOCATION_NAME_RAW,
        LOCATION.VALUE::STRING as LOCATION_NAME_ORIGINAL
    FROM STAGE.JOBS_LLM_ENRICHED,
    LATERAL FLATTEN(input => OFFICE_LOCATIONS) LOCATION
    WHERE OFFICE_LOCATIONS IS NOT NULL
      AND LOCATION.VALUE IS NOT NULL
      AND LENGTH(TRIM(LOCATION.VALUE::STRING)) > 0
      AND LOWER(TRIM(LOCATION.VALUE::STRING)) NOT IN ('null', 'none', 'n/a')
),

BASE_LOCATIONS AS (
    -- Include basic location from jobs_unified
    SELECT
        JOB_UID,
        'location_standardized' as LOCATION_SOURCE,
        TRIM(LOCATION_STANDARDIZED) as LOCATION_NAME_RAW,
        LOCATION_STANDARDIZED as LOCATION_NAME_ORIGINAL
    FROM STAGE.JOBS_UNIFIED
    WHERE LOCATION_STANDARDIZED IS NOT NULL
      AND LENGTH(TRIM(LOCATION_STANDARDIZED)) > 0
      AND LOWER(TRIM(LOCATION_STANDARDIZED)) NOT IN ('null', 'none', 'n/a', 'no location found')
)

SELECT * FROM OFFICE_LOCATIONS_EXPLODED
UNION ALL
SELECT * FROM BASE_LOCATIONS;

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
    RULE_TYPE STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern, fuzzy_match
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

### 🚧 Phase 2: Keywords Standardization Assets - **PLANNED**

**Next Steps:**
- `stage_keywords_normalized`: Standardize industry, role type, and business keywords
- `stage_job_keywords_bridge`: Create job-keyword relationships
- Implement keyword classification logic (industry, role_type, company_stage, technology)

### 🚧 Phase 3: Location Standardization Assets - **PLANNED**

**Next Steps:**
- `stage_locations_normalized`: Standardize location data with geographic hierarchy
- `stage_job_locations_bridge`: Create job-location relationships with work arrangement context
- Implement geographic enrichment (tech hub classification, cost of living data)

### 🚧 Phase 4: Data Quality and Validation Assets - **PLANNED**

**Next Steps:**
- `stage_llm_data_quality_validation`: Comprehensive quality monitoring
- `stage_llm_quality_metrics`: Quality metrics for dashboards
- Implement anomaly detection and automated alerting

### 🚧 Phase 5: Analytics Enablement Assets - **PLANNED**

**Next Steps:**
- `stage_llm_analytics_views`: Pre-aggregated views for analytics
- Performance optimization with materialized views
- Integration preparation for Gold layer dimensional modeling

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
pipeline/dagster_betterjobs/dagster_betterjobs/
├── assets/
│   ├── llm_standardization/
│   │   ├── __init__.py
│   │   ├── skills_normalization.py      # Skills normalization assets
│   │   ├── keywords_normalization.py    # Keywords normalization assets
│   │   ├── locations_normalization.py   # Locations normalization assets
│   │   ├── data_quality.py             # Quality validation assets
│   │   └── analytics_views.py          # Analytics enablement assets
│   └── monitoring/
│       ├── llm_monitoring.py           # Monitoring and metrics assets
│       └── alerting.py                 # Alert generation assets
├── transformations/
│   ├── llm_standardization.py          # Core processing classes
│   ├── data_quality.py                 # Quality validation functions
│   └── monitoring.py                   # Monitoring utilities
├── config/
│   ├── llm_standardization_config.py   # Configuration management
│   └── monitoring_config.py            # Monitoring configuration
├── sql/
│   ├── llm_standardization/
│   │   ├── README.md                           # Setup documentation and usage instructions
│   │   ├── insert_skill_category_patterns.sql # Skill category detection patterns
│   │   ├── insert_skill_standardization_rules.sql # Skill name standardization rules
│   │   ├── insert_skill_family_mappings.sql   # Skill family classification mappings
│   │   ├── insert_location_standardization_rules.sql # Location standardization rules
│   │   └── quality_validation.sql             # Quality validation queries (planned)
│   └── views/
│       ├── skills_analytics.sql        # Skills analysis views
│       ├── location_analytics.sql      # Location analysis views
│       └── monitoring_views.sql        # Monitoring and quality views
└── tests/
    ├── test_skills_standardization.py  # Skills normalization tests
    ├── test_locations_standardization.py # Locations normalization tests
    ├── test_data_quality.py           # Data quality tests
    └── test_monitoring.py             # Monitoring tests
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