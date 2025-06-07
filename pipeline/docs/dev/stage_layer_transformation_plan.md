# STAGE (Silver) Layer Transformation Plan

This document outlines the comprehensive scope and requirements for implementing the STAGE layer transformations in the BetterJobs-Snow pipeline. The STAGE layer will clean, standardize, and enrich the raw Bronze layer data using both traditional data processing techniques and AI-powered extraction.

## Overview

The STAGE layer serves as the cleaned and enriched version of our RAW(Bronze) layer job data. It will transform raw job postings into structured, standardized records ready for Gold layer analytics. The transformation pipeline will leverage Gemini LLM for intelligent data extraction while maintaining data quality and consistency.

## Architecture Overview

```
RAW(Bronze) Layer (Raw Data)
         ↓
   Data Cleaning & Standardization
         ↓
   AI-Powered Information Extraction (Gemini LLM)
         ↓
   Data Validation & Quality Checks
         ↓
   STAGE Layer (Enriched Data)
```

## Transformation Scope

### 1. Data Cleaning and Standardization

#### 1.1 Text Cleaning
- **Job Descriptions**: Remove HTML tags, normalize whitespace, fix encoding issues
- **Job Titles**: Standardize capitalization, remove special characters, normalize variations
- **Company Names**: Standardize formats, remove legal suffixes inconsistencies
- **Locations**: Parse and standardize location formats (City, State, Country)
- **URLs**: Validate and normalize job application URLs
- **Language Detection**: Identify and filter non-English job postings for U.S. market focus

##### 1.1.1 Language Detection and Filtering
**Objective**: Filter out non-English job postings to maintain U.S. market focus

**Multi-Method Detection Approach**:

**Method 1: Python Language Detection (Primary)**
```python
# Using langdetect library for high accuracy
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import pandas as pd

# Set seed for consistent results
DetectorFactory.seed = 0

def detect_language(text):
    """Detect language of job description text"""
    try:
        # Clean text for better detection
        clean_text = re.sub(r'[^\w\s]', ' ', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()

        # Need minimum 50 characters for reliable detection
        if len(clean_text) < 50:
            return 'unknown'

        detected_lang = detect(clean_text)
        return detected_lang
    except LangDetectException:
        return 'unknown'

# Apply to dataframe
jobs_df['detected_language'] = jobs_df['job_description_clean'].apply(detect_language)
jobs_df['is_english'] = jobs_df['detected_language'] == 'en'
```

**Method 2: SQL-based Pattern Detection (Secondary)**
```sql
-- Identify common non-English patterns in Snowflake
WITH language_indicators AS (
    SELECT
        job_id,
        job_description_clean,
        job_title_clean,
        -- Spanish indicators
        (CONTAINS(LOWER(job_description_clean), 'español') OR
         CONTAINS(LOWER(job_description_clean), 'requisitos') OR
         CONTAINS(LOWER(job_description_clean), 'experiencia') OR
         CONTAINS(LOWER(job_description_clean), 'conocimientos')) as has_spanish_indicators,

        -- French indicators
        (CONTAINS(LOWER(job_description_clean), 'français') OR
         CONTAINS(LOWER(job_description_clean), 'expérience') OR
         CONTAINS(LOWER(job_description_clean), 'compétences') OR
         CONTAINS(LOWER(job_description_clean), 'exigences')) as has_french_indicators,

        -- German indicators
        (CONTAINS(LOWER(job_description_clean), 'deutsch') OR
         CONTAINS(LOWER(job_description_clean), 'erfahrung') OR
         CONTAINS(LOWER(job_description_clean), 'kenntnisse') OR
         CONTAINS(LOWER(job_description_clean), 'anforderungen')) as has_german_indicators,

        -- Check for English indicators
        (CONTAINS(LOWER(job_description_clean), 'experience') OR
         CONTAINS(LOWER(job_description_clean), 'requirements') OR
         CONTAINS(LOWER(job_description_clean), 'responsibilities') OR
         CONTAINS(LOWER(job_description_clean), 'qualifications')) as has_english_indicators,

        -- Count non-English characters (accented characters, etc.)
        (LENGTH(job_description_clean) - LENGTH(REGEXP_REPLACE(job_description_clean, '[áéíóúñüç àèìòùäëïöü]', '', 'g'))) as accented_char_count,
        LENGTH(job_description_clean) as total_char_count

    FROM stage_jobs_staging
),
language_scoring AS (
    SELECT
        *,
        -- Calculate likelihood scores
        CASE
            WHEN has_spanish_indicators OR has_french_indicators OR has_german_indicators THEN 'likely_non_english'
            WHEN has_english_indicators AND (accented_char_count::FLOAT / total_char_count) < 0.02 THEN 'likely_english'
            WHEN (accented_char_count::FLOAT / total_char_count) > 0.05 THEN 'likely_non_english'
            ELSE 'uncertain'
        END as sql_language_prediction
    FROM language_indicators
)
SELECT * FROM language_scoring;
```

**Method 3: LLM-based Detection (Tertiary/Validation)**
```python
# Gemini prompt for language detection (for uncertain cases)
LANGUAGE_DETECTION_PROMPT = """
Analyze the following job posting text and determine the primary language.
Return a JSON object with this structure:
{
  "primary_language": "english|spanish|french|german|other",
  "confidence": 0.0-1.0,
  "mixed_language": true/false,
  "english_percentage": 0-100
}

Job posting text:
{job_description}
"""
```

**Implementation Strategy**:

**Phase 1: Python-based Detection**
```python
# In Dagster asset for STAGE layer processing
@asset(required_resource_keys={"snowflake"})
def stage_jobs_language_filtered(context, bronze_jobs_cleaned):
    """Filter jobs by language detection"""

    # Apply language detection
    df = bronze_jobs_cleaned.copy()
    df['detected_language'] = df['job_description_clean'].apply(detect_language)
    df['is_english'] = df['detected_language'] == 'en'

    # Additional validation for uncertain cases
    uncertain_mask = df['detected_language'] == 'unknown'
    if uncertain_mask.sum() > 0:
        # Apply SQL-based detection for uncertain cases
        df.loc[uncertain_mask, 'sql_language_check'] = df.loc[uncertain_mask].apply(
            lambda row: check_english_patterns(row['job_description_clean']), axis=1
        )

    # Final English determination
    df['is_english_final'] = (
        (df['detected_language'] == 'en') |
        ((df['detected_language'] == 'unknown') & (df.get('sql_language_check', False)))
    )

    # Filter to English-only jobs
    english_jobs = df[df['is_english_final'] == True].copy()

    context.log.info(f"Language filtering: {len(df)} total jobs, {len(english_jobs)} English jobs ({len(english_jobs)/len(df)*100:.1f}%)")

    return english_jobs
```

**Quality Validation**:
```sql
-- Monitor language detection quality
SELECT
    detected_language,
    COUNT(*) as job_count,
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER() * 100, 1) as percentage,
    AVG(LENGTH(job_description_clean)) as avg_description_length
FROM stage_jobs_unified
GROUP BY detected_language
ORDER BY job_count DESC;

-- Sample non-English jobs for manual review
SELECT
    job_id,
    job_title_clean,
    LEFT(job_description_clean, 200) as description_sample,
    detected_language
FROM stage_jobs_unified
WHERE detected_language != 'en'
LIMIT 20;
```

**Edge Cases and Considerations**:

1. **Mixed Language Content**:
   - Some U.S. jobs may mention "Spanish preferred" or include brief Spanish phrases
   - Use English percentage threshold (e.g., >80% English content)

2. **Short Descriptions**:
   - Language detection less reliable for very short text
   - Set minimum character threshold (50+ characters)
   - Fall back to location-based inference

3. **Technical Terms**:
   - Programming languages and technical terms may confuse detection
   - Maintain allowlist of technical terms to ignore

4. **Location-based Validation**:
```sql
-- Cross-validate with location data
SELECT
    detected_language,
    location_standardized,
    COUNT(*) as count
FROM stage_jobs_unified
WHERE detected_language != 'en'
  AND (location_standardized LIKE '%US%'
       OR location_standardized LIKE '%United States%'
       OR location_standardized LIKE '%USA%')
GROUP BY detected_language, location_standardized;
```

**Performance Optimization**:
- **Batch Processing**: Process language detection in batches of 1000 jobs
- **Caching**: Cache language detection results to avoid reprocessing
- **Incremental**: Only detect language for new/updated job descriptions

**Monitoring and Alerting**:
- Track percentage of non-English jobs over time
- Alert if non-English percentage suddenly increases (may indicate data source issues)
- Monitor detection accuracy through manual sampling

**Schema Updates**:
```sql
-- Language fields are included in main stage_jobs_unified table
-- No separate ALTER needed as they're part of the core design
```

#### 1.2 Date Standardization
- Normalize posting dates across different ATS platforms
- Handle timezone conversions and date format variations
- Create derived fields: `days_since_posted`, `posting_week`, `posting_month`

#### 1.3 Data Type Conversions
- Ensure consistent data types across all platforms
- Handle null values and empty strings appropriately
- Validate boolean fields (`is_active`, `is_remote`)

### 2. AI-Powered Information Extraction (Gemini LLM)

#### 2.1 Salary Information Extraction
**Objective**: Extract structured salary data from job descriptions and titles

**Target Fields**:
- `salary_min`: Minimum salary amount
- `salary_max`: Maximum salary amount
- `salary_currency`: Currency (USD, EUR, etc.)
- `salary_period`: Period (hourly, annually, monthly)
- `salary_type`: Type (base, total compensation, contract rate)
- `salary_confidence`: Confidence score of extraction (0-1)

**Patterns to Extract**:
- "$80,000 - $120,000 per year"
- "$45-65/hour"
- "Competitive salary, $100K+"
- "Up to $150,000 annually"
- Stock options and bonus mentions

#### 2.2 Experience Requirements Extraction
**Objective**: Extract experience-related requirements

**Target Fields**:
- `min_years_experience`: Minimum years required
- `max_years_experience`: Maximum years mentioned
- `experience_level`: Entry/Mid/Senior/Executive
- `specific_experience`: Domain-specific experience requirements
- `education_requirements`: Degree requirements

**Patterns to Extract**:
- "3-5 years of experience"
- "Senior level position"
- "Entry-level opportunity"
- "Bachelor's degree required"
- "PhD preferred"

#### 2.3 Technical Skills and Stack Extraction
**Objective**: Extract technical requirements and tools

**Target Fields**:
- `programming_languages`: Array of languages (Python, Java, etc.)
- `databases`: Database technologies (PostgreSQL, MongoDB, etc.)
- `cloud_platforms`: Cloud services (AWS, Azure, GCP)
- `frameworks`: Frameworks and libraries
- `tools`: Development and analytics tools
- `soft_skills`: Communication, leadership, etc.

#### 2.4 Work Arrangement Extraction
**Objective**: Extract work type and location flexibility

**Target Fields**:
- `work_type`: Remote/Hybrid/On-site
- `remote_flexibility`: Fully remote/Partially remote/No remote
- `travel_requirements`: Travel percentage if mentioned
- `office_locations`: Physical office locations

#### 2.5 Keyword and Theme Extraction
**Objective**: Generate searchable keywords for trend analysis

**Target Fields**:
- `primary_keywords`: Top 10 most relevant keywords
- `industry_keywords`: Industry-specific terms
- `role_type_keywords`: Role classification keywords
- `company_stage_keywords`: Startup/Enterprise/Scale-up indicators
- `job_family`: Engineering/Data/Product/Sales/Marketing/etc.

### 3. Data Enrichment and Derived Fields

#### 3.1 Company Enrichment
- Map job postings to standardized company profiles
- Add company size, industry, and funding stage
- Calculate company job posting velocity and patterns

#### 3.2 Location Enrichment
- Standardize location names and add geocoding
- Add metro area, state, and country information
- Calculate cost of living adjustments for salary data

#### 3.3 Job Classification
- Classify jobs into standard categories (Engineering, Data, Product, etc.)
- Assign seniority levels based on multiple factors
- Create job family and sub-family classifications

### 4. LLM Integration Implementation

#### 4.1 Using Existing Gemini Resource
**Note**: Gemini is already configured in the project via `GeminiResource` in `definitions.py`:

```python
# Already configured in definitions.py
"gemini": GeminiResource(
    api_key=EnvVar("GEMINI_API_KEY"),
    generative_model_name="gemini-2.5-flash-preview-04-17",
),
```

**Using Gemini in STAGE Layer Assets**:
```python
from dagster import asset

@asset(required_resource_keys={"snowflake", "gemini"})
def stage_jobs_with_llm_extraction(context, stage_jobs_cleaned):
    """Extract structured information using Gemini LLM"""

    # Access the configured Gemini resource
    gemini = context.resources.gemini

    # Process jobs in batches
    jobs_df = stage_jobs_cleaned.copy()
    batch_size = 10

    for i in range(0, len(jobs_df), batch_size):
        batch = jobs_df.iloc[i:i+batch_size]

        for idx, job in batch.iterrows():
            try:
                # Extract salary information
                salary_response = gemini.generate_content(
                    SALARY_EXTRACTION_PROMPT.format(
                        job_description=job['job_description_clean']
                    )
                )

                # Extract skills information
                skills_response = gemini.generate_content(
                    SKILLS_EXTRACTION_PROMPT.format(
                        job_description=job['job_description_clean']
                    )
                )

                # Parse and store results
                # ... processing logic

            except Exception as e:
                context.log.error(f"LLM extraction failed for job {job['job_id']}: {e}")
                continue

    return jobs_df
```

#### 4.2 Prompt Templates
**Salary Extraction Prompt**:
```python
SALARY_EXTRACTION_PROMPT = """
Analyze the following job posting and extract salary information. Return a JSON object with the following structure:
{
  "salary_min": number or null,
  "salary_max": number or null,
  "salary_currency": "USD|EUR|GBP|etc" or null,
  "salary_period": "hourly|annually|monthly" or null,
  "salary_type": "base|total|contract" or null,
  "confidence": 0.0-1.0
}

Job posting text:
{job_description}
"""
```

**Skills Extraction Prompt**:
```python
SKILLS_EXTRACTION_PROMPT = """
Extract technical skills and requirements from this job posting. Return a JSON object:
{
  "programming_languages": ["language1", "language2"],
  "databases": ["db1", "db2"],
  "cloud_platforms": ["aws", "azure", "gcp"],
  "frameworks": ["framework1", "framework2"],
  "tools": ["tool1", "tool2"],
  "min_years_experience": number or null,
  "education_requirements": ["requirement1", "requirement2"]
}

Job posting text:
{job_description}
"""
```

**Keyword Extraction Prompt**:
```python
KEYWORD_EXTRACTION_PROMPT = """
Extract the 10 most important keywords from this job posting that would be useful for job market trend analysis. Focus on:
- Technical skills and tools
- Industry-specific terms
- Role types and seniority levels
- Company stage indicators

Return a JSON object:
{
  "primary_keywords": ["keyword1", "keyword2", ...],
  "job_family": "Engineering|Data|Product|Sales|Marketing|etc",
  "experience_level": "Entry|Mid|Senior|Executive",
  "work_type": "Remote|Hybrid|On-site",
  "confidence": 0.0-1.0
}

Job posting text:
{job_description}
"""
```

#### 4.3 Batch Processing Strategy
- Process jobs in batches of 10-20 to optimize API calls and stay within rate limits
- Implement retry logic with exponential backoff for failed requests
- Cache results to avoid reprocessing unchanged data
- Monitor API usage and costs through Dagster logging

**Example Batch Processing Implementation**:
```python
import time
import json
from typing import Dict, List

def process_jobs_with_gemini(context, jobs_df, gemini_resource) -> pd.DataFrame:
    """Process jobs using Gemini with proper batching and error handling"""

    batch_size = 15  # Optimal batch size for gemini-2.5-flash-preview-04-17
    delay_between_batches = 1.0  # Rate limiting

    for i in range(0, len(jobs_df), batch_size):
        batch = jobs_df.iloc[i:i+batch_size]
        context.log.info(f"Processing batch {i//batch_size + 1}/{(len(jobs_df)//batch_size) + 1}")

        for idx, job in batch.iterrows():
            if pd.isna(job['job_description_clean']) or len(job['job_description_clean']) < 100:
                continue

            try:
                # Extract salary information
                salary_data = extract_with_retry(
                    gemini_resource,
                    SALARY_EXTRACTION_PROMPT,
                    job['job_description_clean'],
                    context
                )

                # Extract skills and keywords
                skills_data = extract_with_retry(
                    gemini_resource,
                    SKILLS_EXTRACTION_PROMPT,
                    job['job_description_clean'],
                    context
                )

                # Update dataframe with extracted data
                if salary_data:
                    jobs_df.loc[idx, 'salary_min'] = salary_data.get('salary_min')
                    jobs_df.loc[idx, 'salary_max'] = salary_data.get('salary_max')
                    jobs_df.loc[idx, 'salary_confidence'] = salary_data.get('confidence', 0.0)

                if skills_data:
                    jobs_df.loc[idx, 'programming_languages'] = json.dumps(skills_data.get('programming_languages', []))
                    jobs_df.loc[idx, 'min_years_experience'] = skills_data.get('min_years_experience')

            except Exception as e:
                context.log.error(f"Failed to process job {job['job_id']}: {e}")
                continue

        # Rate limiting between batches
        time.sleep(delay_between_batches)

    return jobs_df

def extract_with_retry(gemini_resource, prompt_template, job_description, context, max_retries=3):
    """Extract data with retry logic and error handling"""

    for attempt in range(max_retries):
        try:
            prompt = prompt_template.format(job_description=job_description)
            response = gemini_resource.generate_content(prompt)

            # Parse JSON response
            result = json.loads(response.text)
            return result

        except json.JSONDecodeError as e:
            context.log.warning(f"JSON decode error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)  # Exponential backoff

        except Exception as e:
            context.log.error(f"Gemini API error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)

    return None
```

### 5. Data Quality and Validation

#### 5.1 Quality Checks
- Validate extracted salary ranges (min <= max)
- Check for unrealistic salary values (outlier detection)
- Verify experience requirements consistency
- Validate extracted skills against known technology lists
- **Keyword Validation**: SQL-based verification of extracted keywords against job description content

##### 5.1.1 SQL-Based Keyword Validation
**Objective**: Verify that AI-extracted keywords actually appear in the source job description

**Implementation Approach**:
```sql
-- Basic keyword validation query
WITH keyword_validation AS (
    SELECT
        job_id,
        primary_keywords,
        job_description_clean,
        -- Calculate how many keywords are found in the description
        ARRAY_SIZE(primary_keywords) as total_keywords,
        ARRAY_SIZE(
            FILTER(
                primary_keywords,
                x -> CONTAINS(LOWER(job_description_clean), LOWER(x))
            )
        ) as matched_keywords
    FROM stage_jobs_unified
    WHERE primary_keywords IS NOT NULL
)
SELECT
    job_id,
    total_keywords,
    matched_keywords,
    ROUND(matched_keywords::FLOAT / total_keywords, 2) as keyword_match_ratio,
    CASE
        WHEN keyword_match_ratio >= 0.8 THEN 'HIGH_QUALITY'
        WHEN keyword_match_ratio >= 0.6 THEN 'MEDIUM_QUALITY'
        ELSE 'LOW_QUALITY'
    END as quality_score
FROM keyword_validation;
```

**Enhanced Fuzzy Matching**:
```sql
-- More sophisticated validation with fuzzy matching
WITH keyword_validation_advanced AS (
    SELECT
        job_id,
        primary_keywords,
        job_description_clean,
        ARRAY_SIZE(primary_keywords) as total_keywords,
        -- Check for exact matches
        ARRAY_SIZE(
            FILTER(primary_keywords, x -> CONTAINS(LOWER(job_description_clean), LOWER(x)))
        ) as exact_matches,
        -- Check for partial/stemmed matches
        ARRAY_SIZE(
            FILTER(
                primary_keywords,
                x -> CONTAINS(LOWER(job_description_clean), LOWER(SUBSTR(x, 1, LENGTH(x)-1)))
                  OR CONTAINS(LOWER(job_description_clean), LOWER(x || 's'))
                  OR CONTAINS(LOWER(job_description_clean), LOWER(x || 'ing'))
            )
        ) as fuzzy_matches
    FROM stage_jobs_unified
    WHERE primary_keywords IS NOT NULL
)
SELECT
    job_id,
    total_keywords,
    exact_matches,
    fuzzy_matches,
    GREATEST(exact_matches, fuzzy_matches) as best_matches,
    ROUND(GREATEST(exact_matches, fuzzy_matches)::FLOAT / total_keywords, 2) as match_ratio
FROM keyword_validation_advanced;
```

**Quality Thresholds**:
- **High Quality**: ≥80% of keywords found in description
- **Medium Quality**: 60-79% match ratio
- **Low Quality**: <60% match ratio (flag for review)

**Implementation Considerations**:

1. **Case Insensitivity**: Use `LOWER()` function for both keywords and job descriptions
2. **Stemming/Pluralization**: Check for common word variations:
   - "develop" → "developer", "development", "developing"
   - "manage" → "manager", "management", "managing"
3. **Synonym Handling**: Maintain lookup tables for common synonyms:
   - "JS" ↔ "JavaScript"
   - "ML" ↔ "Machine Learning"
   - "DB" ↔ "Database"
4. **Partial Matching**: Allow substring matching for compound terms
5. **Stop Words**: Exclude common words that don't add validation value

**Automated Quality Scoring**:
```sql
-- Update jobs table with keyword quality scores
UPDATE stage_jobs_unified
SET
    keyword_quality_score = (
        SELECT ROUND(matched_keywords::FLOAT / total_keywords, 2)
        FROM (
            SELECT
                ARRAY_SIZE(primary_keywords) as total_keywords,
                ARRAY_SIZE(
                    FILTER(primary_keywords, x -> CONTAINS(LOWER(job_description_clean), LOWER(x)))
                ) as matched_keywords
            WHERE job_id = stage_jobs_unified.job_id
        )
    ),
    data_quality_score = (
        (keyword_quality_score * 0.3) +
        (salary_confidence * 0.3) +
        (extraction_confidence_avg * 0.4)
    )
WHERE primary_keywords IS NOT NULL;
```

**Monitoring and Alerting**:
- Daily quality reports showing keyword validation statistics
- Alerts when quality scores drop below thresholds
- Track quality trends over time to identify model drift

#### 5.2 Confidence Scoring
- Assign confidence scores to all AI extractions
- Flag low-confidence extractions for manual review
- Track extraction accuracy over time

#### 5.3 Error Handling
- Log all LLM API failures and retry attempts
- Gracefully handle extraction failures without breaking pipeline
- Maintain audit logs of all transformations

### 6. STAGE Layer Schema Design

#### 6.1 Unified Jobs Table (`stage_jobs_unified`)

**Design Rationale**: Move from platform-specific tables (RAW schema) to a unified table structure (STAGE schema) that standardizes job data across all ATS platforms while preserving platform-specific information.

```sql
CREATE TABLE stage_jobs_unified (
    -- Core identifiers
    job_id STRING PRIMARY KEY,
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

    -- Extracted salary info (from LLM)
    salary_min NUMBER,
    salary_max NUMBER,
    salary_currency STRING DEFAULT 'USD',
    salary_period STRING,  -- hourly, annually, monthly
    salary_confidence FLOAT,

    -- Experience and requirements (from LLM)
    min_years_experience NUMBER,
    max_years_experience NUMBER,
    experience_level STRING,  -- Entry, Mid, Senior, Executive
    education_requirements VARIANT,  -- Array of requirements

    -- Technical skills (from LLM, stored as JSON arrays)
    programming_languages VARIANT,
    databases VARIANT,
    cloud_platforms VARIANT,
    frameworks VARIANT,
    tools VARIANT,
    soft_skills VARIANT,

    -- Work arrangement (from LLM)
    work_type STRING,  -- Remote, Hybrid, On-site
    remote_flexibility STRING,
    travel_requirements STRING,
    office_locations VARIANT,

    -- Keywords and classification (from LLM)
    primary_keywords VARIANT,
    industry_keywords VARIANT,
    role_type_keywords VARIANT,
    company_stage_keywords VARIANT,
    job_family STRING,
    job_sub_family STRING,

    -- Platform-specific data (flexible VARIANT column)
    platform_specific_data VARIANT,

    -- Quality and metadata
    transformation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    data_quality_score FLOAT,
    extraction_confidence_avg FLOAT,
    keyword_quality_score FLOAT,

    -- Source tracking
    source_raw_table STRING,  -- Original table name for lineage
    raw_data VARIANT,  -- Compressed original raw data for reference

    -- Partitioning and clustering
    partition_date DATE,

    -- Indexes and constraints
    INDEX idx_platform_date (platform, date_posted),
    INDEX idx_company_active (company_id, is_active),
    INDEX idx_location_date (location_standardized, date_posted)
);
```

#### 6.2 Platform-Specific Data Handling

**VARIANT Column Strategy**: Store platform-unique fields in `platform_specific_data` JSON column:

```sql
-- Example platform-specific data structures
-- Workday specific fields
{
  "workday": {
    "job_location_type": "On-Site",
    "job_family": "Information Technology",
    "career_level": "Experienced Professional",
    "travel_percentage": "10%"
  }
}

-- Greenhouse specific fields
{
  "greenhouse": {
    "job_board_name": "Internal",
    "custom_fields": {
      "employment_type": "Full-time",
      "visa_sponsorship": "Available"
    },
    "department_id": "12345"
  }
}

-- BambooHR specific fields
{
  "bamboohr": {
    "posting_location": "Multiple Locations",
    "job_type": "Regular",
    "compensation_type": "Salary"
  }
}
```

#### 6.3 Migration Strategy from RAW to STAGE

**Unified ETL Approach**:
```sql
-- Template for unified transformation
INSERT INTO stage_jobs_unified
SELECT
    -- Standardized core fields
    job_id,
    company_id,
    'workday' as platform,

    -- Clean and standardize text fields
    TRIM(REGEXP_REPLACE(job_title, '[^\x20-\x7E]', '')) as job_title_clean,
    TRIM(REGEXP_REPLACE(job_description, '<[^>]*>', '')) as job_description_clean,
    TRIM(company_name) as company_name_clean,

    -- Standardize location
    CASE
        WHEN location = '' OR location IS NULL THEN 'Remote'
        ELSE TRIM(location)
    END as location_standardized,

    -- Date handling
    TRY_CAST(date_posted as DATE) as date_posted,
    date_retrieved,

    -- Platform-specific data as JSON
    OBJECT_CONSTRUCT(
        'workday', OBJECT_CONSTRUCT(
            'employment_status', employment_status,
            'work_type', work_type,
            'compensation', compensation
        )
    ) as platform_specific_data,

    -- Metadata
    'RAW.WORKDAY_JOBS' as source_raw_table,
    raw_data,
    DATE(date_retrieved) as partition_date

FROM RAW.WORKDAY_JOBS
WHERE is_active = TRUE;
```

#### 6.4 Benefits of Unified Design

**Analytics Advantages**:
```sql
-- Simple cross-platform analytics (no unions needed)
SELECT
    platform,
    job_family,
    COUNT(*) as job_count,
    AVG(salary_min) as avg_min_salary
FROM stage_jobs_unified
WHERE is_english = TRUE
  AND date_posted >= CURRENT_DATE - 30
GROUP BY platform, job_family;

-- Unified keyword analysis
SELECT
    keyword.value::STRING as keyword,
    COUNT(*) as frequency,
    ARRAY_AGG(DISTINCT platform) as platforms
FROM stage_jobs_unified,
LATERAL FLATTEN(input => primary_keywords) keyword
WHERE is_english = TRUE
GROUP BY keyword.value
ORDER BY frequency DESC;
```

**Data Lineage Tracking**:
```sql
-- Track data lineage back to source
SELECT
    source_raw_table,
    platform,
    COUNT(*) as processed_jobs,
    MAX(transformation_timestamp) as last_processed
FROM stage_jobs_unified
GROUP BY source_raw_table, platform;
```

#### 6.5 Alternative: Hybrid Approach (If Needed)

If platform differences are too significant, consider a hybrid approach:

```sql
-- Main unified table + platform extension tables
CREATE TABLE stage_jobs_unified (...);  -- Core standardized fields

CREATE TABLE stage_jobs_workday_ext (
    job_id STRING REFERENCES stage_jobs_unified(job_id),
    career_level STRING,
    job_family STRING,
    travel_percentage STRING
);

CREATE TABLE stage_jobs_greenhouse_ext (
    job_id STRING REFERENCES stage_jobs_unified(job_id),
    custom_fields VARIANT,
    job_board_name STRING,
    department_id STRING
);
```

#### 6.6 Performance Considerations

**Partitioning Strategy**:
```sql
-- Partition by date for time-series queries
ALTER TABLE stage_jobs_unified
ADD PARTITION BY (partition_date);

-- Cluster by platform and location for common queries
ALTER TABLE stage_jobs_unified
CLUSTER BY (platform, location_standardized);
```

**Query Optimization**:
- Use `platform` filter early in queries when analyzing specific ATS data
- Leverage VARIANT column indexing for platform-specific queries
- Consider materialized views for common cross-platform analytics

#### 6.7 Supporting Tables
- `stage_company_profiles`: Enriched company information
- `stage_location_mapping`: Standardized location data
- `stage_skills_taxonomy`: Standardized skills and technologies
- `stage_transformation_logs`: Audit trail of all transformations

### 7. Implementation Timeline

#### Phase 1: Foundation
- Set up STAGE layer Dagster assets
- Implement basic data cleaning and standardization
- Create Snowflake STAGE schema and tables

#### Phase 1 Subtask Breakdown

Based on schema analysis of RAW layer platform differences, Phase 1 will be implemented in the following subtasks:

##### **Subtask 1.1: Text Cleaning Functions** ✅ (COMPLETED)
**Objective**: Create reusable data cleaning functions for consistent text processing across all platforms

**Platform Schema Differences Identified**:
| Field | Workday | Greenhouse | BambooHR | SmartRecruiters |
|-------|---------|------------|----------|----------------|
| `job_id` | ✅ | ✅ | ✅ | ✅ |
| `company_id` | ✅ | ✅ | ✅ | ✅ |
| `job_title` | ✅ | ✅ | ✅ | ✅ |
| `job_description` | ✅ | ✅ | ✅ | ✅ |
| `job_url` | ✅ | ✅ | ✅ | ✅ |
| `location` | ✅ | ✅ | ✅ | ✅ |
| `date_posted` | `published_at` | `published_at` | `date_posted` | `published_at` |
| `employment_type` | ✅ | ❌ | `employment_status` | ❌ |
| `time_type` | ✅ | ❌ | ❌ | ❌ |
| `department` | ❌ | ✅ + `department_id` | ✅ | ✅ |
| `work_type` | ✅ | ✅ | ✅ | ❌ |
| `compensation` | ✅ | ✅ | ✅ | ❌ |
| `valid_through` | ✅ | ❌ | ❌ | ❌ |
| `updated_at` | ❌ | ✅ | ❌ | ❌ |
| `requisition_id` | ❌ | ✅ | ❌ | ✅ |

**Implementation**: ✅ **COMPLETED**
- ✅ Created `transformations/text_cleaning.py` module
- ✅ Functions: HTML removal, whitespace normalization, title standardization, location parsing, URL validation
- ✅ Tested all functions with sample data
- ✅ **Benefits**: Testable, reusable, consistent processing foundation

**Files Created**:
- `dagster_betterjobs/transformations/__init__.py` - Package initialization
- `dagster_betterjobs/transformations/text_cleaning.py` - Core cleaning functions
- `dagster_betterjobs/transformations/test_text_cleaning.py` - Test script

**Key Functions Implemented**:
- `clean_html_tags()` - Remove HTML tags and decode entities
- `normalize_whitespace()` - Standardize whitespace
- `standardize_job_title()` - Clean and normalize job titles
- `standardize_location()` - Parse locations with remote detection
- `validate_url()` - URL validation and normalization
- `clean_job_description()` - Comprehensive description cleaning
- `clean_company_name()` - Company name standardization
- `clean_text_fields()` - Batch processing utility

**Ready for**: Subtask 1.2 (Language Detection Module)

##### **Subtask 1.2: Language Detection Module** ✅ (COMPLETED)
**Objective**: Implement language detection and filtering for English-only jobs

**Enhanced Implementation**: ✅ **COMPLETED**
- ✅ Created `transformations/language_detection.py` module
- ✅ Python-based detection using `langdetect` library with multi-language support
- ✅ SQL-based pattern detection as fallback with regex patterns for Asian languages
- ✅ Comprehensive support for English, Spanish, French, German, Korean, Chinese, Japanese
- ✅ Enhanced regex patterns for accurate Asian language detection
- ✅ **Output**: `detected_language`, `language_confidence`, `is_english` fields
- ✅ **Benefits**: Robust multi-language detection with fallback mechanisms

**Files Created**:
- `dagster_betterjobs/transformations/language_detection.py` - Core language detection functions
- `dagster_betterjobs/transformations/test_language_detection.py` - Comprehensive test suite

**Key Functions Implemented**:
- `LanguageDetector` class with configurable confidence thresholds
- `detect_text_language()` - Single text language detection
- `process_job_dataframe()` - Batch DataFrame processing
- `filter_english_jobs()` - Filter for English-only jobs
- `get_sql_language_detection_query()` - SQL-based fallback detection
- Enhanced regex patterns for Korean (한글), Chinese (中文), Japanese (日本語)

**Enhanced Features**:
- **Multi-Language Support**: Detects English, Spanish, French, German, Korean, Chinese, Japanese
- **Asian Language Detection**: Advanced regex patterns for Korean, Chinese, and Japanese characters
- **Confidence Scoring**: Configurable confidence thresholds for detection accuracy
- **Fallback Mechanisms**: SQL-based detection when Python detection is uncertain
- **Batch Processing**: Efficient DataFrame processing for large datasets

**Ready for**: Subtask 1.3 (Platform Field Mapping)

##### **Subtask 1.3: Platform Field Mapping** ✅ (COMPLETED)
**Objective**: Create standardized field mapping from platform-specific schemas to unified schema

**Enhanced Implementation**: ✅ **COMPLETED**
- ✅ Created `transformations/platform_mapping.py` module
- ✅ Comprehensive `PlatformMapper` class for all 4 platforms (BambooHR, Greenhouse, SmartRecruiters, Workday)
- ✅ Date field standardization (DATE_POSTED vs PUBLISHED_AT handling)
- ✅ Employment type normalization across platforms
- ✅ Platform-specific data preservation in JSON format
- ✅ Data validation and quality scoring
- ✅ **Benefits**: Unified schema transformation with zero data loss

**Files Created**:
- `pipeline/docs/dev/RAW_SCHEMA.md` - Comprehensive RAW schema documentation
- `dagster_betterjobs/transformations/platform_mapping.py` - Core platform mapping logic
- `dagster_betterjobs/transformations/test_platform_mapping.py` - Comprehensive test suite

**Key Functions Implemented**:
- `PlatformMapper` class with configurable field mappings
- `map_platform_data()` - Transform platform-specific data to unified schema
- `map_all_platforms()` - Batch processing for multiple platforms
- `get_platform_field_differences()` - Schema analysis utility
- Platform-specific transformations and data preservation
- Employment status normalization with extensive value mapping

**Platform Coverage**:
- **BambooHR**: `DATE_POSTED` → `date_posted`, `EMPLOYMENT_STATUS` → `employment_status`
- **Greenhouse**: `PUBLISHED_AT` → `date_posted`, preserves `DEPARTMENT_ID`, `REQUISITION_ID`
- **SmartRecruiters**: `PUBLISHED_AT` → `date_posted`, minimal platform-specific data
- **Workday**: `PUBLISHED_AT` → `date_posted`, `EMPLOYMENT_TYPE` → `employment_status`, preserves `TIME_TYPE`, `VALID_THROUGH`

**Advanced Features**:
- **JSON Data Preservation**: Platform-unique fields stored in `platform_specific_data` VARIANT column
- **Employment Status Normalization**: Maps variations (full-time, fulltime, FT) to standard values
- **Date Standardization**: Handles different date field names across platforms
- **Missing Field Handling**: Graceful handling of platform differences
- **Data Quality Validation**: Built-in validation and quality scoring
- **Source Tracking**: Maintains lineage to original RAW tables

**Ready for**: Subtask 1.4 (Core Stage Jobs Unified Asset)

##### **Subtask 1.4: Core Stage Jobs Unified Asset** ✅ (COMPLETED)
**Objective**: Create the main `stage_jobs_unified` asset with basic transformations

**Enhanced Implementation**: ✅ **COMPLETED**
- ✅ Created `assets/stage_jobs_unified.py` with comprehensive unified processing
- ✅ Successfully combines data from all 4 RAW platform tables (BambooHR, Greenhouse, Workday, SmartRecruiters)
- ✅ Applies text cleaning and language detection with fallback handling
- ✅ Uses platform field mapping with data preservation
- ✅ Implements robust data quality validation and duplicate analysis
- ✅ Loads clean, standardized unified job data to Snowflake STAGE.jobs_unified table
- ✅ **Benefits**: Production-ready unified transformation pipeline with comprehensive error handling

**Files Created**:
- `dagster_betterjobs/assets/stage_jobs_unified.py` - Main unified processing asset

**Key Features Implemented**:

**1. Multi-Platform Data Integration**:
- Loads data from all RAW platform tables with JOIN to master_company_urls
- Handles platform-specific schema differences gracefully
- Normalizes column names for consistent processing
- Preserves platform-specific data in JSON format

**2. Robust Text Processing Pipeline**:
- Applies text cleaning functions with fallback logic
- Handles missing job_description_clean columns automatically
- Creates fallback descriptions from raw job_description field
- Comprehensive logging for debugging processing issues

**3. Language Detection & Filtering**:
- Configurable language detection with confidence thresholds
- Filters for English-only jobs (configurable)
- Tracks language detection statistics
- Handles edge cases with graceful fallbacks

**4. Advanced Data Quality Validation**:
- Critical field validation (job_id, job_title_clean, platform, company_id)
- Comprehensive duplicate analysis with detailed logging
- Cross-platform duplicate detection and reporting
- **Smart Deduplication**: Uses `job_id + platform + company_id` to prevent over-aggressive duplicate removal
- Data quality scoring for each record

**5. Production-Grade Snowflake Integration**:
- Auto-creates STAGE schema and jobs_unified table
- Proper data type handling for Snowflake (DATE, TIMESTAMP_NTZ, BOOLEAN, VARIANT)
- Partition-based data loading with daily partitions
- Bulk loading using write_pandas for performance
- Transaction safety with proper error handling

**6. Comprehensive Monitoring & Metadata**:
- Detailed processing statistics tracking
- Platform-level performance metrics
- Dagster metadata integration for observability
- Quality score reporting per platform
- Processing time and success rate tracking

**7. Configuration & Flexibility**:
- Configurable processing options (language detection, English-only filtering)
- Platform selection configuration
- Batch size and record limit controls
- Debug mode for development and testing

**Technical Architecture**:
```python
@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "transformation"},
    required_resource_keys={"snowflake"},
    deps=[
        "bamboohr_company_jobs_discovery",
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery"
    ]
)
def stage_jobs_unified(context, config):
    # 1. Load RAW data from all platforms
    # 2. Apply text cleaning and language detection
    # 3. Map platform-specific fields to unified schema
    # 4. Validate data quality and handle duplicates
    # 5. Load to Snowflake STAGE.jobs_unified table
```

**Data Quality Achievements**:
- **Critical Fields**: Validates job_id, job_title_clean, platform, company_id presence
- **Duplicate Handling**: Prevents over-aggressive deduplication while maintaining data integrity
- **Schema Consistency**: Unified schema across all platforms with platform-specific data preservation
- **Language Filtering**: Accurate English-only job filtering for US market focus
- **Quality Scoring**: Automated data quality scores for monitoring and improvement

**Performance Optimizations**:
- **Bulk Loading**: Uses write_pandas for efficient Snowflake insertion
- **Partitioned Storage**: Daily partitioning for query performance
- **Memory Efficient**: Processes platforms sequentially to manage memory usage
- **Error Recovery**: Continues processing other platforms if one fails

**Bug Fixes Incorporated**:
- Fixed deduplication strategy (BUG-004) - now uses job_id + platform + company_id
- Handles missing job_description_clean columns after text cleaning
- Proper column normalization for Snowflake uppercase column names
- Robust error handling for individual platform failures

**Output Table Schema**:
```sql
CREATE TABLE STAGE.jobs_unified (
    -- Core identifiers
    job_id STRING PRIMARY KEY,
    company_id STRING,
    platform STRING,

    -- Standardized fields
    job_title_clean STRING,
    job_description_clean STRING,
    company_name_clean STRING,
    location_standardized STRING,

    -- Language detection
    detected_language STRING,
    language_confidence FLOAT,
    is_english BOOLEAN,

    -- Platform-specific data preservation
    platform_specific_data VARIANT,

    -- Quality and metadata
    data_quality_score FLOAT,
    partition_date DATE
);
```

**Processing Statistics Example**:
- **Total RAW Jobs**: 10,530+ jobs from all platforms
- **Jobs Processed**: ~9,500+ after cleaning and validation
- **Jobs Loaded**: ~7,800+ after deduplication
- **English Jobs**: ~95% of processed jobs
- **Average Data Quality Score**: >0.85 across all platforms

**Ready for**: Phase 2 - LLM Integration (Gemini-powered information extraction)

#### Phase 2: LLM Integration
- Implement Gemini API integration
- Develop and test prompt templates
- Create batch processing pipeline

#### Phase 3: Advanced Extraction
- Implement salary extraction
- Add technical skills extraction
- Develop keyword and theme extraction

#### Phase 4: Quality and Monitoring
- Implement data quality checks
- Add monitoring and alerting
- Performance optimization and testing

### 8. Success Metrics

#### 8.1 Data Quality Metrics
- **Extraction Accuracy**: >85% for salary data, >90% for skills
- **Data Completeness**: >80% of jobs have key fields populated
- **Processing Success Rate**: >95% of jobs successfully processed

#### 8.2 Performance Metrics
- **Processing Time**: <5 minutes per 1000 jobs
- **API Cost**: <$0.10 per job processed
- **Error Rate**: <5% of LLM API calls fail

#### 8.3 Business Value Metrics
- **Analytics Readiness**: 100% of STAGE data ready for Gold layer
- **Search Improvement**: Enhanced job search capabilities
- **Trend Analysis**: Enable salary and skills trend reporting

### 9. Cost Optimization

#### 9.1 LLM API Cost Management
- Implement intelligent caching to avoid reprocessing
- Use smaller models for simple extractions
- Batch process during off-peak hours for potential discounts
- Monitor and alert on unexpected cost spikes

#### 9.2 Compute Resource Optimization
- Process only new/changed records incrementally
- Use Snowflake's auto-scaling for variable workloads
- Optimize SQL queries for warehouse efficiency

### 10. Risk Mitigation

#### 10.1 Technical Risks
- **LLM API Rate Limits**: Implement retry logic and backoff strategies
- **Data Quality Issues**: Multiple validation layers and manual review processes
- **Performance Bottlenecks**: Scalable architecture with parallel processing

#### 10.2 Business Risks
- **Cost Overruns**: Budget monitoring and automatic throttling
- **Data Privacy**: Ensure no PII is sent to external APIs
- **Accuracy Issues**: Confidence scoring and human validation for critical fields

## Next Steps

1. **Architecture Review**: Review and approve the technical architecture
2. **Resource Planning**: Allocate development resources and timeline
3. **Environment Setup**: Configure Snowflake STAGE schema (Gemini API already configured)
4. **Prototype Development**: Build MVP version of core transformation pipeline
5. **Testing Strategy**: Develop comprehensive testing plan for data quality validation

This STAGE layer implementation will provide a robust foundation for the Gold layer analytics while maintaining high data quality and cost efficiency.