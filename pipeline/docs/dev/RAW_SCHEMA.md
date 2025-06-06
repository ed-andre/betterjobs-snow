# RAW Schema Documentation

This document provides comprehensive documentation of all tables in the RAW schema layer of the BetterJobs-Snow pipeline.

## Overview

The RAW schema serves as the Bronze layer in our medallion architecture, storing job data directly from various ATS platforms with minimal transformation. This layer preserves the original structure and data from each platform while providing a foundation for downstream processing.

## Table Definitions

### Job Tables by Platform

#### 1. BAMBOOHR_JOBS

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.BAMBOOHR_JOBS (
    JOB_ID VARCHAR(16777216),
    COMPANY_ID VARCHAR(16777216),
    JOB_TITLE VARCHAR(16777216),
    JOB_DESCRIPTION VARCHAR(16777216),
    JOB_URL VARCHAR(16777216),
    LOCATION VARCHAR(16777216),
    DEPARTMENT VARCHAR(16777216),
    EMPLOYMENT_STATUS VARCHAR(16777216),
    DATE_POSTED DATE,
    DATE_RETRIEVED TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN,
    RAW_DATA VARCHAR(16777216),
    PARTITION_KEY VARCHAR(16777216),
    COMPENSATION VARCHAR(16777216),
    WORK_TYPE VARCHAR(16777216)
);
```

**Platform-Specific Fields:**
- `EMPLOYMENT_STATUS` - Employment type (Full-time, Part-time, etc.)
- `DATE_POSTED` - Uses DATE_POSTED instead of PUBLISHED_AT
- `COMPENSATION` - Salary/compensation information
- `WORK_TYPE` - Work arrangement (Remote, Hybrid, On-site)

#### 2. GREENHOUSE_JOBS

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.GREENHOUSE_JOBS (
    JOB_ID VARCHAR(16777216),
    COMPANY_ID VARCHAR(16777216),
    JOB_TITLE VARCHAR(16777216),
    JOB_DESCRIPTION VARCHAR(16777216),
    JOB_URL VARCHAR(16777216),
    LOCATION VARCHAR(16777216),
    DEPARTMENT VARCHAR(16777216),
    DEPARTMENT_ID VARCHAR(16777216),
    PUBLISHED_AT DATE,
    UPDATED_AT TIMESTAMP_NTZ(9),
    REQUISITION_ID VARCHAR(16777216),
    DATE_RETRIEVED TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN,
    RAW_DATA VARCHAR(16777216),
    PARTITION_KEY VARCHAR(16777216),
    WORK_TYPE VARCHAR(16777216),
    COMPENSATION VARCHAR(16777216)
);
```

**Platform-Specific Fields:**
- `DEPARTMENT_ID` - Greenhouse department identifier
- `PUBLISHED_AT` - Job posting date
- `UPDATED_AT` - Last update timestamp
- `REQUISITION_ID` - Greenhouse requisition identifier
- `WORK_TYPE` - Work arrangement
- `COMPENSATION` - Salary/compensation information

#### 3. SMARTRECRUITERS_JOBS

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.SMARTRECRUITERS_JOBS (
    JOB_ID VARCHAR(16777216),
    COMPANY_ID VARCHAR(16777216),
    JOB_TITLE VARCHAR(16777216),
    JOB_DESCRIPTION VARCHAR(16777216),
    JOB_URL VARCHAR(16777216),
    LOCATION VARCHAR(16777216),
    DEPARTMENT VARCHAR(16777216),
    PUBLISHED_AT DATE,
    REQUISITION_ID VARCHAR(16777216),
    DATE_RETRIEVED TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN,
    RAW_DATA VARCHAR(16777216),
    PARTITION_KEY VARCHAR(16777216)
);
```

**Platform-Specific Fields:**
- `PUBLISHED_AT` - Job posting date
- `REQUISITION_ID` - SmartRecruiters requisition identifier
- **Missing Fields:** COMPENSATION, WORK_TYPE, EMPLOYMENT_STATUS

#### 4. WORKDAY_JOBS

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.WORKDAY_JOBS (
    JOB_ID VARCHAR(16777216),
    COMPANY_ID VARCHAR(16777216),
    JOB_TITLE VARCHAR(16777216),
    JOB_DESCRIPTION VARCHAR(16777216),
    JOB_URL VARCHAR(16777216),
    LOCATION VARCHAR(16777216),
    TIME_TYPE VARCHAR(16777216),
    EMPLOYMENT_TYPE VARCHAR(16777216),
    PUBLISHED_AT DATE,
    VALID_THROUGH DATE,
    DATE_RETRIEVED TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN,
    RAW_DATA VARCHAR(16777216),
    PARTITION_KEY VARCHAR(16777216),
    WORK_TYPE VARCHAR(16777216),
    COMPENSATION VARCHAR(16777216)
);
```

**Platform-Specific Fields:**
- `TIME_TYPE` - Full-time/Part-time classification (Workday-specific)
- `EMPLOYMENT_TYPE` - Employment type (different from BambooHR's EMPLOYMENT_STATUS)
- `PUBLISHED_AT` - Job posting date
- `VALID_THROUGH` - Job expiration date (unique to Workday)
- `WORK_TYPE` - Work arrangement
- `COMPENSATION` - Salary/compensation information

### Master Data Tables

#### 5. MASTER_COMPANY_URLS

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.MASTER_COMPANY_URLS (
    COMPANY_ID VARCHAR(16777216),
    COMPANY_NAME VARCHAR(16777216),
    COMPANY_INDUSTRY VARCHAR(16777216),
    PLATFORM VARCHAR(16777216),
    ATS_URL VARCHAR(16777216),
    CAREER_URL VARCHAR(16777216),
    URL_VERIFIED BOOLEAN,
    DATE_ADDED TIMESTAMP_NTZ(9),
    LAST_UPDATED TIMESTAMP_NTZ(9),
    SOURCE_FILE VARCHAR(16777216),
    FILE_HASH VARCHAR(16777216),
    INGESTED_AT TIMESTAMP_NTZ(9),
    RN NUMBER(18,0)
);
```

**Purpose:** Central registry of company URLs and ATS platform information

#### 6. RAW_COMPANY_PROFILES

```sql
CREATE OR REPLACE TABLE BETTERJOBS_DB.RAW.RAW_COMPANY_PROFILES (
    PROFILE_ID VARCHAR(16777216) NOT NULL,
    COMPANY_NAME VARCHAR(16777216) NOT NULL,
    COMPANY_INDUSTRY VARCHAR(16777216),
    EMPLOYEE_COUNT_RANGE VARCHAR(16777216),
    CITY VARCHAR(16777216),
    SOURCE_FILE VARCHAR(16777216),
    FILE_HASH VARCHAR(16777216),
    INGESTED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    RAW_DATA VARCHAR(16777216),
    PRIMARY KEY (PROFILE_ID)
);
```

**Purpose:** Company profile and metadata information

## Field Mapping Analysis

### Common Fields Across All Platforms

| Field | BambooHR | Greenhouse | SmartRecruiters | Workday |
|-------|----------|------------|-----------------|---------|
| `JOB_ID` | ✅ | ✅ | ✅ | ✅ |
| `COMPANY_ID` | ✅ | ✅ | ✅ | ✅ |
| `JOB_TITLE` | ✅ | ✅ | ✅ | ✅ |
| `JOB_DESCRIPTION` | ✅ | ✅ | ✅ | ✅ |
| `JOB_URL` | ✅ | ✅ | ✅ | ✅ |
| `LOCATION` | ✅ | ✅ | ✅ | ✅ |
| `DEPARTMENT` | ✅ | ✅ | ✅ | ❌ |
| `DATE_RETRIEVED` | ✅ | ✅ | ✅ | ✅ |
| `IS_ACTIVE` | ✅ | ✅ | ✅ | ✅ |
| `RAW_DATA` | ✅ | ✅ | ✅ | ✅ |
| `PARTITION_KEY` | ✅ | ✅ | ✅ | ✅ |

### Platform-Specific Field Variations

#### Date Fields
| Platform | Date Posted Field | Additional Date Fields |
|----------|------------------|----------------------|
| BambooHR | `DATE_POSTED` | - |
| Greenhouse | `PUBLISHED_AT` | `UPDATED_AT` |
| SmartRecruiters | `PUBLISHED_AT` | - |
| Workday | `PUBLISHED_AT` | `VALID_THROUGH` |

#### Employment Classification
| Platform | Employment Field | Work Type Field | Values |
|----------|-----------------|----------------|--------|
| BambooHR | `EMPLOYMENT_STATUS` | `WORK_TYPE` | Full-time, Part-time, Contract |
| Greenhouse | ❌ | `WORK_TYPE` | Remote, Hybrid, On-site |
| SmartRecruiters | ❌ | ❌ | - |
| Workday | `EMPLOYMENT_TYPE` | `WORK_TYPE` | Full-time, Part-time, Contract |
| Workday | `TIME_TYPE` | - | Full-time, Part-time (Workday-specific) |

#### Compensation and Identifiers
| Platform | Compensation | Requisition ID | Department ID |
|----------|-------------|----------------|---------------|
| BambooHR | `COMPENSATION` | ❌ | ❌ |
| Greenhouse | `COMPENSATION` | `REQUISITION_ID` | `DEPARTMENT_ID` |
| SmartRecruiters | ❌ | `REQUISITION_ID` | ❌ |
| Workday | `COMPENSATION` | ❌ | ❌ |

## Mapping Requirements for STAGE Layer

### Required Transformations

1. **Date Standardization:**
   - Map `DATE_POSTED` (BambooHR) → `date_posted`
   - Map `PUBLISHED_AT` (Others) → `date_posted`

2. **Employment Type Standardization:**
   - Map `EMPLOYMENT_STATUS` (BambooHR) → `employment_status`
   - Map `EMPLOYMENT_TYPE` (Workday) → `employment_status`
   - Handle missing values for Greenhouse/SmartRecruiters

3. **Platform-Specific Data Preservation:**
   - Store unique fields in `platform_specific_data` VARIANT column
   - Maintain `DEPARTMENT_ID`, `REQUISITION_ID`, `TIME_TYPE`, `VALID_THROUGH`

4. **Work Type Standardization:**
   - Standardize `WORK_TYPE` values across platforms
   - Handle missing values for SmartRecruiters

5. **Compensation Handling:**
   - Preserve raw `COMPENSATION` data
   - Prepare for LLM-based salary extraction

## Data Quality Considerations

### Missing Data Patterns
- **SmartRecruiters:** No compensation or work type data
- **Workday:** No department field (but has TIME_TYPE)
- **BambooHR:** No requisition ID or department ID
- **Greenhouse:** No employment status (but has work type)

### Data Consistency Issues
- Date field naming inconsistencies
- Employment classification variations
- Compensation format differences across platforms

## Next Steps

1. **Create Platform Field Mapping Module** - Implement standardized field mapping logic
2. **Implement Unified Transformation Logic** - Create STAGE layer assets
3. **Handle Platform-Specific Data** - Preserve unique fields in VARIANT columns
4. **Data Quality Validation** - Implement checks for missing/inconsistent data