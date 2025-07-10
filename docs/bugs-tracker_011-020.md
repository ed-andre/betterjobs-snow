# Bugs Tracker 011-020

This document tracks known bugs and issues in the BetterJobs Snowflake project.

## Bug Format
- **Bug ID**: Unique identifier
- **Status**: Open/In Progress/Resolved
- **Severity**: Critical/High/Medium/Low
- **Component**: Which part of the system is affected
- **Description**: Brief description of the issue
- **Root Cause Analysis**: Analysis of why the bug occurs
- **Reproduction Steps**: How to reproduce the issue
- **Expected vs Actual**: What should happen vs what actually happens
- **Date Reported**: When the bug was first identified

---

## BUG STATUS

- **OPEN**
  - BUG-017: Analytics Fact Job Postings Duplicate Records
  - BUG-018: [ANALYTICS] Duplicate Job Posting Records in FACT_JOB_POSTINGS

- **IN PROGRESS**
  - **RESOLVED**
    - BUG-011: Greenhouse Scraper Character Encoding Corruption - Unicode Escape Processing
    - BUG-012: LLM Enrichment Maximum Recursion Depth Exceeded - JSON Parsing Overflow
    - BUG-013: Schema-as-Code Process Failing - SnowflakeConnection Object No Attribute Execute Error
    - BUG-014: Inconsistent COMPANY_ID Generation Across Pipeline Tables
    - BUG-015: Dead EXPERIENCE_LEVEL_CONTEXT Column and Missing Experience Extraction Pipeline
    - BUG-019: Table Creation Order Failures Due to Foreign Key Dependencies
    - BUG-020: All FACT_JOB_POSTINGS Records Show LOCATION_KEY as LOC_UNKNOWN
- **NO ACTION REQUIRED**

---

## BUG-011: Greenhouse Scraper Character Encoding Corruption - Unicode Escape Processing

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Greenhouse Scraper (`greenhouse_scraper.py`)
**Date Reported:** 2025-01-08
**Date Resolved:** 2025-01-08
**Related Bug:** BUG-010 (Text Cleaning Aggressive Processing)

### Description
The Greenhouse scraper was corrupting UTF-8 characters during JSON/HTML extraction, causing legitimate characters like apostrophes (') to appear as corrupted sequences (â) in the raw job data. This corruption occurred upstream in data extraction, meaning the raw `greenhouse_jobs` table contained already-corrupted data that downstream text cleaning could not fix.

### Root Cause Analysis - FINAL
The character corruption was occurring in the `_extract_json_from_response()` method in `greenhouse_scraper.py` where aggressive `unicode_escape` decoding was being applied to all text content, not just actual HTML escape sequences.

**Technical Root Cause**:
- **File**: `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/greenhouse_scraper.py`
- **Method**: `_extract_json_from_response()` (lines 275-276, 295, 750-754)
- **Issue**: `bytes(content_text, "utf-8").decode("unicode_escape")` was being applied to all extracted content

**Problem**: The `unicode_escape` decoder was misinterpreting legitimate UTF-8 characters as escape sequences, causing corruption:
- Original: `"we're"` (correct UTF-8)
- Corrupted: `"weâ"` (after incorrect unicode_escape processing)

### Evidence of Issue

**Raw Database Corruption**:
```html
<p>At Udemy, weâre on a mission to transform lives through learning.</p>
```
Should be:
```html
<p>At Udemy, we're on a mission to transform lives through learning.</p>
```

**Affected Content**:
- Job descriptions containing apostrophes, quotes, and other UTF-8 characters
- HTML content with legitimate Unicode characters
- Company names and location information with special characters

**Example Corruption Patterns**:
- `we're` → `weâre`
- `you'll` → `youâll`
- `can't` → `canât`
- `"smart"` → `âsmartâ`

### Technical Details

**Root Cause Code Locations**:
1. **Line 275-276**: `content_text = bytes(content_text, "utf-8").decode("unicode_escape")`
2. **Line 295**: `html_content = bytes(html_content, "utf-8").decode("unicode_escape")`
3. **Lines 750-754**: Similar unicode_escape handling in embedded job processing

**Processing Flow**:
1. Greenhouse API returns correct UTF-8 content: `"we're"`
2. Scraper extracts JSON/HTML content
3. **BUG**: Applies `unicode_escape` decoding unnecessarily: `"we're"` → `"weâre"`
4. Corrupted data stored in `greenhouse_jobs` table
5. Downstream processing receives already-corrupted data

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/greenhouse_scraper.py`

**Changes Made**:

**Phase 1 - JSON Content Extraction (Lines 269-290)**:
1. **Conditional Unicode Escape Processing**: Only apply `unicode_escape` when content actually contains HTML escape sequences (`\\u003c` = `<`)
2. **Added Error Handling**: Try-catch blocks around unicode decoding with fallback to raw content
3. **Preserved Original Content**: When decoding fails, keep the original UTF-8 content

**Phase 2 - HTML Content Extraction (Lines 291-315)**:
4. **Similar Conditional Processing**: Only unescape when HTML tags are actually escaped
5. **Error Handling**: Graceful fallback to raw content if unicode decoding fails

**Phase 3 - Embedded Job Processing (Lines 747-765)**:
6. **Selective HTML Entity Unescaping**: Only unescape common HTML entities (`&lt;`, `&gt;`, `&amp;`, `&quot;`)
7. **Conditional Unicode Processing**: Only apply unicode_escape for actual HTML tag escapes

**Technical Fix**:
```python
# BEFORE (problematic):
content_text = bytes(content_text, "utf-8").decode("unicode_escape")  # ❌ Always applied

# AFTER (fixed):
if "\\u003c" in content_text:  # Only if contains HTML tag escapes
    try:
        content_text = bytes(content_text, "utf-8").decode("unicode_escape")
    except Exception as e:
        self.log_message("warning", f"Failed to decode unicode escapes, using raw content: {str(e)}")
        # Keep original content if decoding fails ✅
```

**Key Insight**: The original JSON/HTML from Greenhouse already contains correct UTF-8 characters. Aggressive decoding was corrupting valid UTF-8 by treating it as escape sequences.

### Test Results - Character Integrity Preserved
✅ **Apostrophes**: `"we're"` remains `"we're"` (no corruption to `"weâre"`)
✅ **Quotes**: `"smart"` remains `"smart"` (no corruption to `"âsmartâ"`)
✅ **Contractions**: `"can't"` remains `"can't"` (no corruption to `"canât"`)
✅ **HTML Escapes**: Legitimate `\\u003c` sequences still properly decoded when needed
✅ **Error Handling**: Graceful fallback to raw content when decoding fails

### Impact
- ✅ **Raw Data Quality Restored**: `greenhouse_jobs` table now contains correct UTF-8 characters
- ✅ **Upstream Corruption Fixed**: Issue resolved at data extraction source, not just downstream cleaning
- ✅ **Character Integrity Preserved**: All UTF-8 characters (apostrophes, quotes, accents) maintain original form
- ✅ **HTML Processing Maintained**: Legitimate HTML escape sequences still properly handled
- ✅ **Downstream Benefits**: Clean raw data eliminates need for aggressive downstream character fixing
- ✅ **User Experience**: Job descriptions display with proper punctuation and readability

### Files Affected
- ✅ `dagster_betterjobs/scrapers/greenhouse_scraper.py` - Fixed unicode_escape processing logic
- ✅ Raw data quality improved for all future Greenhouse job extractions

### Relationship to BUG-010
This bug is **upstream** from BUG-010:
- **BUG-011 (This)**: Fixed character corruption at data extraction source (greenhouse_scraper.py)
- **BUG-010**: Fixed text cleaning aggressiveness in downstream processing (text_cleaning.py)

**Combined Impact**: Raw data extraction now preserves character integrity, and downstream text cleaning properly handles structure without being overly aggressive.

### Verification Steps
1. ✅ Test Greenhouse job extraction with apostrophes and quotes in content
2. ✅ Verify raw `greenhouse_jobs` table contains correct UTF-8 characters
3. ✅ Check logs for conditional unicode_escape processing (only when `\\u003c` present)
4. ✅ Confirm job descriptions display with proper punctuation throughout pipeline
5. ✅ Validate HTML escape sequences still properly handled when legitimately present

### Additional Notes
- **Upstream Fix**: Resolves character corruption at the source (data extraction)
- **Preserved Functionality**: HTML escape sequence processing still works when actually needed
- **Future-Proof**: Conditional processing handles various content formats safely
- **Data Reprocessing**: Existing Greenhouse jobs should be re-extracted to get clean UTF-8 content

---

## BUG-012: LLM Enrichment Maximum Recursion Depth Exceeded - JSON Parsing Overflow

**Status:** RESOLVED ✅
**Severity:** Critical
**Component:** LLM Processing Pipeline - BambooHR Platform (`llm_processing.py`, `llm_prompts.py`)
**Date Reported:** 2025-06-11
**Date Resolved:** 2025-06-11

### Description
The BambooHR LLM enrichment asset (`stage_jobs_llm_enriched_bamboohr`) crashed with "maximum recursion depth exceeded while calling a Python object" error during batch 64 out of 84, causing the entire LLM processing pipeline to fail. The error occurred during JSON response parsing from the Gemini API.

### Root Cause Analysis - FINAL
The recursion issue was caused by the `_extract_json_with_balanced_brackets()` function in `llm_prompts.py` attempting to parse extremely large or deeply nested JSON responses from Gemini. The BambooHR job data contained massive nested JSON structures in the RAW_DATA field (hundreds of country/state options), and when this data influenced Gemini's response generation, the resulting JSON parsing hit Python's recursion limits.

**Technical Root Cause**:
- **Primary File**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_prompts.py`
- **Method**: `_extract_json_with_balanced_brackets()` and `validate_extraction_response()`
- **Secondary File**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_processing.py`
- **Issue**: No size limits, iteration limits, or recursion depth protection during JSON parsing

**Processing Flow Where Error Occurred**:
1. BambooHR job with massive RAW_DATA (600+ country/state options) processed
2. Gemini API returns large/complex JSON response
3. `validate_extraction_response()` attempts to parse response
4. Falls back to `_extract_json_with_balanced_brackets()` for malformed JSON
5. **Recursion Error**: Function exceeds Python's recursion depth during bracket matching
6. Entire batch processing crashes, stopping LLM enrichment pipeline

### Evidence of Issue
**Error Log from Production**:
```
ERROR: [BAMBOOHR] Gemini API error for 8e677d24c6e75f9028a899a5105e4004 (attempt 1): maximum recursion depth exceeded while calling a Python object
ERROR: [BAMBOOHR] Gemini API error for 8e677d24c6e75f9028a899a5105e4004 (attempt 2): maximum recursion depth exceeded
ERROR: [BAMBOOHR] Gemini API error for 8e677d24c6e75f9028a899a5105e4004 (attempt 3): maximum recursion depth exceeded while calling a Python object
WARNING: ❌ [BAMBOOHR] Failed to extract data for job 8e677d24c6e75f9028a899a5105e4004
```

**Problematic Job Data**:
- Job UID: `8e677d24c6e75f9028a899a5105e4004`
- Platform: BambooHR
- RAW_DATA: Contains 256 countries and 51 US states (massive nested JSON structure)
- Job Description: Normal-sized sales role description (~2KB)

**Affected Batch**: Batch 64/84 processing completely failed, stopping all downstream jobs in the batch

### Technical Details
**Root Cause Components**:
1. **No Size Limits**: JSON parsing attempted on responses of unlimited size
2. **No Iteration Limits**: Bracket matching could loop infinitely on malformed data
3. **No Recursion Protection**: No depth limits for nested bracket structures
4. **No Input Validation**: Large job descriptions/prompts sent to Gemini without size checks
5. **No Response Validation**: Gemini responses not size-checked before parsing

**System Environment**:
- Python recursion limit: Default (usually 1000)
- Input data size: 100KB+ RAW_DATA JSON structure
- Gemini response size: Unknown (likely very large due to complex input)

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_prompts.py` (Primary fixes)
- `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_processing.py` (Secondary protection)

**Multi-Layered Protection Implemented**:

**Layer 1 - System-Level Protection (`llm_processing.py`)**:
```python
# Set conservative system recursion limit
DEFAULT_RECURSION_LIMIT = sys.getrecursionlimit()
SAFE_RECURSION_LIMIT = min(2000, DEFAULT_RECURSION_LIMIT)
sys.setrecursionlimit(SAFE_RECURSION_LIMIT)

# Added RecursionError handling in retry logic
except RecursionError as e:
    context.log.error(f"[{platform.upper()}] Recursion error for {job_uid} (attempt {attempt + 1}): {str(e)}")
```

**Layer 2 - Input Validation (`llm_processing.py`)**:
```python
# Job description size validation (100KB limit)
if not job_description or len(str(job_description)) > 100000:
    context.log.warning(f"❌ [{platform.upper()}] Skipping job {job['JOB_UID']}: description too large")
    continue

# Final prompt size validation (60KB limit)
if len(full_prompt) > 60000:
    context.log.warning(f"❌ [{platform.upper()}] Skipping job {job['JOB_UID']}: final prompt too large")
    continue
```

**Layer 3 - Response Size Protection (`llm_processing.py`)**:
```python
# Validate response size before parsing (100KB limit)
response_text = response.text if hasattr(response, 'text') else str(response)
MAX_RESPONSE_SIZE = 100000

if len(response_text) > MAX_RESPONSE_SIZE:
    context.log.warning(f"[{platform.upper()}] Large response ({len(response_text)} chars) truncated")
    response_text = response_text[:MAX_RESPONSE_SIZE]
```

**Layer 4 - JSON Parsing Protection (`llm_prompts.py`)**:
```python
# Response size limits in validate_extraction_response (50KB)
MAX_RESPONSE_SIZE = 50000
if len(response_text) > MAX_RESPONSE_SIZE:
    response_text = response_text[:MAX_RESPONSE_SIZE]

# Size limits for all JSON extraction methods
if len(json_content) > MAX_RESPONSE_SIZE:
    json_content = json_content[:MAX_RESPONSE_SIZE]
```

**Layer 5 - Balanced Bracket Algorithm Protection (`llm_prompts.py`)**:
```python
# Safety limits to prevent recursion and infinite loops
MAX_TEXT_SIZE = 100000    # 100KB limit
MAX_ITERATIONS = 10000    # Maximum iterations
MAX_BRACKET_DEPTH = 50    # Maximum nesting depth

# Iteration count protection
iteration_count = 0
for i, char in enumerate(text[start_idx:], start_idx):
    iteration_count += 1
    if iteration_count > MAX_ITERATIONS:
        return None

# Bracket depth protection
if bracket_count > MAX_BRACKET_DEPTH:
    return None
```

**Layer 6 - Enhanced Logging**:
```python
context.log.info(f"🛡️ [{platform.upper()}] Recursion protection enabled: max depth={SAFE_RECURSION_LIMIT}, size limits active")
```

### Test Results - All Protection Layers Working
✅ **System Recursion Limit**: Conservative 2000 limit prevents stack overflow
✅ **Input Validation**: Large job descriptions filtered out early (100KB limit)
✅ **Response Protection**: Oversized Gemini responses truncated safely (100KB limit)
✅ **JSON Parsing Limits**: Multiple size limits prevent parsing overflow (50KB limit)
✅ **Bracket Algorithm**: Iteration and depth limits prevent infinite loops
✅ **Error Handling**: RecursionError specifically caught and logged
✅ **Batch Continuation**: Failed jobs don't crash entire batch processing

### Impact
- ✅ **Pipeline Stability Restored**: LLM enrichment no longer crashes on complex job data
- ✅ **Batch Processing Resilience**: Individual job failures don't stop entire batches
- ✅ **Memory Protection**: Size limits prevent excessive memory usage
- ✅ **Graceful Degradation**: Problematic jobs skipped with proper logging
- ✅ **Resource Efficiency**: Large/problematic content filtered before expensive API calls
- ✅ **Data Quality Maintained**: Normal-sized jobs process successfully without changes

### Companies/Jobs Fixed
- **BambooHR Platform**: All jobs with large RAW_DATA structures (form fields, country lists)
- **Job UID `8e677d24c6e75f9028a899a5105e4004`**: TailWind Voice & Data sales role (now processes successfully)
- **Future Protection**: Any platform with complex nested data structures

### Files Affected
- ✅ `dagster_betterjobs/transformations/llm_processing.py` - System limits, input validation, response protection
- ✅ `dagster_betterjobs/transformations/llm_prompts.py` - JSON parsing protection, bracket algorithm limits
- ✅ All platform-specific LLM assets inherit the protection automatically

### Verification Steps
1. ✅ Test with BambooHR job UID `8e677d24c6e75f9028a899a5105e4004`
2. ✅ Verify batch processing continues after individual job failures
3. ✅ Check logs for recursion protection status messages
4. ✅ Confirm size limits properly filter oversized content
5. ✅ Validate normal-sized jobs still process without performance impact
6. ✅ Test all platform-specific LLM enrichment assets

### Prevention Measures
- **Proactive Size Filtering**: Multiple layers catch problematic content before processing
- **Conservative Resource Limits**: System-level protection prevents catastrophic failures
- **Graceful Error Handling**: Specific error types handled with appropriate recovery
- **Comprehensive Logging**: Full visibility into protection mechanisms and failures
- **Future-Proof Design**: Extensible limits that can be adjusted based on system capacity

### Additional Notes
- **Multi-Platform Protection**: Fix applies to all LLM enrichment assets (BambooHR, Greenhouse, Workday, SmartRecruiters)
- **Performance Optimized**: Early filtering reduces unnecessary API calls and processing
- **Backward Compatible**: Normal-sized job processing unchanged
- **Monitoring Ready**: Enhanced logging enables proactive issue detection

---

## BUG-013: Schema-as-Code Process Failing - SnowflakeConnection Object No Attribute Execute Error

**Status:** RESOLVED ✅
**Severity:** Critical
**Component:** Database Infrastructure Setup - Schema-as-Code (`snowflake_setup.py`, `schema_utils.py`, `analytics_dimensions.py`)
**Date Reported:** 2025-06-17
**Date Resolved:** 2025-06-17

### Description
The schema-as-code process introduced in Enhancement 20 was failing when running the `database_schema_setup` asset and all related infrastructure setup assets. All SQL queries in the schema setup scripts were throwing `'SnowflakeConnection' object has no attribute 'execute'` errors, completely blocking the database initialization process.

### Root Cause Analysis - FINAL
The issue was a fundamental misunderstanding of how the Dagster `SnowflakeResource` connection object works. The codebase was attempting to call `conn.execute()` directly on the connection object returned by `snowflake.get_connection()`, but this returns a native `snowflake.connector.Connection` object which doesn't have an `execute()` method.

**Technical Root Cause**:
- **Primary Issue**: `SnowflakeResource.get_connection()` returns a native `snowflake.connector.Connection` object
- **Required Pattern**: Snowflake connections require creating a cursor first: `cursor = conn.cursor()` then `cursor.execute()`
- **Incorrect Assumption**: Code assumed the connection had the same interface as other database connectors (like DuckDB)

**Error Pattern**:
```python
# INCORRECT (causing the error):
with snowflake.get_connection() as conn:
    conn.execute(sql_statement)  # ❌ AttributeError: no attribute 'execute'

# CORRECT (required pattern):
with snowflake.get_connection() as conn:
    cursor = conn.cursor()
    try:
        cursor.execute(sql_statement)  # ✅ Works correctly
        result = cursor.fetchone()
    finally:
        cursor.close()
```

### Evidence of Issue
**Error Log from Production**:
```
ERROR: Failed to execute statement 1: 'SnowflakeConnection' object has no attribute 'execute'
ERROR: Failed to execute statement 2: 'SnowflakeConnection' object has no attribute 'execute'
ERROR: Failed to execute statement 3: 'SnowflakeConnection' object has no attribute 'execute'
```

**Affected Operations**:
- `database_schema_setup` asset - Database and schema creation
- `infrastructure_setup` asset - Stages and integrations
- `tables_setup` asset - Table creation
- `views_setup` asset - View creation
- `static_data_population` asset - Reference data loading
- `setup_validation` asset - Setup verification
- `analytics_dim_date` asset - Date dimension creation

**Total Impact**: Complete failure of all Snowflake infrastructure setup and schema-as-code operations.

### Technical Details
**Files Affected and Root Cause**:

1. **`snowflake_setup.py`**:
   - `execute_sql_file()` function (lines 160-170)
   - `setup_validation()` asset (lines 495-499)

2. **`schema_utils.py`**:
   - `execute_sql_file()` function (lines 205)
   - `object_exists()` function (lines 105)


**Confusion Source**: Other database connectors in the codebase (DuckDB via `duckdb_resource`) do have `execute()` methods directly on the connection object, leading to incorrect assumptions about Snowflake's interface.

### Reproduction Steps
1. Run `database_schema_setup` asset in Dagster
2. Observe immediate failure with "no attribute execute" error
3. All subsequent schema setup operations fail with same error
4. Infrastructure setup pipeline completely blocked

### Expected vs Actual
**Expected**: SQL statements execute successfully using Snowflake connection
**Actual**: All SQL executions fail with AttributeError, blocking entire schema setup

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py`
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/schema_utils.py`

**Changes Made**:

**Phase 1 - `snowflake_setup.py` Fixes**:
1. **Fixed `execute_sql_file()` function**: Added cursor creation and proper error handling
2. **Fixed `setup_validation()` asset**: Added cursor pattern for validation queries

**Phase 2 - `schema_utils.py` Fixes**:
3. **Fixed `execute_sql_file()` function**: Added cursor creation with try/finally cleanup
4. **Fixed `object_exists()` function**: Added cursor pattern for existence checks

**Universal Fix Pattern Applied**:
```python
# BEFORE (problematic):
with snowflake.get_connection() as conn:
    result = conn.execute(statement)  # ❌ AttributeError

# AFTER (correct):
with snowflake.get_connection() as conn:
    cursor = conn.cursor()
    try:
        cursor.execute(statement)  # ✅ Works correctly
        result = cursor.fetchone()  # or fetchall()
    finally:
        cursor.close()
```

**Key Technical Changes**:
- Added `cursor = conn.cursor()` before all SQL executions
- Wrapped cursor operations in try/finally blocks for proper cleanup
- Updated result access patterns (`cursor.fetchone()`, `cursor.fetchall()`, `cursor.rowcount`)
- Preserved all existing error handling and logging

### Impact
- ✅ **Database Setup Restored**: All schema-as-code operations now work correctly
- ✅ **Infrastructure Pipeline Fixed**: Complete database initialization from scratch
- ✅ **Enhancement 20 Functional**: Schema-as-code system fully operational
- ✅ **All Assets Working**: Database, schema, table, view, and validation assets execute successfully
- ✅ **Proper Error Handling**: Maintained robust error handling with correct connection patterns
- ✅ **Resource Management**: Proper cursor cleanup prevents connection leaks

### Files Affected
- ✅ `dagster_betterjobs/assets/snowflake_setup.py` - Fixed execute_sql_file() and setup_validation()
- ✅ `dagster_betterjobs/utils/schema_utils.py` - Fixed execute_sql_file() and object_exists()
- ✅ `dagster_betterjobs/assets/analytics_dimensions.py` - Fixed analytics_dim_date() asset

### Verification Steps
1. ✅ Run `database_schema_setup` asset - executes all SQL statements successfully
2. ✅ Verify infrastructure_setup, tables_setup, views_setup assets work
3. ✅ Check setup_validation asset completes without errors
4. ✅ Confirm analytics_dim_date asset populates date dimension
5. ✅ Validate all schema-as-code operations end-to-end

### Prevention Measures
- **Documentation Update**: Clear examples of Snowflake connection patterns
- **Code Standards**: Establish cursor pattern as standard for all Snowflake operations
- **Testing Framework**: Add integration tests for Snowflake connection handling
- **Developer Training**: Ensure team understands Snowflake connector requirements

### Additional Notes
- **Connector Difference**: DuckDB connections have `execute()` method, Snowflake connections require cursors
- **Best Practice**: Always use try/finally pattern for cursor cleanup
- **Performance**: Minimal performance impact from proper cursor usage
- **Future Protection**: Pattern established for all new Snowflake operations

---

## BUG-014: Inconsistent COMPANY_ID Generation Across Pipeline Tables

**Status**: RESOLVED ✅
**Severity**: Critical
**Component**: Data Pipeline - Company ID Standardization
**Date Reported**: 2025-06-17
**Date Resolved**: 2025-06-17

### Description
Critical inconsistency in `COMPANY_ID` generation across pipeline tables. The `STAGE.COMPANY_PROFILES` table maps `COMPANY_ID` directly to `PROFILE_ID` from upstream `raw_company_profiles`, while other tables like `MASTER_COMPANY_URLS` use the deterministic `generate_company_id()` function. This creates mismatched company identifiers that break downstream joins and analytics.

### Root Cause Analysis
**File**: `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_company_profiles.py` (Line 224)

**Issue**: The transformation directly maps `PROFILE_ID` to `COMPANY_ID`:
```python
df['company_id'] = df['profile_id']  # Direct mapping (already standardized)
```

**Comparison with other tables**:
- `MASTER_COMPANY_URLS` uses: `generate_company_id(company_name, platform)` from `snowflake_master_company_urls.py`
- `raw_company_profiles` generates: `PROFILE_ID` using `generate_company_platform_id(company_name, "PROFILE")`
- `stage_company_profiles` incorrectly uses: `PROFILE_ID` as `COMPANY_ID`

### Expected vs Actual
**Expected**:
- `COMPANY_ID` should be generated using `generate_company_id(company_name, platform)` for consistency
- `PROFILE_ID` should be preserved as a separate field for lineage tracking

**Actual**:
- `COMPANY_ID = PROFILE_ID` (using platform="PROFILE" in the hash)
- No separate `PROFILE_ID` field in STAGE schema
- Incompatible with other tables using deterministic company ID generation

### Impact
- **Critical**: Company joining across tables fails due to ID mismatch
- **Analytics**: Downstream analytics cannot properly link company data
- **Data Integrity**: Same companies have different IDs across tables
- **Scalability**: Manual ID mapping required for cross-table queries

### Reproduction Steps
1. Query `STAGE.COMPANY_PROFILES` for a company (e.g., "Microsoft")
2. Query `MASTER_COMPANY_URLS` for the same company
3. Compare `COMPANY_ID` values - they will be completely different
4. Observe that joins on `COMPANY_ID` return no matches

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_company_profiles.py` (Lines 224, schema definition)
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/raw_company_profiles.py` (PROFILE_ID generation)
- `pipeline/sql/objects/tables/stage_company_profiles.sql` (TABLE DEFINITION)

- Schema: `STAGE.COMPANY_PROFILES` table structure

### Proposed Resolution
1. **Update STAGE.COMPANY_PROFILES schema** to include both fields:
   - `PROFILE_ID` (maps to upstream PROFILE_ID for lineage)
   - `COMPANY_ID` (generated using `generate_company_id()` for consistency)

2. **Modify stage_company_profiles.py transformation**:
   ```python
   # Preserve profile_id for lineage
   df['profile_id'] = df['profile_id']

   # Generate consistent company_id using standardized function
   df['company_id'] = df.apply(lambda row: generate_company_id(row['company_name'], 'COMPANY_PROFILES'), axis=1)
   ```

3. **Import generate_company_id function** from `snowflake_master_company_urls.py`

4. **Update all downstream queries** to use the new consistent `COMPANY_ID`

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_company_profiles.py`

**Root Cause - FINAL**: The `stage_company_profiles` asset was using `PROFILE_ID` directly as `COMPANY_ID`, while other pipeline tables used the deterministic `generate_company_id(company_name, platform)` function. This created incompatible company identifiers across tables.

**Changes Made**:

**Phase 1 - Table Schema Update**:
1. **Updated table schema** to include both `PROFILE_ID` and `COMPANY_ID` fields
2. **Preserved PROFILE_ID** for data lineage tracking
3. **Added COMPANY_ID** as primary key using standardized generation

**Phase 2 - Data Join Enhancement**:
4. **Added JOIN with MASTER_COMPANY_URLS** to retrieve platform information for each company
5. **Enhanced SQL query** to use `LEFT JOIN master_company_urls ON company_name` for platform data

**Phase 3 - Transformation Logic Fix**:
6. **Imported generate_company_id function** from `snowflake_master_company_urls.py`
7. **Updated transformation logic**:
   - Preserve original `profile_id` as `PROFILE_ID` field
   - Use platform from MASTER_COMPANY_URLS join, fallback to 'COMPANY_PROFILES' if null
   - Generate `COMPANY_ID` using `generate_company_id(company_name, platform)` with actual platform
8. **Added comprehensive logging** for company ID generation and platform distribution

**Technical Fix**:
```python
# BEFORE (problematic):
df['company_id'] = df['profile_id']  # Direct mapping causing inconsistency

# AFTER (fixed):
# Enhanced SQL with JOIN to get platform data
load_query = """
SELECT DISTINCT rcp.*, mcu.platform
FROM raw_company_profiles rcp
LEFT JOIN master_company_urls mcu
    ON TRIM(UPPER(rcp.company_name)) = TRIM(UPPER(mcu.company_name))
"""

# Use actual platform from join
df['profile_id_preserved'] = df['profile_id']  # Preserve for lineage
df['platform_for_id'] = df['platform'].fillna('COMPANY_PROFILES')  # Fallback
df['company_id'] = df.apply(lambda row: generate_company_id(row['company_name'], row['platform_for_id']), axis=1)
```

**Schema Changes**:
```sql
-- Updated table schema to include both fields:
CREATE TABLE STAGE.COMPANY_PROFILES (
    PROFILE_ID STRING,              -- Preserved for lineage
    COMPANY_ID STRING PRIMARY KEY,  -- Generated using standardized function
    COMPANY_NAME_STANDARDIZED STRING,
    -- ... other fields
);
```

### Impact
- ✅ **Company ID Consistency**: All pipeline tables now use same deterministic company ID generation
- ✅ **Cross-Table Joins**: COMPANY_ID can now be used to join across all pipeline tables
- ✅ **Data Lineage Preserved**: Original PROFILE_ID maintained for tracking
- ✅ **Analytics Enabled**: Downstream analytics can properly link company data
- ✅ **Pipeline Integrity**: Eliminates data integrity issues caused by mismatched IDs

### Files Affected
- ✅ `dagster_betterjobs/assets/stage_company_profiles.py` - Updated transformation logic and imports
- ✅ STAGE.COMPANY_PROFILES table schema - Added PROFILE_ID field, fixed COMPANY_ID generation

### Verification Steps
1. ✅ Run `stage_company_profiles` asset - generates deterministic company IDs
2. ✅ Compare COMPANY_ID values with `MASTER_COMPANY_URLS` for same companies
3. ✅ Verify cross-table joins work using COMPANY_ID
4. ✅ Confirm PROFILE_ID preserved for data lineage
5. ✅ Test downstream analytics queries linking company data

### Priority Justification
This was a **Critical** bug because:
- Broke fundamental data linking across the entire pipeline
- Prevented accurate company analytics and reporting
- Created data integrity issues that compound over time
- Required immediate resolution before further data processing

---

## BUG-015: Dead EXPERIENCE_LEVEL_CONTEXT Column and Missing Experience Extraction Pipeline

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Skills Normalization Pipeline - Job Skills Bridge
**Date Reported:** 2025-06-18
**Date Resolved:** 2025-06-18

### Description
**COMPLETED**: The dead `EXPERIENCE_LEVEL_CONTEXT` column has been REMOVED from the `BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE` table schema.

**COMPLETED**: Implemented comprehensive experience extraction assets to properly utilize rich experience data from LLM extraction (`JOBS_LLM_ENRICHED`) in structured format for job market analytics.

### Root Cause Analysis
**Primary Issue**: Dead column with no insertion logic in `stage_job_skills_bridge` asset (Removed already)
**Conceptual Issue**: Experience level belongs at the job level, not skill level

**Technical Root Cause**:
- **File**: `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py`
- **Method**: `stage_job_skills_bridge()` asset (lines 617, 700+)
- **Issue**: Column defined in schema but never populated in INSERT statement

**Data Model Problem**:
- Current approach tries to assign experience level to skill-job relationships
- Reality: Experience requirements are characteristics of job postings
- Example: "Python" skill could require 2 years experience in one job, 5 years in another

**Evidence from Code**:
```python
# Line 617: Column defined in table creation
EXPERIENCE_LEVEL_CONTEXT STRING,               -- entry, mid, senior (if mentioned)

# Lines 700+: INSERT statement never populates this field
# Column exists but is always NULL
```

### Evidence of Issue
**Database Evidence**:
```sql
-- All records have NULL experience context
SELECT EXPERIENCE_LEVEL_CONTEXT, COUNT(*)
FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
GROUP BY EXPERIENCE_LEVEL_CONTEXT;
-- Result: NULL | 50000+ (all records are NULL)
```

**Available Experience Data in JOBS_LLM_ENRICHED**:
- `MIN_YEARS_EXPERIENCE` / `MAX_YEARS_EXPERIENCE` - Specific years required
- `EXPERIENCE_LEVEL` - Entry/Mid/Senior/Executive classifications
- `SENIORITY_LEVEL` - Staff/Principal/Director/VP levels
- `SPECIFIC_TECHNOLOGIES_YEARS` - Technology-specific experience (JSON)
- `EXPERIENCE_CONFIDENCE` - Confidence in experience extraction

### Impact
- **Data Quality**: Dead column wastes storage and confuses developers
- **Missed Analytics**: Rich experience data from LLM extraction not utilized
- **Incorrect Model**: Experience attribution at wrong granularity level
- **Limited Insights**: Cannot analyze experience requirements across jobs/markets
- **Development Confusion**: Schema suggests functionality that doesn't exist

### Reproduction Steps
1. Query `JOB_SKILLS_BRIDGE` table: `SELECT DISTINCT EXPERIENCE_LEVEL_CONTEXT FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE`
2. Observe all values are NULL despite schema defining the column
3. Check `stage_job_skills_bridge` asset code - no insertion logic for this column
4. Query `JOBS_LLM_ENRICHED` - observe rich experience data available but unused

### Expected vs Actual
**Expected**: Experience-related data should be extracted from LLM data and properly modeled at job level
**Actual**:
- ✅ Dead column removed from skills bridge table
- ❌ Rich LLM experience data still unused in structured format
- ❌ No job-level experience analytics capability

### Resolution
**Fixed in**: Experience Normalization Pipeline - Complete Implementation

**Changes Made**:

**Phase 1 - Dead Column Removal (Previously Completed)**:
1. ✅ Removed `EXPERIENCE_LEVEL_CONTEXT` column from `JOB_SKILLS_BRIDGE` table schema
2. ✅ Removed column reference from `stage_job_skills_bridge` asset Python code

**Phase 2 - Experience Extraction Assets (New Implementation)**:
3. ✅ **Created SQL Table Definitions** using schema-as-code pattern:
   - `stage_experience_raw_extraction.sql` - Extraction table with source tracking
   - `stage_experience_normalized.sql` - Master experience levels with market intelligence
   - `stage_job_experience_bridge.sql` - Job-experience relationships with context

4. ✅ **Implemented Python Assets** following existing LLM standardization patterns:
   - `stage_llm_experience_raw_extraction()` - Extracts from 5 LLM data sources
   - `stage_experience_normalized()` - Creates 10+ standardized experience levels
   - `stage_job_experience_bridge()` - Maps jobs to experience requirements

**Phase 3 - Integration and Asset Dependencies**:
5. ✅ **Updated imports** in `llm_standardization/__init__.py` for asset discovery
6. ✅ **Schema-as-code compliance** using `ensure_object_exists()` pattern
7. ✅ **Proper dependency chains** between assets for reliable execution

**Technical Implementation Details**:

**Raw Extraction Processing**:
- **MIN_YEARS_EXPERIENCE/MAX_YEARS_EXPERIENCE**: Numeric year requirements
- **EXPERIENCE_LEVEL**: Entry/Mid/Senior/Executive classifications
- **SENIORITY_LEVEL**: Staff/Principal/Director/VP levels
- **SPECIFIC_TECHNOLOGIES_YEARS**: Technology-specific requirements (JSON flattening)

**Experience Normalization Logic**:
- **General Experience**: Entry (0-2), Junior (1-3), Mid (3-5), Senior (5-8), Lead (7-12)
- **Role Seniority**: Staff (8-15), Principal (10-20), Director (12-25), VP (15-30), Executive (20-40)
- **Technology-Specific**: Dynamic levels based on actual job requirements
- **Market Frequency**: Calculated based on job mention frequency

**Bridge Relationship Features**:
- **Source Tracking**: Distinguishes min_years vs max_years vs level requirements
- **Technology Context**: Maps tech-specific experience (e.g., "Python: 3 years")
- **Requirement Type**: Minimum vs preferred requirements
- **Confidence Scoring**: LLM extraction confidence preserved

### Files Affected
**Existing Files Modified (COMPLETED)**:
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py` - Dead column removed
- ✅ `pipeline/sql/objects/tables/stage_job_skills_bridge.sql` - Dead column removed from schema
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/__init__.py` - Added experience assets imports

**New Files Created (COMPLETED)**:
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/experience_normalization.py` - Complete experience pipeline
- ✅ `pipeline/sql/objects/tables/stage_experience_raw_extraction.sql` - Raw experience extraction table
- ✅ `pipeline/sql/objects/tables/stage_experience_normalized.sql` - Normalized experience levels table
- ✅ `pipeline/sql/objects/tables/stage_job_experience_bridge.sql` - Job-experience relationships table

### Impact
- ✅ **Dead Column Eliminated**: Removed confusing and unused `EXPERIENCE_LEVEL_CONTEXT` from skills bridge
- ✅ **Rich LLM Data Utilized**: All experience data from LLM extraction now properly structured
- ✅ **Job-Level Experience Analytics**: Proper data model with experience as job characteristics
- ✅ **Market Intelligence**: Experience levels include market frequency and confidence scoring
- ✅ **Technology-Specific Tracking**: Separate handling for technology-specific experience requirements
- ✅ **Analytics-Ready Structure**: Clean relational structure enables advanced analytics queries
- ✅ **Comprehensive Coverage**: Extracts from 5 different LLM data sources for complete picture
- ✅ **Pipeline Integration**: Follows existing patterns and integrates seamlessly with other LLM assets

### Proposed Resolution - Detailed Development Plan

**Phase 1: Remove Dead Column ✅ COMPLETED**
1. ✅ Remove `EXPERIENCE_LEVEL_CONTEXT` column from `JOB_SKILLS_BRIDGE` table schema
2. ✅ Remove column reference from `stage_job_skills_bridge` asset Python code

**Phase 2: Create Experience Extraction Assets (Schema-as-Code) ✅ COMPLETED**

**2.1 Create Table Definitions (Following analytics_dimensions.py pattern)**
```sql
-- File: pipeline/sql/objects/tables/stage_experience_raw_extraction.sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.EXPERIENCE_RAW_EXTRACTION (
    EXTRACTION_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,
    EXPERIENCE_SOURCE STRING NOT NULL,        -- 'min_years', 'max_years', 'experience_level', 'seniority_level', 'specific_tech'
    EXPERIENCE_TYPE STRING NOT NULL,          -- 'general', 'technology_specific', 'role_level'
    EXPERIENCE_VALUE VARIANT,                 -- Years (number) or level (string) or JSON for tech-specific
    ORIGINAL_TEXT STRING,                     -- Original extraction from LLM
    EXTRACTION_CONFIDENCE FLOAT,             -- From EXPERIENCE_CONFIDENCE field
    TECHNOLOGY_NAME STRING,                   -- For technology-specific experience (e.g., 'Python', 'AWS')
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (JOB_UID) REFERENCES BETTERJOBS_DB.STAGE.JOBS_UNIFIED(JOB_UID)
) CLUSTER BY (JOB_UID, EXPERIENCE_TYPE);

-- File: pipeline/sql/objects/tables/stage_experience_normalized.sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED (
    EXPERIENCE_ID STRING PRIMARY KEY,
    EXPERIENCE_NAME STRING NOT NULL,          -- 'Entry Level', 'Mid Level', 'Senior Level', 'Executive Level'
    EXPERIENCE_CATEGORY STRING NOT NULL,      -- 'general', 'technology_specific', 'role_seniority'
    MIN_YEARS_REQUIRED INTEGER,              -- Minimum years for this level
    MAX_YEARS_REQUIRED INTEGER,              -- Maximum years for this level
    SENIORITY_ORDER INTEGER,                 -- 1=Entry, 2=Mid, 3=Senior, 4=Staff, 5=Principal, 6=Director, 7=VP, 8=Executive
    EXPERIENCE_DESCRIPTION STRING,           -- Human-readable description
    MARKET_FREQUENCY INTEGER DEFAULT 0,      -- How often this requirement appears
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,     -- Confidence in standardization
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (EXPERIENCE_CATEGORY, SENIORITY_ORDER);

-- File: pipeline/sql/objects/tables/stage_job_experience_bridge.sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.JOB_EXPERIENCE_BRIDGE (
    BRIDGE_ID STRING PRIMARY KEY,
    JOB_UID STRING NOT NULL,
    EXPERIENCE_ID STRING NOT NULL,
    EXPERIENCE_SOURCE STRING NOT NULL,       -- 'llm_min_years', 'llm_max_years', 'llm_level', 'llm_seniority'
    EXPERIENCE_VALUE_NUMERIC INTEGER,        -- Years required (for numeric requirements)
    EXPERIENCE_VALUE_TEXT STRING,            -- Level name (for categorical requirements)
    TECHNOLOGY_CONTEXT STRING,               -- Technology name if tech-specific (e.g., 'Python: 3 years')
    IS_MINIMUM_REQUIREMENT BOOLEAN DEFAULT TRUE,  -- true=minimum, false=preferred
    EXTRACTION_CONFIDENCE FLOAT,             -- Confidence from LLM extraction
    PROCESSING_METHOD STRING DEFAULT 'llm_auto',
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (JOB_UID) REFERENCES BETTERJOBS_DB.STAGE.JOBS_UNIFIED(JOB_UID),
    FOREIGN KEY (EXPERIENCE_ID) REFERENCES BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED(EXPERIENCE_ID)
) CLUSTER BY (JOB_UID, EXPERIENCE_SOURCE);
```

**2.2 Create Experience Normalization Assets**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/experience_normalization.py

@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten experience requirements from LLM VARIANT columns",
    group_name="2b_stage_normalization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_experience_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all experience requirements from JOBS_LLM_ENRICHED and flatten into workable format.

    Processes:
    - MIN_YEARS_EXPERIENCE/MAX_YEARS_EXPERIENCE: Numeric year requirements
    - EXPERIENCE_LEVEL: Entry/Mid/Senior/Executive classifications
    - SENIORITY_LEVEL: Staff/Principal/Director/VP levels
    - SPECIFIC_TECHNOLOGIES_YEARS: Technology-specific requirements (JSON)

    Output: Raw experience requirements with source tracking and confidence scores
    """

    # Use schema-as-code pattern like analytics_dimensions.py
    table_name = ensure_object_exists("tables/stage_experience_raw_extraction.sql", snowflake, context)

    # Implementation details...

@asset(
    deps=["stage_llm_experience_raw_extraction"],
    description="Create normalized experience master table with standardized levels",
    group_name="2b_stage_normalization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_experience_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create experience master table.

    Processing:
    - Standardize experience levels (Entry: 0-2 years, Mid: 3-5 years, etc.)
    - Create seniority ordering for analytics
    - Calculate market frequency for each experience level
    - Handle technology-specific experience requirements

    Output: Clean experience master table for analytics
    """

    # Use schema-as-code pattern
    table_name = ensure_object_exists("tables/stage_experience_normalized.sql", snowflake, context)

    # Implementation details...

@asset(
    deps=["stage_experience_normalized", "stage_jobs_unified"],
    description="Create job-experience relationships with requirement context",
    group_name="2b_stage_normalization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_experience_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized experience requirements with context.

    Features:
    - Source tracking (min_years vs max_years vs level vs seniority)
    - Technology-specific experience requirements
    - Minimum vs preferred requirement classification
    - Confidence scoring for job-experience associations
    """

    # Use schema-as-code pattern
    table_name = ensure_object_exists("tables/stage_job_experience_bridge.sql", snowflake, context)

    # Implementation details...
```

**Phase 3: Integration and Testing ✅ COMPLETED**

**3.1 Update Dependencies**
- Add new assets to appropriate job definitions
- Update downstream analytics assets to use experience data
- Add foreign key constraints and validation

**3.2 Data Population Logic**
```sql
-- Raw extraction logic example:
INSERT INTO STAGE.EXPERIENCE_RAW_EXTRACTION
SELECT
    -- General years experience
    CONCAT('exp_', JOB_UID, '_min_years') as EXTRACTION_ID,
    JOB_UID,
    'min_years' as EXPERIENCE_SOURCE,
    'general' as EXPERIENCE_TYPE,
    MIN_YEARS_EXPERIENCE as EXPERIENCE_VALUE,
    CAST(MIN_YEARS_EXPERIENCE AS STRING) as ORIGINAL_TEXT,
    EXPERIENCE_CONFIDENCE as EXTRACTION_CONFIDENCE,
    NULL as TECHNOLOGY_NAME
FROM JOBS_LLM_ENRICHED
WHERE MIN_YEARS_EXPERIENCE IS NOT NULL

UNION ALL

-- Technology-specific experience (flatten JSON)
SELECT
    CONCAT('exp_', JOB_UID, '_tech_', TECH.KEY) as EXTRACTION_ID,
    JOB_UID,
    'specific_tech' as EXPERIENCE_SOURCE,
    'technology_specific' as EXPERIENCE_TYPE,
    TECH.VALUE as EXPERIENCE_VALUE,
    CONCAT(TECH.KEY, ': ', TECH.VALUE, ' years') as ORIGINAL_TEXT,
    EXPERIENCE_CONFIDENCE as EXTRACTION_CONFIDENCE,
    TECH.KEY as TECHNOLOGY_NAME
FROM JOBS_LLM_ENRICHED,
LATERAL FLATTEN(input => SPECIFIC_TECHNOLOGIES_YEARS) TECH
WHERE SPECIFIC_TECHNOLOGIES_YEARS IS NOT NULL;
```

**3.3 Analytics Integration**
- Update analytics layer to use experience dimensions
- Create views for experience-based job market analysis
- Add experience metrics to existing dashboards

### Verification Steps
1. **Raw Extraction Validation**:
   ```sql
   SELECT EXPERIENCE_SOURCE, EXPERIENCE_TYPE, COUNT(*)
   FROM STAGE.EXPERIENCE_RAW_EXTRACTION
   GROUP BY EXPERIENCE_SOURCE, EXPERIENCE_TYPE;
   ```

2. **Normalization Validation**:
   ```sql
   SELECT EXPERIENCE_CATEGORY, COUNT(*), MIN(MIN_YEARS_REQUIRED), MAX(MAX_YEARS_REQUIRED)
   FROM STAGE.EXPERIENCE_NORMALIZED
   GROUP BY EXPERIENCE_CATEGORY;
   ```

3. **Bridge Validation**:
   ```sql
   SELECT
     COUNT(*) as total_relationships,
     COUNT(DISTINCT JOB_UID) as jobs_with_experience,
     AVG(EXPERIENCE_VALUE_NUMERIC) as avg_years_required
   FROM STAGE.JOB_EXPERIENCE_BRIDGE
   WHERE EXPERIENCE_VALUE_NUMERIC IS NOT NULL;
   ```

4. **Analytics Integration Test**:
   ```sql
   -- Test job-experience analytics query
   SELECT
     en.EXPERIENCE_NAME,
     COUNT(DISTINCT jeb.JOB_UID) as job_count,
     AVG(jeb.EXPERIENCE_VALUE_NUMERIC) as avg_years
   FROM STAGE.JOB_EXPERIENCE_BRIDGE jeb
   JOIN STAGE.EXPERIENCE_NORMALIZED en ON jeb.EXPERIENCE_ID = en.EXPERIENCE_ID
   GROUP BY en.EXPERIENCE_NAME
   ORDER BY job_count DESC;
   ```

### Priority Justification
This is a **High** priority bug because:
- Dead column indicates incomplete feature implementation
- Rich LLM experience data is being wasted
- Experience analytics is crucial for job market intelligence
- Proper data modeling enables advanced analytics and insights
- Affects data warehouse completeness and user trust

### Schema-as-Code Requirements
- **Critical**: All new tables must use the `ensure_object_exists()` pattern from `analytics_dimensions.py`
- **Pattern**: Store SQL table definitions in `pipeline/sql/objects/tables/` directory
- **Asset Structure**: Use same initialization pattern as `analytics_dim_date()` and `analytics_dim_company()`
- **Dependencies**: Properly declare asset dependencies using `deps=[]` parameter
- **Error Handling**: Include try/finally cursor cleanup like existing analytics assets

### Success Criteria
- ✅ Dead column removed from skills bridge
- ✅ Three new experience assets created and functional
- ✅ All experience data from LLM extraction properly structured
- ✅ Job-experience relationships enable market analysis queries
- ✅ Schema-as-code pattern consistently applied
- ✅ Analytics layer can consume experience dimensions
- ✅ Data quality validation confirms accurate extraction and normalization


---

## BUG-016: Asset Success Masking SQL Execution Failures - Critical Data Quality Issue

**Status:** RESOLVED ✅
**Severity:** Critical
**Component:** Data Pipeline Integrity - SQL Execution Error Handling
**Date Reported:** 2025-06-18
**Date Resolved:** 2025-06-18

### Description
Dagster assets were reporting success even when critical SQL operations failed, leading to corrupted or incomplete data downstream. This occurred because the `execute_sql_file()` utility function continues execution after individual statement failures and doesn't propagate critical failures to the asset level.

### Root Cause Analysis
The `execute_sql_file()` function in `schema_utils.py` is designed to be **resilient** by continuing execution even when individual SQL statements fail. However, this creates a critical data quality issue when:

1. **Asset Reports Success**: Asset completes successfully despite SQL failures
2. **Data Corruption**: Downstream processes receive incomplete data
3. **Silent Failures**: Critical operations fail without surfacing as pipeline failures
4. **False Confidence**: Pipeline appears healthy when data quality is compromised

**Technical Root Cause**:
- **File**: `pipeline/dagster_betterjobs/dagster_betterjobs/utils/schema_utils.py`
- **Method**: `execute_sql_file()` (lines 170-250)
- **Issue**: Function returns `"success"` status even when statements fail, and assets don't validate critical operation success

**Evidence**:
```python
# Problematic pattern in assets:
if execution_result["status"] == "success":
    # Asset continues successfully
    if execution_result['statements_failed'] > 0:
        context.log.warning(f"⚠️ {statements_failed} statements failed")  # Only warning ❌
    # Asset reports success despite failures ❌
```

### Impact
- **Critical Data Quality**: Corrupted/incomplete data in downstream tables
- **Pipeline Reliability**: False confidence in data pipeline health
- **Debugging Difficulty**: Failures masked as warnings instead of errors
- **Downstream Effects**: Analytics and reporting based on incomplete data
- **Production Risk**: Silent failures in production environments

### Reproduction Steps
1. Run asset with failing SQL operations (e.g., `stage_experience_standardization_rules`)
2. Observe SQL statement failures in logs (e.g., "Statement 2 failed: SQL compilation error")
3. Note that asset still reports success in Dagster UI
4. Verify downstream data is incomplete/corrupted
5. Pipeline continues with bad data

### Expected vs Actual
**Expected**: Asset should fail when critical SQL operations fail, preventing downstream corruption
**Actual**: Asset reports success, logs warnings, and allows corrupted data downstream

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/experience_normalization.py`
- Pattern established for other critical data population assets

**Changes Made**:

**Phase 1 - Critical Failure Detection**:
1. **Added SQL Failure Validation**: Check if any SQL statements failed during critical operations
2. **Detailed Error Reporting**: Extract and log specific failure details with statement numbers
3. **Exception Propagation**: Raise exceptions when critical SQL operations fail

**Phase 2 - Data Verification**:
4. **Post-Execution Validation**: Verify expected data was actually populated after SQL execution
5. **Zero-Data Detection**: Fail if no data found after population operations
6. **Comprehensive Error Context**: Provide detailed error messages for debugging

**Technical Fix**:
```python
# BEFORE (problematic):
if execution_result["status"] == "success":
    if execution_result['statements_failed'] > 0:
        context.log.warning(f"⚠️ {statements_failed} statements failed")  # Only warning ❌
    # Asset continues successfully ❌

# AFTER (fixed):
if execution_result["status"] == "success":
    # CRITICAL: Fail asset if any SQL statements failed
    if execution_result['statements_failed'] > 0:
        failed_statements = [r for r in execution_result.get('results', []) if r.get('status') == 'error']
        error_details = []
        for failed in failed_statements[:3]:  # Show first 3 failures
            error_details.append(f"Statement {failed['statement_num']}: {failed.get('error', 'Unknown error')}")

        error_summary = "; ".join(error_details)
        if len(failed_statements) > 3:
            error_summary += f" (and {len(failed_statements) - 3} more failures)"

        context.log.error(f"❌ {execution_result['statements_failed']} SQL statements failed: {error_summary}")
        raise Exception(f"Critical SQL operation failed: {error_summary}")  # ✅ Fail asset

# CRITICAL: Verify data was actually populated
if rule_count == 0:
    raise Exception("Data population verification failed: No rules found in table after execution")  # ✅ Fail asset
```

### Impact
- ✅ **Data Quality Protected**: Assets fail when critical SQL operations fail
- ✅ **Pipeline Integrity**: False confidence eliminated, true failures surface
- ✅ **Detailed Error Reporting**: Specific failure information for rapid debugging
- ✅ **Downstream Protection**: Prevents corrupted data from flowing to dependent assets
- ✅ **Production Safety**: Critical failures in production properly surface as pipeline failures

### Files Affected
- ✅ `dagster_betterjobs/assets/llm_standardization/experience_normalization.py` - Fixed `stage_experience_standardization_rules` asset
- 🔄 **Pattern for Other Assets**: Same fix should be applied to other critical data population assets

### Verification Steps
1. ✅ Run asset with intentionally failing SQL - asset should fail (not just warn)
2. ✅ Verify detailed SQL failure information appears in error logs
3. ✅ Confirm downstream assets don't execute when critical upstream operations fail
4. ✅ Test data verification catches cases where SQL "succeeds" but no data populated

### Prevention Measures
- **Code Review Standards**: All critical SQL operations must validate success and fail fast
- **Asset Patterns**: Establish standard pattern for critical data population operations
- **Testing**: Include failure scenarios in asset tests
- **Monitoring**: Alert on any assets that fail due to SQL execution issues

### Broader Application
This pattern should be applied to **all critical data population assets**:
- `stage_skills_standardization_rules`
- `stage_keywords_standardization_rules`
- `stage_location_standardization_rules`
- `static_data_population` asset
- Any asset performing critical INSERT/UPDATE operations

### Success Criteria
- ✅ Asset fails when critical SQL operations fail
- ✅ Detailed error information available for debugging
- ✅ Data verification prevents silent failures
- ✅ Downstream data quality protected
- ✅ Pipeline reliability and confidence restored

---

## BUG-017: Analytics Fact Job Postings Duplicate Records

**Status**: OPEN 🔴
**Severity**: High
**Component**: Analytics Layer - Fact Table
**Date Reported**: 2025-06-22

### Description
The `ANALYTICS.FACT_JOB_POSTINGS` table contains duplicate records for the same job posting, with identical data except for different `JOB_FAMILY_KEY` values. This causes data quality issues and inflated metrics in analytics queries.

### Root Cause Analysis
The issue appears to be in the `analytics_dim_job_family` dimension creation and its join logic back to the fact table. The problem stems possibly from:

1. **Dimension Creation Logic**: A single job from `STAGE.JOBS_LLM_ENRICHED` can create multiple records in `DIM_JOB_FAMILY` if it has the same `JOB_FAMILY` and `SENIORITY_LEVEL` but different `JOB_SUB_FAMILY` values.

2. **Incomplete Join Logic**: The original join in `analytics_fact_job_postings` only matched on `JOB_FAMILY` and `SENIORITY_LEVEL`, but not `JOB_SUB_FAMILY`:
   ```sql
   LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_FAMILY djf
       ON jd.JOB_FAMILY = djf.JOB_FAMILY
       AND jd.SENIORITY_LEVEL = djf.SENIORITY_LEVEL
   ```

3. **Cartesian Product Effect**: When one job matches multiple dimension records, it creates multiple fact table records for the same job posting.

### Reproduction Steps
1. Run `analytics_dim_job_family` asset
2. Run `analytics_fact_job_postings` asset
3. Query for duplicate job postings:
   ```sql
   SELECT JOB_UID, COUNT(*) as duplicate_count
   FROM ANALYTICS.FACT_JOB_POSTINGS
   GROUP BY JOB_UID
   HAVING COUNT(*) > 1
   ```
4. Observe identical records with different `JOB_FAMILY_KEY` values

### Expected vs Actual
**Expected**: One record per unique job posting (`JOB_UID`)
**Actual**: Multiple records per job posting when job has multiple sub-family classifications

### Evidence
Screenshot shows same `JOB_POSTING_KEY` (JP_bc5aa293e7c13eddce2e0fef7acbaf30) appearing twice with different `JOB_FAMILY_KEY` values but otherwise identical data.

### Impact
- **Data Quality**: Inflated job counts in analytics
- **Business Metrics**: Incorrect hiring velocity and market intelligence
- **User Trust**: Inaccurate reporting undermines analytics credibility
- **Performance**: Unnecessary storage and processing overhead

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/analytics_dimensions.py` (DIM_JOB_FAMILY creation)
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/analytics_facts.py` (fact table join logic)
- `pipeline/sql/objects/tables/analytics_fact_job_postings.sql` (table definition)

### Potential Solutions
1. **Fix Join Logic**: Add `JOB_SUB_FAMILY` to the join condition (partially implemented in recent changes)
2. **Dimension Deduplication**: Modify dimension creation to avoid multiple records for same job classification
3. **Primary Key Selection**: Implement logic to select single "primary" job family per job
4. **Grain Validation**: Add constraints to prevent duplicate job postings in fact table

### Investigation Required
- [ ] Analyze how many jobs have multiple sub-family classifications
- [ ] Determine business rules for primary job family selection
- [ ] Validate if recent join logic fix resolves the issue completely
- [ ] Implement data quality constraints to prevent future duplicates

---

## BUG-018: [ANALYTICS] Duplicate Job Posting Records in FACT_JOB_POSTINGS

**Issue ID**: BUG-018
**Status**: OPEN
**Priority**: CRITICAL
**Module**: `analytics_fact_job_postings`
**Created**: 2024-06-24
**Resolved**: 2024-06-24

### Problem Description
The `ANALYTICS.FACT_JOB_POSTINGS` table contains duplicate records for the same job posting, with identical data except for different `JOB_FAMILY_KEY` values. This causes data quality issues and inflated metrics in analytics queries.

### Root Cause Analysis
The issue appears to be in the `analytics_dim_job_family` dimension creation and its join logic back to the fact table. The problem stems possibly from:

1. **Dimension Creation Logic**: A single job from `STAGE.JOBS_LLM_ENRICHED` can create multiple records in `DIM_JOB_FAMILY` if it has the same `JOB_FAMILY` and `SENIORITY_LEVEL` but different `JOB_SUB_FAMILY` values.

2. **Incomplete Join Logic**: The original join in `analytics_fact_job_postings` only matched on `JOB_FAMILY` and `SENIORITY_LEVEL`, but not `JOB_SUB_FAMILY`:
   ```sql
   LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_FAMILY djf
       ON jd.JOB_FAMILY = djf.JOB_FAMILY
       AND jd.SENIORITY_LEVEL = djf.SENIORITY_LEVEL
   ```

3. **Cartesian Product Effect**: When one job matches multiple dimension records, it creates multiple fact table records for the same job posting.

### Reproduction Steps
1. Run `analytics_dim_job_family` asset
2. Run `analytics_fact_job_postings` asset
3. Query for duplicate job postings:
   ```sql
   SELECT JOB_UID, COUNT(*) as duplicate_count
   FROM ANALYTICS.FACT_JOB_POSTINGS
   GROUP BY JOB_UID
   HAVING COUNT(*) > 1
   ```
4. Observe identical records with different `JOB_FAMILY_KEY` values

### Expected vs Actual
**Expected**: One record per unique job posting (`JOB_UID`)
**Actual**: Multiple records per job posting when job has multiple sub-family classifications

### Evidence
Screenshot shows same `JOB_POSTING_KEY` (JP_bc5aa293e7c13eddce2e0fef7acbaf30) appearing twice with different `JOB_FAMILY_KEY` values but otherwise identical data.

### Impact
- **Data Quality**: Inflated job counts in analytics
- **Business Metrics**: Incorrect hiring velocity and market intelligence
- **Dashboard Accuracy**: All downstream analytics affected

### Resolution Steps Taken
1. **Enhanced Join Logic**: Updated the job family dimension join to include all fields used in the dimension key generation:
   ```sql
   -- Job family dimension lookup (complete match to prevent duplicates)
   LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_FAMILY djf
       ON jd.JOB_FAMILY = djf.JOB_FAMILY
       AND COALESCE(jd.JOB_SUB_FAMILY, 'General') = djf.JOB_SUB_FAMILY
       AND COALESCE(jd.SENIORITY_LEVEL, 'Not Specified') = djf.SENIORITY_LEVEL
       AND COALESCE(jd.ROLE_TYPE, 'Not Specified') = djf.ROLE_TYPE
   ```

2. **Added ROLE_TYPE Field**: Ensured `ROLE_TYPE` field is included from LLM enriched data in the fact table preparation.

3. **Enhanced Validation**: Added duplicate detection logic to identify cardinality issues early:
   ```sql
   SELECT
       COUNT(*) as total_records,
       COUNT(DISTINCT job_uid) as unique_jobs,
       COUNT(*) - COUNT(DISTINCT job_uid) as duplicate_jobs
   FROM ANALYTICS.FACT_JOB_POSTINGS
   ```

4. **Updated Dependencies**: Fixed `analytics_dim_job_family` dependency to use correct source table.

### Testing Instructions
1. Run `analytics_dim_job_family` asset
2. Run `analytics_fact_job_postings` asset
3. Verify logs show "Duplicate check passed" message
4. Query to confirm 1:1 ratio:
   ```sql
   SELECT
       COUNT(*) as total_records,
       COUNT(DISTINCT job_uid) as unique_jobs
   FROM ANALYTICS.FACT_JOB_POSTINGS;
   ```
5. Should show equal counts with no duplicates

### Prevention Measures
- Enhanced logging to detect cardinality issues early
- Complete join logic that matches dimension grain exactly
- Validation queries in the asset to catch future issues

---

## BUG-019: Table Creation Order Failures Due to Foreign Key Dependencies

**Status:** RESOLVED ✅
**Severity:** Critical
**Component:** Database Infrastructure Setup - Table Creation (`tables_setup` asset)
**Date Reported:** 2025-06-26
**Date Resolved:** 2025-06-26

### Description
The `tables_setup` asset fails when creating tables due to foreign key constraint violations. Tables are currently processed in alphabetical order within each schema layer (raw, stage, analytics), but this doesn't respect foreign key dependencies. Tables with foreign key references get created before their referenced tables exist, causing creation failures.

### Root Cause Analysis
**File**: `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` (lines 320-340)
**Method**: `tables_setup()` asset

**Current Sorting Logic**:
```python
def sort_key(filename):
    if filename.startswith('raw_'):
        return (0, filename)  # RAW layer first
    elif filename.startswith('stage_'):
        return (1, filename)  # STAGE layer second
    elif filename.startswith('analytics_'):
        return (2, filename)  # ANALYTICS layer third
    else:
        return (3, filename)

table_files.sort(key=sort_key)  # Alphabetical within each layer
```

**Problem**: Within each schema layer, alphabetical ordering ignores foreign key dependencies.

### Evidence of Issue
**Specific Example**:
- `stage_job_experience_bridge.sql` (alphabetically earlier) has foreign keys to:
  - `STAGE.JOBS_UNIFIED` (created by `stage_jobs_unified.sql`)
  - `STAGE.EXPERIENCE_NORMALIZED` (created by `stage_experience_normalized.sql`)
- Alphabetical order: `stage_job_experience_bridge.sql` < `stage_jobs_unified.sql`
- **Result**: Bridge table creation fails because referenced tables don't exist yet

**Error Pattern**:
```
ERROR: Failed to create STAGE.JOB_EXPERIENCE_BRIDGE
SQL compilation error: Object 'BETTERJOBS_DB.STAGE.JOBS_UNIFIED' does not exist
```

### Impact
- **Complete Setup Failure**: Schema-as-code setup process fails completely
- **Development Blocked**: Cannot initialize database from scratch
- **CI/CD Issues**: Automated deployments fail on fresh environments
- **Migration Problems**: Cannot migrate to new Snowflake instances
- **Manual Intervention Required**: Developers must manually create tables in correct order

### Reproduction Steps
1. Run `tables_setup` asset on fresh database
2. Observe failure when creating bridge/junction tables
3. Check error logs for "Object does not exist" foreign key violations
4. Note that referenced tables haven't been created yet due to alphabetical ordering

### Expected vs Actual
**Expected**: Tables created in dependency order, all foreign keys resolve successfully
**Actual**: Tables created alphabetically, foreign key constraints fail, setup process crashes

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` (sorting logic)
- All table SQL files in `pipeline/sql/objects/tables/` (dependency relationships)

### Proposed Resolution - Table Dependency Configuration

**Approach**: Create a configuration file that explicitly defines table creation order based on dependencies.

**Implementation Plan**:

**Phase 1 - Configuration File**:
Create `pipeline/sql/objects/tables/table_creation_order.yaml`:
```yaml
# Table Creation Order Configuration
# Tables listed in dependency order - referenced tables first

raw_layer:
  - raw_master_company_urls.sql
  - raw_master_company_urls_processing_log.sql
  - raw_bamboohr_jobs.sql
  - raw_greenhouse_jobs.sql
  - raw_smartrecruiters_jobs.sql
  - raw_workday_jobs.sql

stage_layer:
  # Core tables first (no dependencies)
  - stage_company_profiles.sql
  - stage_jobs_unified.sql
  - stage_jobs_llm_enriched.sql

  # Lookup/reference tables
  - stage_countries_mapping.sql
  - stage_us_states_mapping.sql
  - stage_location_mapping.sql
  - stage_location_metro_area_mapping.sql
  - stage_location_region_mapping.sql
  - stage_location_tech_hub_mapping.sql
  - stage_location_standardization_rules.sql

  # Normalized dimension tables
  - stage_experience_normalized.sql
  - stage_skills_normalized.sql
  - stage_keywords_normalized.sql
  - stage_locations_normalized.sql
  - stage_salary_normalized.sql

  # Standardization rules (reference normalized tables)
  - stage_experience_standardization_rules.sql
  - stage_skill_standardization_rules.sql
  - stage_keyword_standardization_rules.sql

  # Bridge tables (reference multiple other tables)
  - stage_job_experience_bridge.sql
  - stage_job_skills_bridge.sql
  - stage_job_keywords_bridge.sql
  - stage_job_locations_bridge.sql
  - stage_job_salary_bridge.sql

  # Monitoring/logging tables
  - stage_transformation_logs.sql
  - stage_llm_quality_metrics.sql
  - stage_llm_confidence_monitoring.sql
  - stage_llm_coverage_analysis.sql
  - stage_llm_anomaly_detection_results.sql
  - stage_llm_quality_validation_results.sql
  - stage_llm_quality_metrics_history.sql
  - stage_llm_manual_review_queue.sql

analytics_layer:
  # Dimension tables first
  - analytics_dim_date.sql
  - analytics_dim_company.sql
  - analytics_dim_platform.sql
  - analytics_dim_location.sql
  - analytics_dim_experience.sql
  - analytics_dim_skills.sql
  - analytics_dim_keywords.sql
  - analytics_dim_job_family.sql
  - analytics_dim_salary.sql

  # Bridge tables
  - analytics_job_experience_bridge.sql

  # Fact tables (reference dimensions)
  - analytics_fact_job_postings.sql
  - analytics_fact_company_hiring_weekly.sql
  - analytics_fact_skills_demand_weekly.sql

  # Analysis tables
  - analytics_skills_trend_analysis.sql
  - analytics_market_weekly_summary.sql
```

**Phase 2 - Code Changes**:
Update `snowflake_setup.py` to use configuration:
```python
import yaml
from pathlib import Path

def load_table_creation_order(objects_dir: Path) -> List[str]:
    """Load table creation order from configuration file."""
    config_file = objects_dir / "table_creation_order.yaml"

    if not config_file.exists():
        context.log.warning("Table order config not found, falling back to alphabetical")
        return None

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # Combine all layers in order
    ordered_files = []
    for layer in ['raw_layer', 'stage_layer', 'analytics_layer']:
        if layer in config:
            ordered_files.extend(config[layer])

    return ordered_files

def tables_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    # ... existing code ...

    # Try to load dependency-ordered list
    ordered_files = load_table_creation_order(objects_dir)

    if ordered_files:
        # Use dependency order
        table_files = []
        for file_name in ordered_files:
            file_path = objects_dir / file_name
            if file_path.exists():
                table_files.append(file_name)
            else:
                context.log.warning(f"Configured table file not found: {file_name}")

        # Add any files not in config (for safety)
        all_sql_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]
        missing_files = set(all_sql_files) - set(table_files)
        if missing_files:
            context.log.warning(f"Files not in config, adding at end: {missing_files}")
            table_files.extend(sorted(missing_files))

        context.log.info(f"Using dependency-ordered table creation: {len(table_files)} files")
    else:
        # Fallback to existing logic
        table_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]
        table_files.sort(key=sort_key)
        context.log.info(f"Using alphabetical table creation: {len(table_files)} files")
```

### Advantages of Configuration Approach
✅ **Explicit Dependencies**: Clear, visible dependency relationships
✅ **Easy Maintenance**: Simple to add new tables or reorder existing ones
✅ **Documentation**: Configuration serves as dependency documentation
✅ **Flexible**: Can handle complex dependency graphs
✅ **Safe Fallback**: Falls back to alphabetical if config missing
✅ **Validation**: Can detect missing files and add them safely
✅ **Version Control**: Changes tracked in git like any other code

### Alternative Approaches Considered
1. **Automatic Dependency Parsing**: Parse SQL files for foreign key references
   - **Pro**: Fully automated
   - **Con**: Complex, fragile, hard to debug

2. **Database-Level Dependency Resolution**: Let database handle order
   - **Pro**: Technically correct
   - **Con**: Requires complex dependency graph algorithms

3. **Filename Prefixes**: Add numeric prefixes to files (001_, 002_, etc.)
   - **Pro**: Simple
   - **Con**: Requires renaming many files, less maintainable

### Priority Justification
**Critical** because:
- Completely blocks database initialization
- Prevents fresh environment setup
- Breaks CI/CD pipelines
- Affects ability to migrate to new Snowflake instances
- Manual workarounds are time-consuming and error-prone

### Resolution
**Fixed in**:
- `pipeline/sql/objects/tables/table_creation_order.yaml` (Configuration file)
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` (Implementation)

**Changes Made**:

**Phase 1 - Configuration File Creation**:
1. **Created dependency-ordered configuration**: `table_creation_order.yaml` with explicit table ordering
2. **Organized by schema layers**: Raw → Stage → Analytics with proper dependencies within each layer
3. **Bridge table ordering**: All bridge/junction tables placed after their referenced tables
4. **Comprehensive coverage**: All 49 table files included in proper dependency order

**Phase 2 - Implementation Updates**:
5. **Added YAML loading function**: `load_table_creation_order()` with error handling and logging
6. **Enhanced tables_setup logic**: Uses configuration when available, falls back to alphabetical
7. **Safety validation**: Detects missing files and adds any unconfigured files at the end
8. **Comprehensive logging**: Clear indication of ordering method used and file counts

**Technical Implementation**:
```python
# Key fix - dependency-based ordering
if ordered_files:
    # Use dependency order from YAML configuration
    for file_name in ordered_files:
        if file_path.exists():
            table_files.append(file_name)
    context.log.info(f"🔧 Using dependency-ordered table creation: {len(table_files)} files")
else:
    # Safe fallback to alphabetical by schema layer
    table_files.sort(key=sort_key)
    context.log.info(f"📋 Using alphabetical table creation (fallback): {len(table_files)} files")
```

**Key Dependency Fixes**:
- `stage_jobs_unified.sql` now created BEFORE `stage_job_experience_bridge.sql`
- `stage_experience_normalized.sql` created BEFORE bridge tables that reference it
- All dimension tables created BEFORE fact tables in analytics layer
- Bridge tables consistently placed after their referenced tables

### Impact
- ✅ **Foreign Key Violations Eliminated**: Tables created in proper dependency order
- ✅ **Fresh Database Setup**: Complete schema initialization works reliably
- ✅ **CI/CD Pipeline Fixed**: Automated deployments can provision fresh environments
- ✅ **Migration Support**: Snowflake instance migrations work without manual intervention
- ✅ **Configuration Driven**: Easy to maintain and adjust dependencies as schema evolves
- ✅ **Safety Mechanisms**: Fallback logic prevents total failures if configuration issues occur

### Files Affected
- ✅ `pipeline/sql/objects/tables/table_creation_order.yaml` - New dependency configuration
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` - Enhanced ordering logic

### Verification Steps
1. ✅ Run `tables_setup` asset on fresh database - all tables create successfully
2. ✅ Verify logs show "Using dependency-ordered table creation" message
3. ✅ Check that `stage_job_experience_bridge.sql` creates after `stage_jobs_unified.sql`
4. ✅ Confirm no foreign key constraint violation errors
5. ✅ Test fallback behavior by temporarily renaming configuration file

### Success Criteria
✅ All tables create successfully in dependency order
✅ No foreign key constraint violations during setup
✅ Configuration file properly maintained and documented
✅ Setup process works reliably on fresh databases
✅ Fallback mechanism works when configuration unavailable
✅ Comprehensive logging shows which ordering method is being used

---

## BUG-020: All FACT_JOB_POSTINGS Records Show LOCATION_KEY as LOC_UNKNOWN

**Status:** RESOLVED ✅
**Date Resolved:** 2025-06-30

### Resolution Summary
Mismatch in character case during location dimension lookup caused every join to fail and default to `LOC_UNKNOWN`. The fix was to apply case-insensitive comparison:

```sql
-- analytics_facts.py (excerpt)
LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_LOCATION dl
    ON LOWER(jd.LOCATION_STANDARDIZED) = LOWER(dl.LOCATION_NAME)
```

This change aligns the casing between source and dimension values, restoring correct `LOCATION_KEY` assignments throughout `FACT_JOB_POSTINGS`.

### Status Update
Issue verified in production; `LOC_UNKNOWN` is now only present for jobs with no location. Marking as **RESOLVED**.

---

## Template for New Bugs

**Status**: [Open/In Progress/Resolved]
**Severity**: [Critical/High/Medium/Low]
**Component**: [Which part of the system is affected]
**Date Reported**: [YYYY-MM-DD]
**Date Resolved**: [YYYY-MM-DD] (if resolved)

### Description
Brief description of the issue

### Root Cause Analysis
Analysis of why the bug occurs

### Reproduction Steps
1. Step 1
2. Step 2
3. Step 3

### Expected vs Actual
**Expected**: What should happen
**Actual**: What actually happens

### Impact
- Impact on system/users
- Data quality issues
- Performance implications

### Files Affected
- List of files that need to be modified
- Database objects affected
- Configuration changes needed

### Resolution
**Fixed in**: [File/Component]

**Changes Made**:
- Description of changes
- Code snippets if relevant

### Verification Steps
1. Steps to verify the fix works
2. Tests to run
3. Expected outcomes