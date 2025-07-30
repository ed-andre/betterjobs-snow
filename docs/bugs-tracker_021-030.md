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

- **IN PROGRESS**

  - **RESOLVED**
    - BUG-021: Partitioned Stage Assets Temporary Table Race Condition
    - BUG-022: Master Company URLs Temp Table Race Condition

- **NO ACTION REQUIRED**

---

## BUG-021: Partitioned Stage Assets Temporary Table Race Condition

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Stage Processing Pipeline - Watermark Management (`watermark_management.py`)
**Date Reported:** 2025-06-30
**Date Resolved:** 2025-06-30

### Description
The `stage_jobs_*` assets are now partitioned, creating a race condition where multiple partitions of the same platform could generate identical temporary table names and interfere with each other during the upsert process. This could lead to data corruption, table not found errors, or inconsistent results.

### Root Cause Analysis
The temporary table naming in `upsert_platform_data_to_snowflake()` function used only second-level timestamp precision:

```python
temp_table_name = f"temp_{platform}_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
```

**Problem**: If two partitions of the same platform start within the same second, they generate identical temp table names.

**Technical Root Cause**:
- **File**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/watermark_management.py`
- **Method**: `upsert_platform_data_to_snowflake()` (line 309)
- **Issue**: Insufficient uniqueness in temporary table naming for concurrent partitions

### Reproduction Steps
1. Configure a platform (e.g., BambooHR) with multiple partitions
2. Trigger multiple partitions to start simultaneously or within the same second
3. Both partitions generate same temp table name (e.g., `temp_bamboohr_jobs_20250630_143022`)
4. Second partition overwrites first partition's temp table with `overwrite=True`
5. Race condition occurs during MERGE and cleanup phases

### Expected vs Actual
**Expected**: Each partition should have its own isolated temporary table
**Actual**: Multiple partitions can share the same temporary table, causing data corruption

### Potential Impact
- **Data Corruption**: Partition A's MERGE uses Partition B's data due to temp table overwrite
- **Table Not Found Errors**: First partition to finish drops the shared temp table, causing second partition's MERGE to fail
- **Inconsistent Results**: Depending on timing, partial or incorrect data loads occur

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/watermark_management.py`

**Changes Made**:
Added unique UUID component to temporary table naming to ensure uniqueness even when partitions start simultaneously:

```python
# BEFORE (problematic):
temp_table_name = f"temp_{platform}_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# AFTER (fixed):
import uuid
unique_id = str(uuid.uuid4()).replace('-', '')[:8]  # 8 chars for readability
temp_table_name = f"temp_{platform}_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}"
```

**Result**: Each partition now gets a unique temp table name:
- `temp_bamboohr_jobs_20250630_143022_a1b2c3d4`
- `temp_bamboohr_jobs_20250630_143022_e5f6g7h8`

### Impact
- ✅ **Race Condition Eliminated**: Each partition gets its own isolated temporary table
- ✅ **Data Integrity Preserved**: No risk of partitions overwriting each other's data
- ✅ **Error Prevention**: Eliminates "table not found" errors during concurrent processing
- ✅ **Scalability Improved**: Multiple partitions can safely run simultaneously
- ✅ **Readability Maintained**: Temp table names remain identifiable with platform and timestamp

### Files Affected
- ✅ `dagster_betterjobs/transformations/watermark_management.py` - Added UUID-based unique naming

### Verification Steps
1. ✅ Test multiple partitions of the same platform starting simultaneously
2. ✅ Verify each partition generates unique temporary table names
3. ✅ Confirm no temp table conflicts or overwrite issues
4. ✅ Validate successful concurrent partition processing without errors
5. ✅ Check that temp tables are properly cleaned up after each partition completes

### Additional Notes
- **Immediate Fix**: Resolves the race condition completely with minimal code change
- **UUID Approach**: Uses standard library UUID for guaranteed uniqueness
- **Performance Impact**: Negligible - UUID generation is very fast
- **Backwards Compatible**: No breaking changes to existing functionality

---

## BUG-022: Master Company URLs Temp Table Race Condition

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Master Company URLs Ingestion (`snowflake_master_company_urls.py`)
**Date Reported:** 2025-07-06
**Date Resolved:** 2025-07-29

### Description
When the `snowflake_master_company_urls` asset is scheduled inside **jobs** it can be materialised several times in quick succession. Each run creates a temporary external table named `MASTER_COMPANY_URLS_TEMP_S3`. Because the name is static, the first run that finishes drops the table while another in-flight run still expects it, leading to:

```
002003 (42S02): SQL compilation error:
Object 'MASTER_COMPANY_URLS_TEMP_S3' does not exist or not authorized.
```

### Root Cause Analysis
- **File:** `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_master_company_urls.py`
- **Function:** `snowflake_master_company_urls` (around lines 600+)
- **Issue:** Temporary table name is hard-coded. Concurrent materialisations share the same name; when one run closes its Snowflake session it drops the temp table, breaking others.
- **Similarity:** Same pattern fixed previously in **BUG-021** for `watermark_management.py` temp tables.

### Reproduction Steps
1. Include `snowflake_master_company_urls` asset inside a Dagster job.
2. Trigger the job manually twice (or have overlapping schedules).
3. Observe that the second run fails during `CREATE OR REPLACE TEMPORARY` / subsequent query because the table was dropped by the first run.

### Expected vs Actual
**Expected:** Each run uses its own uniquely named temporary table so concurrent executions do not interfere.

**Actual:** All runs share `MASTER_COMPANY_URLS_TEMP_S3`; race condition causes `OBJECT DOES NOT EXIST` errors.

### Potential Impact
- **Pipeline Failures:** Job fails unpredictably on busy schedules.
- **Data Staleness:** Master company URL list may not refresh.
- **Operator Fatigue:** Manual re-triggers required.

### Suggested Fix (TBD)
Introduce a helper (e.g., `ensure_temp_object_exists(platform: str, base_name: str)`) that appends a short random/UUID suffix to the base temp table name, mirroring the approach in BUG-021. Asset then references the generated unique name for the entire session.

### Files Affected (planned)
- `dagster_betterjobs/utils/temp_object_utils.py` (new) – `ensure_temp_object_exists()`
- `dagster_betterjobs/assets/snowflake_master_company_urls.py` – replace static temp table name with call to helper.

### Verification Steps (after fix)
1. Trigger two concurrent materialisations – both should succeed.
2. Check Snowflake history: temp tables have unique names per run.
3. Confirm tables are cleaned up automatically when session closes.

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/schema_utils.py` - Added `ensure_temp_object_exists()` function
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_master_company_urls.py` - Updated to use unique temp tables and added failure detection

**Changes Made**:

**1. Created `ensure_temp_object_exists()` Utility Function**:
```python
def ensure_temp_object_exists(
    base_name: str,
    sql_file_path: str,
    snowflake: SnowflakeResource,
    context: AssetExecutionContext,
    session_specific: bool = True,
    existing_connection = None  # Required for TEMPORARY tables
) -> str:
```

**Key Features**:
- **Unique Naming**: Generates unique table names using timestamp + 8-char UUID
- **Session Consistency**: Uses existing connection for TEMPORARY table visibility
- **Template-Based**: Uses existing SQL template files as source of truth
- **Auto-Cleanup**: Session-specific TEMPORARY tables auto-drop when session ends

**2. Updated Asset Implementation**:
```python
# BEFORE (Race Condition):
temp_table_fqn = ensure_object_exists("tables/raw_master_company_urls_temp_s3.sql", ...)
# Static name: BETTERJOBS_DB.RAW.MASTER_COMPANY_URLS_TEMP_S3

# AFTER (Race Condition Fixed):
temp_table_fqn = ensure_temp_object_exists(
    "temp_master_company_urls_s3",
    "tables/raw_master_company_urls_temp_s3.sql",
    context.resources.snowflake,
    context,
    session_specific=True,
    existing_connection=conn  # Same session for visibility
)
# Unique name: BETTERJOBS_DB.RAW.temp_master_company_urls_s3_20250729_222551_d82699f5
```

**3. Added Comprehensive Failure Detection**:
- **File Processing Tracking**: Track attempted, succeeded, and failed file counts
- **Asset Failure Logic**: Asset now fails when all attempted files fail to process
- **Enhanced Monitoring**: Added detailed Dagster metadata for success/failure rates
- **Accurate Status Reporting**: Eliminates false positive successes

**4. Session Isolation Fix**:
The critical issue was that TEMPORARY tables in Snowflake are session-specific. The original implementation created the table in one session but tried to use it in another, causing "Table does not exist" errors even though creation succeeded.

### Impact
- ✅ **Race Condition Eliminated**: Each concurrent run gets unique temporary table names
- ✅ **Session Consistency**: TEMPORARY tables created and used within same connection/session
- ✅ **Accurate Status Reporting**: Asset correctly fails when all files fail to process
- ✅ **Enhanced Monitoring**: Detailed tracking of file processing success/failure rates
- ✅ **Automatic Cleanup**: TEMPORARY tables auto-drop when session ends
- ✅ **No Breaking Changes**: Backward compatible with existing functionality

### Files Affected
- ✅ `dagster_betterjobs/utils/schema_utils.py` - Added `ensure_temp_object_exists()` function
- ✅ `dagster_betterjobs/assets/snowflake_master_company_urls.py` - Updated temp table creation and failure detection
- ✅ `dagster_betterjobs/utils/test_schema_utils.py` - Added test coverage for new function

### Verification Steps
1. ✅ Trigger two concurrent materializations - both succeed with unique temp table names
2. ✅ Verify temp tables are session-specific and automatically cleaned up
3. ✅ Confirm asset fails when all files fail to process (no more false positives)
4. ✅ Check enhanced Dagster metadata shows file processing success/failure rates

### Additional Notes
- **Pattern Consistency**: Reuses UUID-based suffix strategy from BUG-021
- **Generic Solution**: `ensure_temp_object_exists()` can be used for future temp table needs
- **Session Management**: Critical insight about Snowflake TEMPORARY table session isolation
- **Comprehensive Fix**: Addresses both race condition and false positive success issues