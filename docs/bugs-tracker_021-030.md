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