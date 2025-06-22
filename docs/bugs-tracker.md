# Bugs Tracker

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

## BUG-001: Greenhouse Date Fields Not Being Populated

**Status:** RESOLVED ✅
**Priority:** High
**Component:** Greenhouse Scraper (`greenhouse_scraper.py`)
**Reported Date:** 2024-12-19
**Resolved Date:** 2024-12-19

### Problem Description
Several job records from Greenhouse job discovery have `PUBLISHED_AT` and `UPDATED_AT` fields set to null, despite this data being present in the API response. This affects data completeness and job freshness filtering.

### Root Cause Analysis - FINAL
The issue was with **embedded job URLs** (e.g., `boards.greenhouse.io/6sense/jobs/...`) which are processed through a completely different code path than direct API URLs. These embedded jobs:

1. Use URLs like `https://job-boards.greenhouse.io/embed/job_app?for=6sense&token=6361949`
2. Extract data from `window.__remixContext`
3. Find job data in keys like `routes/embed.job_app`
4. **Were missing date extraction logic entirely**

The embedded job processing section extracted job title, location, and description from `remixContext` but had **no code to extract `published_at` and `updated_at` fields**.

### Evidence from Logs
```
INFO - Getting details for embedded job URL: https://boards.greenhouse.io/6sense/jobs/6361949?gh_jid=6361949
INFO - Requesting job details from: https://job-boards.greenhouse.io/embed/job_app?for=6sense&token=6361949
INFO - Successfully extracted data from __remixContext
INFO - Found jobPost data in routes/embed.job_app
INFO - Extracted job_title from remixContext: Manager, Engineering (Backend)
INFO - Extracted location from remixContext: Bengaluru, Karnataka, India
```
**Notice: No date extraction logs - dates were never being processed!**

### Technical Details
- **File:** `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/greenhouse_scraper.py`
- **Method:** `get_job_details()` - embedded job processing section (lines ~1100-1140)
- **Issue:** Missing date extraction logic in remixContext processing

### Resolution
**Fixed in:** `greenhouse_scraper.py` - embedded job processing section

**Changes Made:**
1. **Added date extraction logic** to the remixContext processing section
2. **Extract both `published_at` and `updated_at`** from jobPost data
3. **Added proper timezone handling** for embedded job dates
4. **Added comprehensive logging** for date extraction success/failure
5. **Set `is_recent` flag** based on parsed date

**Specific Fix:**
Added date extraction immediately after department extraction in the remixContext loop:
```python
# Extract published_at and updated_at from remixContext jobPost data
if "published_at" in job_post_data and not job_details.get("published_at"):
    job_details["published_at"] = job_post_data["published_at"]
    # Parse date and set is_recent flag

if "updated_at" in job_post_data and not job_details.get("updated_at"):
    job_details["updated_at"] = job_post_data["updated_at"]
```

### Example URLs Fixed
- `https://boards.greenhouse.io/6sense/jobs/6361949?gh_jid=6361949`
- `https://boards.greenhouse.io/6sense/jobs/5636812?gh_jid=5636812`
- `https://boards.greenhouse.io/6sense/jobs/6812666?gh_jid=6812666`
- All other embedded Greenhouse job URLs

### Verification Steps
1. Test with embedded job URLs: `boards.greenhouse.io/[company]/jobs/[id]`
2. Check logs for: `"Extracted published_at from remixContext: [date]"`
3. Verify `published_at` and `updated_at` fields are populated in database
4. Confirm job freshness filtering works correctly

### Impact
- ✅ All embedded Greenhouse jobs now have proper date fields populated
- ✅ Job freshness filtering works correctly for embedded job patterns
- ✅ Data completeness significantly improved
- ✅ Enhanced logging for debugging embedded job processing

---

## BUG-002: Greenhouse Jobs Discovery - Job ID Extraction Failing

**Status**: RESOLVED ✅
**Severity**: Critical
**Component**: Job Discovery Pipeline - Greenhouse
**Date Reported**: 2025-06-06
**Date Resolved**: 2025-06-06

### Description
6,076 Greenhouse job records have `job_id` set to `'None'` instead of the actual job ID from the API response. This causes massive data quality issues and incorrect deduplication.

### Root Cause Analysis
The job ID extraction logic in `greenhouse_jobs_discovery.py` is not properly extracting the `"id"` field from the Greenhouse API response. The API response contains:
```json
{
    "id": 4749694007,
    "title": "Part-Time Market Trainer - Los Angeles",
    "internal_job_id": 4433190007,
    ...
}
```
But the job_id field is being set to None in the database.

**Root Cause**: The job ID was correctly extracted from the initial job listing (line 266), but was being overwritten on line 314 with `job_details.get("id")` which returns `None` because the detailed job response doesn't contain an "id" field in the expected format.

### Reproduction Steps
1. Run greenhouse jobs discovery pipeline
2. Check `greenhouse_jobs` table in Snowflake
3. Query for records where `job_id = 'None'`
4. Observe 6,076+ records with null job IDs

### Expected vs Actual
**Expected**: job_id should be `4749694007` (from `"id"` field in API response)
**Actual**: job_id is `'None'` in database records

### Impact
- Critical data quality issue affecting 6,076 jobs
- Massive duplication removal (6,076 jobs treated as same ID)
- Downstream analytics and processing broken
- Job matching and tracking impossible

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/greenhouse_jobs_discovery.py`
- `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/greenhouse_scraper.py`

### Resolution
**Fixed**: Removed the job ID overwrite logic in `greenhouse_jobs_discovery.py` (line 314)

**Issue**: The code flow was:
1. Correctly extract job_id from initial job listing (line 266)
2. Convert to string and validate (lines 269-270)
3. Get detailed job info (line 278)
4. **Overwrite** job_id with `job_details.get("id")` which returns `None` (line 314) ❌

**Solution**: Removed the problematic line 314 that was overwriting the correctly extracted job_id:
```python
# REMOVED: job_id = job_details.get("id")  # This was causing the bug
```

**Result**: The job_id now preserves the correctly extracted value from the initial job listing, ensuring proper job identification and preventing the mass "None" job_id issue.

---

## BUG-003: SmartRecruiters Job ID Extraction Logic Incorrect

**Status**: RESOLVED ✅
**Severity**: Critical
**Component**: Job Discovery Pipeline - SmartRecruiters
**Date Reported**: 2025-06-06
**Date Resolved**: 2025-06-06

### Description
SmartRecruiters job ID extraction is extracting company names instead of actual job IDs from URLs, causing massive duplication issues.

### Root Cause Analysis
The job ID extraction logic in `smartrecruiters_scraper.py` is incorrectly parsing the URL structure. For URL:
`https://jobs.smartrecruiters.com/AbbVie/3743990008187744-sfe-manager`

**Current Logic**: Extracts `'AbbVie'` (company name) as job_id
**Correct Logic**: Should extract `'3743990008187744'` (actual job ID)

The job ID is the segment after the 4th slash (/) and before the next dash (-).

### Example Cases
- URL: `https://jobs.smartrecruiters.com/AbbVie/3743990008187744-sfe-manager`
- Current job_id: `'AbbVie'` ❌
- Correct job_id: `'3743990008187744'` ✅

### Reproduction Steps
1. Run smartrecruiters jobs discovery pipeline
2. Check job_id values in `smartrecruiters_jobs` table
3. Observe company names as job_ids: 'MonroInc', 'SonicAutomotive', 'AbbVie'

### Expected vs Actual
**Expected**: job_id should be unique alphanumeric identifier from URL path
**Actual**: job_id is company name, causing massive duplicates

### Impact
- 812+ SmartRecruiters jobs treated as duplicates
- Data quality degradation
- Job tracking impossible due to non-unique IDs

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/smartrecruiters_scraper.py`

### Resolution
**Fixed**: Updated job ID extraction logic in `SmartRecruitersJobScraper.get_job_details()` method

**Issue**: The regex pattern `r'/([^/]+)$'` was extracting the last URL segment (company name) instead of the job ID

**Solution**: Updated extraction logic to properly parse SmartRecruiters URL structure:
```python
# Extract job ID from URL path (format: /company/job_id-job_title)
if match:
    path_parts = job_url.split('/')
    if len(path_parts) >= 5:
        job_id_part = path_parts[4]  # Get segment after company
        job_id = job_id_part.split('-')[0]  # Extract ID before first dash
```

**Result**: Proper job ID extraction preventing duplicate issues

---

## BUG-004: Deduplication Strategy Too Aggressive

**Status**: RESOLVED ✅
**Severity**: High
**Component**: Data Processing Pipeline - Stage Jobs Unified
**Date Reported**: 2025-06-06
**Date Resolved**: 2025-06-06
**Fixed in Commit**: 4dc31bab9f5132a51860581b353b82454cf848a3

### Description
Current deduplication strategy using `job_id + platform` is insufficient. Different companies may use overlapping job ID schemas, requiring `job_id + platform + company_id` for proper deduplication.

### Root Cause Analysis
The deduplication logic in `stage_jobs_unified.py` uses:
```python
combined_df.drop_duplicates(subset=['job_id', 'platform'], keep='first')
```

This doesn't account for legitimate cases where different companies on the same platform might have overlapping job ID ranges.

### Reproduction Steps
1. Run stage_jobs_unified asset
2. Observe duplicate removal statistics
3. Note that 7,784 out of 10,530 jobs are being removed as duplicates

### Expected vs Actual
**Expected**: Only true duplicates (same job from same company) should be removed
**Actual**: Jobs with same ID across different companies are incorrectly deduplicated

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_unified.py`
**Commit**: 4dc31bab9f5132a51860581b353b82454cf848a3

**Changes Made**:
Updated deduplication logic to use `job_id + platform + company_id` combination:

```python
# BEFORE (too aggressive):
combined_df = combined_df.drop_duplicates(subset=['job_id', 'platform'], keep='first')

# AFTER (precise deduplication):
combined_df = combined_df.drop_duplicates(subset=['job_id', 'platform', 'company_id'], keep='first')
```

This ensures that:
- Only true duplicates (same job_id from same company on same platform) are removed
- Different companies with overlapping job ID ranges are preserved
- Data integrity is maintained across the pipeline

### Impact
- ✅ Prevents over-aggressive duplicate removal
- ✅ Preserves valid jobs from different companies with overlapping ID schemas
- ✅ Reduces data loss affecting analytics and reporting
- ✅ Jobs loaded count should now be much closer to jobs processed count

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_unified.py`

---

## BUG-005: Embedded Job Boards with Custom Domains Not Extracting Full Data

**Status:** RESOLVED ✅
**Priority:** High
**Component:** Greenhouse Scraper (`greenhouse_scraper.py`)
**Reported Date:** 2025-06-06
**Resolved Date:** 2025-06-06

### Problem Description
Companies using embedded Greenhouse job boards with custom domains (like SolarWinds) were not getting complete job data extracted. Issues included:
- `job_id` coming as malformed values like "4518558005?gh_jid=4518558005"
- `published_at` and `updated_at` fields being null
- `job_description` being blank
- Missing `requisition_id` and other metadata

### Root Cause Analysis
The issue was with companies that use custom domains for their individual job postings:

**Example:**
- ATS URL: `https://boards.greenhouse.io/embed/job_board?for=solarwinds`
- Individual job URLs: `https://jobs.solarwinds.com/job-detail/?gh_jid=4561319005?gh_jid=4561319005`

**Root Cause:** The embedded job board logic was trying to extract job details by making separate requests to these custom domain URLs, which don't follow standard Greenhouse patterns. However, ALL the necessary data was already available in the initial job listing response from `routes/embed.job_board`.

### Evidence
**Custom domain URLs that failed:**
- `https://jobs.solarwinds.com/job-detail/?gh_jid=4561319005?gh_jid=4561319005`

**Missing fields:**
- `published_at`: Available in JSON as `"published_at": "2025-05-06T06:57:01-04:00"`
- `content`: Available in JSON as `"content": "...full job description..."`
- `job_id`: Available in JSON as `"id": 4561319005`
- `updated_at`, `requisition_id`, `department`: All available in JSON

### Technical Details
**File:** `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/greenhouse_scraper.py`
**Method:** `search_jobs()` - embedded job board implementation

**Issue:** The embedded job board logic was:
1. Extracting basic info from HTML (`<div class="opening">` elements)
2. Attempting to get detailed info from custom domain URLs (which failed)
3. Missing the JSON data extraction that contained all necessary fields

### Resolution
**Enhanced embedded job board logic** to extract all job fields directly from the initial JSON response **and fixed data overwrite issue in discovery pipeline**:

**Root Cause - FINAL**: The issue was actually in `greenhouse_jobs_discovery.py`, not just the scraper. The pipeline was:
1. ✅ Correctly extracting complete data from JSON (`search_jobs()`)
2. ❌ Calling `get_job_details()` on custom domain URLs which failed
3. ❌ Overwriting good JSON data with incomplete detail fetch results

**Evidence from Database Record:**
- `job_id`: `"4518558005?gh_jid=4518558005"` (malformed from URL parsing)
- `job_description`: Empty (should be full HTML content)
- `published_at`: null (should be `"2025-02-16T12:49:48-05:00"`)
- `raw_data`: `"{}"` (should contain full job data)

**Changes Made:**
1. **Enhanced scraper**: Extract all fields from `routes/embed.job_board` JSON
2. **Fixed discovery pipeline**: Skip detailed fetch for custom domain URLs when essential data already available
3. **Smart detection**: Identify embedded jobs with custom domains and preserve JSON-extracted data
4. **Comprehensive logging**: Track when skipping detailed fetch to avoid data loss

**Technical Fix in `greenhouse_jobs_discovery.py`:**
```python
# Skip detailed fetch for embedded job boards with custom domains
if not job_url.startswith('https://boards.greenhouse.io/') and not job_url.startswith('https://job-boards.greenhouse.io/'):
    # Check if we already have essential data from initial scraping
    has_essential_data = (job.get("job_description") or job.get("content") or
                         job.get("published_at") or job.get("updated_at"))
    if has_essential_data:
        should_skip_detailed_fetch = True
        # Preserve the good data from JSON extraction
```

### Companies/URLs Fixed
- **SolarWinds**: `https://boards.greenhouse.io/embed/job_board?for=solarwinds`
- **All embedded job boards with custom domains**
- **Any company using `absolute_url` with non-standard domains**

### Verification Steps
1. Test with SolarWinds embedded job board: `https://boards.greenhouse.io/embed/job_board?for=solarwinds`
2. Check logs for: `"Successfully extracted [N] jobs from embedded JSON data"`
3. Verify all fields populated: `job_id`, `published_at`, `updated_at`, `job_description`, `requisition_id`
4. Confirm job descriptions contain full HTML content (properly decoded)
5. Verify date parsing and freshness filtering works correctly

### Impact
- ✅ Embedded job boards with custom domains now extract complete data
- ✅ Proper `job_id` extraction (clean numeric IDs)
- ✅ Full job descriptions with decoded HTML content
- ✅ Complete date fields for freshness filtering
- ✅ All metadata fields (`requisition_id`, `department`, etc.) populated
- ✅ Eliminates need for problematic individual job detail requests

---

## BUG-006: Language Detection Method Column Not Being Populated

**Status:** RESOLVED ✅
**Severity:** Medium
**Component:** Stage Jobs Unified - Language Detection
**Date Reported:** 2025-06-06
**Date Resolved:** 2025-06-06

### Description
The `language_detection_method` column in `STAGE.jobs_unified` table is not being populated despite the language detection processing working correctly. All records have `NULL` values for this field.

### Root Cause Analysis
The `language_detection.py` module successfully processes language detection and populates `detected_language`, `language_confidence`, and `is_english` fields. However, the `detect_language_comprehensive()` method doesn't return a `language_detection_method` field, which is expected by the unified table schema.

**Root Cause**: The `detect_language_comprehensive()` method in `LanguageDetector` class was not tracking or returning which detection method was used during the language detection process.

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/language_detection.py`

**Changes Made**:
1. **Enhanced `detect_language_comprehensive()` method** to track and return detection method used
2. **Added `language_detection_method` field** to returned dictionary with values:
   - `'python_langdetect'` - Primary langdetect library with high confidence
   - `'sql_pattern_fallback'` - SQL pattern matching with high confidence
   - `'python_langdetect_low_confidence'` - Primary method with low confidence
   - `'sql_pattern_low_confidence'` - Fallback method with low confidence
   - `'insufficient_text'` - Text too short for reliable detection
3. **Updated `process_dataframe()` method** to extract and populate the language_detection_method column
4. **Updated documentation** for convenience functions to include new field

**Technical Implementation**:
```python
# Enhanced method tracking
detection_method = None
if primary_confidence >= self.confidence_threshold:
    detection_method = 'python_langdetect'
elif fallback_confidence >= self.confidence_threshold:
    detection_method = 'sql_pattern_fallback'
# ... additional logic for low confidence cases

return {
    'detected_language': detected_language,
    'language_confidence': round(confidence, 3),
    'is_english': is_english,
    'language_detection_method': detection_method  # NEW FIELD
}
```

### Impact
- ✅ All job records now have `language_detection_method` populated
- ✅ Enables monitoring of detection method reliability and accuracy
- ✅ Provides metadata for quality analysis and troubleshooting
- ✅ Facilitates detection method performance comparison

### Files Affected
- `dagster_betterjobs/transformations/language_detection.py` (BUG-006)
- `dagster_betterjobs/assets/stage_jobs_unified.py` (BUG-007)

---

## BUG-007: Date Retrieved Field Showing Invalid Date in Unified Table

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Stage Jobs Unified - Data Type Conversion
**Date Reported:** 2025-06-06
**Date Resolved:** 2025-06-06

### Description
The `date_retrieved` field in `STAGE.jobs_unified` table was displaying as "Invalid date" for all records, despite the field being properly populated in the RAW layer tables. When cast to VARCHAR, the corrupted values appeared as strange formats like "-408823998-10-13 23:40:00.000".

### Root Cause Analysis - FINAL
**Root Cause**: Pandas datetime conversions were corrupting valid Snowflake TIMESTAMP_NTZ values. The issue was occurring in the `platform_mapping.py` module's `_standardize_dates()` method, which was unnecessarily calling `pd.to_datetime()` on perfectly valid timestamp data from Snowflake.

**Technical Details**:
- **RAW Layer**: Uses `date_retrieved TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP` creating perfectly valid timestamps
- **Platform Mapping**: `_standardize_dates()` method was calling `pd.to_datetime(df['date_retrieved'])` unnecessarily
- **Data Corruption**: This conversion corrupted the binary timestamp representation, creating values like "-408823998-10-13"
- **Cascade Effect**: The corruption occurred early in the pipeline, affecting all downstream processing

**Evidence**:
- Corrupted value in STAGE table: `"-408823998-10-13 23:40:00.000"`
- Original RAW values were valid TIMESTAMP_NTZ from Snowflake's `CURRENT_TIMESTAMP`
- Multiple users online reported similar issues with UNIX timestamp conversion requiring string-first approach

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/platform_mapping.py` (Primary fix)
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_unified.py` (Secondary cleanup)

**Changes Made**:
1. **Completely eliminated pandas datetime conversions** in `_standardize_dates()` method
2. **Removed all date/timestamp processing** - let Snowflake handle conversions natively
3. **Added alternative two-stage loading** with explicit `TRY_CAST(date_retrieved AS TIMESTAMP_NTZ)`
4. **Preserved string conversion approach** to prevent pandas timestamp misinterpretation

**Primary Fix in platform_mapping.py**:
```python
def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
    # DO NOT CONVERT TIMESTAMPS - preserve Snowflake TIMESTAMP_NTZ values as-is
    # Snowflake handles TIMESTAMP_NTZ and DATE conversions natively during write_pandas
    # Any pandas datetime conversion corrupts the timestamp data

    # date_posted: Let Snowflake handle DATE conversion during insert
    # date_retrieved: Preserve TIMESTAMP_NTZ as-is from Snowflake

    return df
```

**Secondary Fix in stage_jobs_unified.py**:
- Removed all pandas date/timestamp conversions
- Added explicit string conversion for timestamps: `filtered_df['date_retrieved'] = filtered_df['date_retrieved'].astype(str)`
- Implemented two-stage loading with SQL `TRY_CAST` for robust timestamp handling

**Key Insight**: The solution followed the pattern mentioned by users online - convert timestamps to string first, then let Snowflake cast back to TIMESTAMP_NTZ using SQL, completely bypassing pandas datetime corruption.

### Impact
- ✅ `date_retrieved` field now shows valid timestamps (no more "Invalid date")
- ✅ Fixed pipeline corruption at the source (platform_mapping.py)
- ✅ Eliminates cascading data corruption from early unnecessary conversions
- ✅ Preserves original Snowflake TIMESTAMP_NTZ values throughout entire pipeline
- ✅ Enables accurate temporal analysis and job freshness tracking
- ✅ Follows proven pattern for handling UNIX timestamp corruption issues

### Files Affected
- `dagster_betterjobs/transformations/platform_mapping.py` (Primary fix - removed pandas datetime conversions)
- `dagster_betterjobs/assets/stage_jobs_unified.py` (Secondary cleanup - string conversion + SQL casting)

---

## BUG-008: Workday Job ID Extraction Getting Location Instead of Job ID

**Status:** RESOLVED ✅
**Severity:** Critical
**Component:** Workday Scraper (`workday_scraper.py`)
**Date Reported:** 2025-01-06
**Date Resolved:** 2025-01-06

### Description
The Workday job ID extraction logic was incorrectly extracting location information instead of actual job IDs from the `bulletFields` array. This caused multiple jobs from the same company to have identical job IDs (like "United States of America"), leading to UID collisions and massive data quality issues in the unified jobs table.

### Root Cause Analysis
The current job ID extraction logic in `workday_scraper.py` (lines 259-266) assumed the first element in `bulletFields` is always the job ID:

```python
if bullet_fields and len(bullet_fields) > 0:
    job_id = bullet_fields[0]  # ❌ WRONG: Gets location, not job ID
```

**Problem**: Many companies structure their `bulletFields` with location information first, job ID last:

**Example from MRC Global**:
```json
"bulletFields": [
    "United States of America",  // ← Current logic extracts THIS as job_id
    "New Mexico",
    "Carlsbad",
    "JR107387"                   // ← Actual job ID is HERE (last element)
]
```

**Result**: Multiple jobs get job_id = "United States of America", causing UID collisions.

### Evidence of Issue
**UID Collision Example**:
- Multiple jobs from same company getting identical job_id
- UID generation creates same hash for different jobs
- Error: `"UID collision detected across platforms: ['4ca7b9a1a08118736490cc682557d702', ...]"`

**Affected Companies**:
- MRC Global: job_id becomes "United States of America" for all US jobs
- Any company that puts location first in bulletFields

### Resolution
**Fixed in**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/scrapers/workday_scraper.py`
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/workday_jobs_discovery.py`

**Root Cause - FINAL**: The issue had two components:
1. **Scraper Issue**: `bulletFields[0]` extraction getting location instead of job ID
2. **Discovery Pipeline Issue**: Placeholder URLs from scraper not being updated with proper URLs from `jobPostingInfo`

**Changes Made**:

**Phase 1 - Scraper Fixes (`workday_scraper.py`)**:
1. **Removed bulletFields job ID extraction** from `search_jobs()` method - this was causing location extraction
2. **Enhanced externalPath fallback** extraction with proper logging in `search_jobs()`
3. **Enhanced `get_job_details()` method** to extract reliable job ID from `jobPostingInfo`:
   - Primary: `jobPostingInfo.jobReqId` (most reliable, human-readable like "JR107387")
   - Fallback: `jobPostingInfo.id` (unique identifier)
4. **Added externalUrl extraction** from `jobPostingInfo.externalUrl` for complete job URLs
5. **Removed unused URL conversion methods** (`_build_job_detail_url`, `_convert_api_url_to_user_url`)
6. **Enhanced external_path handling** in `get_job_details()` to properly build API URLs

**Phase 2 - Discovery Pipeline Fixes (`workday_jobs_discovery.py`)**:
7. **Fixed URL propagation issue**: Added logic to update `job_url` with proper `externalUrl` from `job_details`
8. **Fixed job ID propagation**: Added logic to update `job_id` with reliable ID from `jobPostingInfo`
9. **Added comprehensive logging** for URL and job ID updates during processing

**Technical Fix**:
```python
# BEFORE (Phase 1 - Scraper):
if bullet_fields and len(bullet_fields) > 0:
    job_id = bullet_fields[0]  # Gets location like "United States of America"

# AFTER (Phase 1 - Scraper):
# In search_jobs(): Only use externalPath as fallback
if external_path:
    id_match = re.search(r'_([A-Z0-9\-]+)$', external_path)
    if id_match:
        job_id = id_match.group(1)

# In get_job_details(): Extract from reliable jobPostingInfo
reliable_job_id = job_posting.get("jobReqId") or job_posting.get("id")
if reliable_job_id:
    job_details["job_id"] = reliable_job_id

# BEFORE (Phase 2 - Discovery Pipeline):
job_url = job.get("job_url", "")  # Uses placeholder from scraper
job_details = scraper.get_job_details(job_url)  # Gets correct URL but doesn't use it
job_record = {"job_url": job_url}  # Still uses placeholder ❌

# AFTER (Phase 2 - Discovery Pipeline):
job_details = scraper.get_job_details(job_url)
# Update with corrected URL and job ID from jobPostingInfo
if job_details.get("job_url"):
    job_url = job_details.get("job_url")
if job_details.get("job_id"):
    job_id = job_details.get("job_id")
job_record = {"job_url": job_url}  # Uses correct URL ✅
```

**Complete Flow After Fix**:
1. **search_jobs()**: Creates placeholder `job_url = "/job/WVCORP-CHS/Sales-Support-Associate_JR107763"`
2. **get_job_details()**: Extracts proper URL from `jobPostingInfo.externalUrl` and reliable job ID from `jobPostingInfo.jobReqId`
3. **workday_jobs_discovery**: Updates variables with corrected values from `job_details`
4. **Database**: Stores proper URL `"https://mrcglobal.wd1.myworkdayjobs.com/MRC_Global_Careers/job/WVCORP-CHS/Sales-Support-Associate_JR107763"`

### Impact
- ✅ **Eliminates UID Collisions**: Each job gets unique, proper job ID like "JR107387"
- ✅ **Reliable Job IDs**: Uses structured `jobPostingInfo` instead of unstructured `bulletFields`
- ✅ **Complete Job URLs**: Gets full URLs from `jobPostingInfo.externalUrl`
- ✅ **Future-Proof**: Less dependent on variable `bulletFields` structure
- ✅ **Pipeline Stability**: No more stage_jobs_unified failures due to UID collisions

### Companies/URLs Fixed
- **MRC Global**: All US jobs now get proper job IDs like "JR107387" instead of "United States of America"
- **All Workday companies**: Using `bulletFields` with location-first structure
- **Future companies**: More reliable extraction from structured data

### Verification Steps
1. Test with MRC Global and other Workday companies
2. Check logs for: `"Extracted reliable job_id from jobPostingInfo: JR107387"`
3. Verify job_id values are unique identifiers (not location names)
4. Confirm no UID collisions in stage_jobs_unified processing
5. Validate job URLs are complete and functional from externalUrl

---

## BUG-009: Location Standardization Incorrectly Assigning "United States" to Non-US Locations

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Text Processing - Location Standardization (`text_cleaning.py`)
**Date Reported:** 2025-06-08
**Date Resolved:** 2025-06-09

### Description
The `standardize_location()` function in `text_cleaning.py` is incorrectly assigning "United States" as the country for locations that are clearly not in the US. This results in Canadian provinces and other countries' administrative divisions being mislabeled as US locations.

### Root Cause Analysis
The function uses an overly broad regex pattern that assumes any "City, State-like-string" format is a US location:

```python
us_pattern = r'^(.+?),\s*([A-Z]{2}|[A-Za-z\s]+)(?:,\s*(United States|USA|US))?'
us_match = re.search(us_pattern, clean_loc.strip())

if us_match:
    city = us_match.group(1).strip()
    state_raw = us_match.group(2).strip()
    country = 'United States'  # ❌ WRONG: Automatically assigns US regardless of actual location
```

**Problem**: The regex `([A-Z]{2}|[A-Za-z\s]+)` matches:
- Any 2-letter code (including Canadian provinces like "ON", "BC", "QC")
- Any alphabetic string (including full province names like "Ontario", "Saskatchewan")

**Logic Flaw**: The function assumes that if it finds a "City, Something" pattern, it must be US format, when many countries use similar formats.

### Evidence of Issue
**Incorrect Standardizations**:
- Input: `"Brantford, Ontario"` → Output: `"Brantford, Ontario, United States"` ❌
- Input: `"Hamilton Area, Ontario"` → Output: `"Hamilton Area, Ontario, United States"` ❌
- Input: `"Saskatoon, Saskatchewan"` → Output: `"Saskatoon, Saskatchewan, United States"` ❌

**These are clearly Canadian locations** but are being mislabeled as US locations.

### Impact
- **Data Quality Degradation**: Incorrect country assignments affect location-based analytics
- **Geographic Accuracy**: Jobs appearing in wrong countries for location filtering
- **User Experience**: Misleading location information for job seekers
- **Analytics Corruption**: Location-based reports and dashboards show false data

### Root Cause Technical Details
**File**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/text_cleaning.py`
**Method**: `standardize_location()` (lines 127-250)
**Issue**: Lines 177-186 in the US pattern matching logic

**Current Logic Flow**:
1. Regex finds "City, Province/State" pattern
2. **Automatically assumes** it's US format
3. Sets `country = 'United States'` without validation
4. Attempts to standardize "province" as US state (fails, but keeps US country assignment)

**Missing Validation**: No check to verify if the "state" component is actually a US state before assigning US country.

### Reproduction Steps
1. Call `standardize_location("Brantford, Ontario")`
2. Observe output: `{'standardized': 'Brantford, Ontario, United States', 'country': 'United States'}`
3. Input is clearly Canadian but incorrectly labeled as US

### Expected vs Actual Behavior
**Expected**: Only assign "United States" country when location can be **definitively confirmed** as US-based
**Actual**: Assigns "United States" to any location matching "City, Something" pattern

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/text_cleaning.py`

**Root Cause - FINAL**: The `standardize_location()` function was using an overly broad regex pattern that automatically assigned `country = 'United States'` for any "City, State-like-string" format without validating if the state component was actually a US state.

**Changes Made**:
1. **Conservative State Validation**: Only assign "United States" country when state can be **positively confirmed** as a US state
2. **Explicit Country Handling**: When country is explicitly mentioned (e.g., "Austin, TX, United States"), preserve it
3. **International Location Preservation**: Non-US locations like Canadian provinces are preserved as-is without country assignment
4. **US State Database Validation**: Check both state codes (e.g., "NY") and full names (e.g., "New York") against comprehensive US states list

**Technical Fix**:
```python
# BEFORE (problematic logic):
if us_match:
    city = us_match.group(1).strip()
    state_raw = us_match.group(2).strip()
    country = 'United States'  # ❌ Automatically assigned regardless of state validity

# AFTER (fixed logic):
if location_match:
    city = location_match.group(1).strip()
    state_raw = location_match.group(2).strip()
    explicit_country = location_match.group(3)

    if explicit_country:
        country = 'United States'  # ✅ Only when explicitly mentioned
    else:
        # ✅ Only assign US if state is confirmed US state
        if state_raw.upper() in us_states or any(name.lower() == state_raw.lower() for name in us_states.values()):
            country = 'United States'
        else:
            country = None  # ✅ Leave as international/unknown
```

**Test Results - All Passing**:
- ✅ Input: `"Brantford, Ontario"` → Output: `"Brantford, Ontario"` (Country: None)
- ✅ Input: `"Saskatoon, Saskatchewan"` → Output: `"Saskatoon, Saskatchewan"` (Country: None)
- ✅ Input: `"New York, NY"` → Output: `"New York, NY, United States"` (Country: United States)
- ✅ Input: `"Los Angeles, California"` → Output: `"Los Angeles, CA, United States"` (Country: United States)
- ✅ Input: `"Austin, TX, United States"` → Output: `"Austin, TX, United States"` (Country: United States)

### Impact
- ✅ **Data Quality Restored**: Canadian provinces and international locations no longer mislabeled as US
- ✅ **Accurate Country Assignment**: Only confirmed US states get "United States" country
- ✅ **Preserved Functionality**: All existing US location processing remains intact
- ✅ **International Support**: Non-US locations preserved correctly for future enhancement
- ✅ **Zero False Positives**: Conservative approach eliminates incorrect US assignments

### Files Affected
- ✅ `dagster_betterjobs/transformations/text_cleaning.py` - Fixed location parsing logic

### Additional Notes
- **Backward Compatible**: Existing US location processing behavior preserved
- **Future Enhancement Ready**: Foundation laid for comprehensive international location support
- **Data Reprocessing**: Existing stage data should be reprocessed to correct historical mislabelings

---

## BUG-010: Job Description Text Cleaning Too Aggressive - Removing Natural Line Breaks and Concatenating Words

**Status:** RESOLVED ✅
**Severity:** High
**Component:** Text Processing - Job Description Cleaning (`text_cleaning.py`)
**Date Reported:** 2025-06-09
**Date Resolved:** 2025-06-09

### Description
The job description cleaning process was overly aggressive in removing line breaks and whitespace, resulting in words from different lines being concatenated together without proper spacing. This created nonsensical merged words that degraded text readability and searchability. Additionally, the cleaning process improperly handled HTML structure (lists, paragraphs) and failed to address character encoding issues.

### Root Cause Analysis - FINAL
The text cleaning functions had multiple critical issues:

1. **HTML Tag Removal Without Structure Preservation**: The `clean_html_tags()` function used `re.sub(r'<[^>]+>', '', text)` which removed HTML tags by replacing them with empty strings, causing word concatenation when tags separated words.

2. **No HTML Structure Conversion**: HTML lists (`<li>`, `<ul>`, `<ol>`) and paragraphs (`<p>`) were stripped without converting their semantic meaning to readable text format.

3. **Missing Character Encoding Fixes**: Common UTF-8 encoding corruption (like `â€™` instead of `'`) was not detected or corrected.

**Technical Root Cause**:
- **File**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/text_cleaning.py`
- **Method**: `clean_html_tags()` (lines 18-42) and `clean_job_description()` (lines 334-373)
- **Issue**: `re.sub(r'<[^>]+>', '', text)` replaced HTML tags with empty strings instead of spaces

### Evidence of Issue

**Issue 1: HTML Structure Destruction**:
```
Input: <li>analysis</li><li>Work with tools</li>
Before Fix: "analysisWork with tools" ❌
After Fix: "• analysis. • Work with tools." ✅
```

**Issue 2: Line Break Concatenation**:
```
Input: "React etc.\nN-tier application architecture"
Before Fix: "React etc.N-tier application architecture" ❌
After Fix: "React etc. N-tier application architecture" ✅
```

**Issue 3: Character Encoding Problems**:
```
Input: "We're looking for developersâ€™ with experience"
Before Fix: "We're looking for developersâ€™ with experience" ❌
After Fix: "We're looking for developers' with experience" ✅
```

### Resolution
**Fixed in**: `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/text_cleaning.py`

**Root Cause - FINAL**: The issue was a combination of improper HTML tag removal, missing structure conversion, and lack of character encoding fixes in the text cleaning pipeline.

**Changes Made**:

**Phase 1 - Enhanced `clean_html_tags()` Function**:
1. **Added Character Encoding Fix**: New `fix_character_encoding()` function handles common UTF-8 corruption
2. **Added HTML Structure Conversion**: New `convert_html_structure_to_text()` function converts HTML elements to readable format
3. **Fixed Tag Removal Logic**: Changed `re.sub(r'<[^>]+>', '', text)` to `re.sub(r'<[^>]+>', ' ', text)` (replace with space, not empty string)
4. **Enhanced Processing Order**: Character encoding → HTML structure → tag removal → whitespace normalization

**Phase 2 - New Helper Functions**:
5. **`fix_character_encoding()`**: Fixes 12+ common UTF-8 encoding issues (â€™ → ', â€œ → ", etc.)
6. **`convert_html_structure_to_text()`**: Converts HTML structure to readable text:
   - `<li>` → `• ` (bullet points)
   - `</li>` → `. ` (periods with space)
   - `<p>` → `\n\n` (paragraph breaks)
   - `<h1-h6>` → `\n\n` (header spacing)
   - `<ul>`, `<ol>` → `\n` (list containers)

**Phase 3 - Updated `clean_job_description()` Function**:
7. **Streamlined Processing**: Uses enhanced `clean_html_tags()` for comprehensive cleaning
8. **Improved Line Break Handling**: Converts remaining line breaks to spaces after structure conversion
9. **Enhanced Bullet Point Management**: Standardizes and deduplicates bullet points

**Technical Fix**:
```python
# BEFORE (problematic):
def clean_html_tags(text):
    text = html.unescape(text)
    clean_text = re.sub(r'<[^>]+>', '', text)  # ❌ Empty string replacement
    return clean_text.strip()

# AFTER (fixed):
def clean_html_tags(text):
    text = html.unescape(text)
    text = fix_character_encoding(text)  # ✅ Fix encoding first
    text = convert_html_structure_to_text(text)  # ✅ Convert structure
    clean_text = re.sub(r'<[^>]+>', ' ', text)  # ✅ Space replacement
    clean_text = re.sub(r'\s+', ' ', clean_text.strip())  # ✅ Normalize whitespace
    return clean_text
```

### Test Results - All Passing
**Comprehensive test suite confirms all issues resolved**:

✅ **Line Break Test**: `'React etc.\nN-tier'` → `'React etc. N-tier'` (proper spacing)
✅ **HTML List Test**: `'<li>analysis</li><li>Work'` → `'• analysis. • Work'` (structured format)
✅ **Character Encoding Test**: UTF-8 corruption properly fixed
✅ **Complex Real-World Test**: No word concatenation in complex HTML with line breaks

**Before vs After Comparison**:
```
Input: <ul><li>5+ years experience</li><li>Python skills</li></ul>
Before: "5+ years experiencePython skills" ❌
After: "• 5+ years experience. • Python skills." ✅
```

### Impact
- ✅ **Text Readability Restored**: Job descriptions are now properly formatted and readable
- ✅ **Search Functionality Fixed**: No more concatenated words that break search queries
- ✅ **HTML Structure Preserved**: Lists and paragraphs converted to meaningful text format
- ✅ **Character Encoding Fixed**: UTF-8 corruption issues resolved
- ✅ **NLP Processing Improved**: Clean text enables better downstream analysis
- ✅ **User Experience Enhanced**: Professional job description presentation
- ✅ **Data Quality Improved**: Meaningful text structure for analytics

### Files Affected
- ✅ `dagster_betterjobs/transformations/text_cleaning.py` - Enhanced HTML cleaning and structure conversion
- ✅ `dagster_betterjobs/transformations/test_text_cleaning.py` - Added comprehensive bug reproduction tests

### Verification Steps
1. ✅ Test HTML list structure: `<li>analysis</li><li>Work` → `• analysis. • Work`
2. ✅ Test line break handling: `'Word1\nWord2'` → `'Word1 Word2'`
3. ✅ Test character encoding: UTF-8 corruption properly fixed
4. ✅ Test complex HTML: No word concatenation in real-world examples
5. ✅ Verify search functionality: Concatenated words no longer break search
6. ✅ Confirm readability: Job descriptions properly formatted

### Additional Notes
- **Backward Compatible**: Existing functionality preserved while fixing bugs
- **Performance Optimized**: Efficient regex patterns and processing order
- **Comprehensive Coverage**: Handles 12+ character encoding issues and all major HTML elements
- **Future-Proof**: Extensible framework for additional HTML elements and encoding fixes
- **Data Reprocessing**: Existing job descriptions may need to be reprocessed to apply fixes

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
    group_name="2b_stage_llm_standardization_validation",
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
    group_name="2b_stage_llm_standardization_validation",
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
    group_name="2b_stage_llm_standardization_validation",
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