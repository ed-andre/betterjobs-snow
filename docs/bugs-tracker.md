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

**Status**: Resolved
**Severity**: Critical
**Component**: Job Discovery Pipeline - Greenhouse
**Date Reported**: 2025-06-06
**Date Resolved**: 2025-01-06

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

**Status**: Resolved
**Severity**: Critical
**Component**: Job Discovery Pipeline - SmartRecruiters
**Date Reported**: 2025-06-06
**Date Resolved**: 2025-01-06

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
**Fixed**: Updated job ID extraction logic in `smartrecruiters_scraper.py` (lines 186-202)

**Changes Made**:
1. Modified URL parsing to specifically target the 3rd path segment (index 2)
2. Added logic to extract numeric part before any dash separator
3. Added validation to ensure extracted job ID is numeric
4. Improved error handling and logging for invalid job IDs

**New Logic**:
```python
# SmartRecruiters URL format: https://jobs.smartrecruiters.com/CompanyName/JobID-job-title
path_segments = urlparse(job_url).path.split('/')
if len(path_segments) >= 3:
    job_segment = path_segments[2]  # Get JobID-title segment
    job_id = job_segment.split('-')[0]  # Extract numeric part before dash
    # Validate numeric job ID
    if not re.match(r'^\d+$', job_id):
        job_id = None
```

This fix ensures proper extraction of numeric job IDs like `3743990008187744` instead of company names.

---

## BUG-004: Deduplication Strategy Too Aggressive

**Status**: Open
**Severity**: High
**Component**: Data Processing Pipeline - Stage Jobs Unified
**Date Reported**: 2025-06-06

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

### Impact
- Over-aggressive duplicate removal
- Valid jobs from different companies incorrectly filtered out
- Data loss affecting analytics and reporting

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_unified.py`

---

## BUG-005: Embedded Job Boards with Custom Domains Not Extracting Full Data

**Status:** RESOLVED ✅
**Priority:** High
**Component:** Greenhouse Scraper (`greenhouse_scraper.py`)
**Reported Date:** 2024-12-19
**Resolved Date:** 2024-12-19

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

## Template for New Bugs
