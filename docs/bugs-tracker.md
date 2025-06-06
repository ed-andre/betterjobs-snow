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

## BUG-001: Greenhouse Jobs Discovery - Date Fields Not Populated

**Status**: Open
**Severity**: Medium
**Component**: Job Discovery Pipeline - Greenhouse
**Date Reported**: 2025-01-06

### Description
Several job records in the `greenhouse_jobs` table have `PUBLISHED_AT` and `UPDATED_AT` fields set to null, despite these values being present in the API response from Greenhouse.

### Root Cause Analysis
The issue appears to be in the date field extraction logic in `greenhouse_jobs_discovery.py`. The API response contains the date fields with the correct values:
- `published_at`: "2025-01-23T13:58:37-05:00"
- `updated_at`: "2025-05-28T11:03:51-04:00"

However, these values are not being properly extracted and mapped to the corresponding database fields during the job processing.

### Example Case
**Job Details:**
- Job ID: 6069349
- Company ID: 401cb0a3
- Job Title: Creative Director
- Job URL: https://www.sterlingbrands.com/careers?gh_jid=6069349

**API Response (excerpt):**
```json
{
    "id": 6069349,
    "title": "Creative Director",
    "updated_at": "2025-05-28T11:03:51-04:00",
    "published_at": "2025-01-23T13:58:37-05:00",
    ...
}
```

**Database Record:**
- PUBLISHED_AT: null
- UPDATED_AT: null

### Reproduction Steps
1. Run the greenhouse jobs discovery pipeline
2. Check job records in Snowflake `greenhouse_jobs` table
3. Identify records with null PUBLISHED_AT and UPDATED_AT
4. Cross-reference with API response data to confirm values exist

### Expected vs Actual
**Expected**: PUBLISHED_AT and UPDATED_AT fields should be populated with the date values from the API response
**Actual**: These fields are set to null despite the data being available in the source

### Impact
- Data quality issue affecting job freshness tracking
- Potential issues with job filtering based on publish/update dates
- Analytics and reporting may be affected due to missing temporal data

### Files Affected
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/greenhouse_jobs_discovery.py`

### Investigation Notes
- The issue appears to be in the job details extraction logic around lines 350-400
- Need to verify the field mapping and date parsing logic
- May be related to the `job_details.get()` calls for these specific fields

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

## Template for New Bugs
