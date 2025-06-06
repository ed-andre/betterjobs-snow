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

## Template for New Bugs

```markdown
## BUG-XXX: [Brief Description]

**Status**: Open
**Severity**: [Critical/High/Medium/Low]
**Component**: [System Component]
**Date Reported**: [YYYY-MM-DD]

### Description
[Detailed description of the issue]

### Root Cause Analysis
[Analysis of why the bug occurs]

### Reproduction Steps
1. [Step 1]
2. [Step 2]
3. [Step 3]

### Expected vs Actual
**Expected**: [What should happen]
**Actual**: [What actually happens]

### Impact
[Description of impact on system/users]

### Files Affected
- [List of affected files]
```