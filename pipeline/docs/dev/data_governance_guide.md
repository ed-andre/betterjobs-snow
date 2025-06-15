# Data Standardization - Data Governance Guide

This guide provides comprehensive instructions for using the data quality and validation assets to ensure high-quality standardized data in the pipeline. It covers data quality monitoring, issue identification, review processes, and corrective actions.

## Table of Contents

1. [Overview](#overview)
2. [Data Quality Assets](#data-quality-assets)
3. [Finding Data Quality Issues](#finding-data-quality-issues)
4. [Understanding Quality Metrics](#understanding-quality-metrics)
5. [Manual Review Process](#manual-review-process)
6. [Making Corrections](#making-corrections)
7. [Monitoring and Alerting](#monitoring-and-alerting)
8. [Best Practices](#best-practices)
9. [Troubleshooting Guide](#troubleshooting-guide)

## Overview

The Data Standardization section of the pipeline includes comprehensive data quality validation and governance capabilities. These tools help ensure that:

- **Skills, keywords, and locations are accurately standardized**
- **Data coverage meets business requirements**
- **Confidence scores reflect data quality accurately**
- **Issues are identified proactively and resolved systematically**

### Key Data Quality Dimensions

1. **Data Completeness**: Coverage of jobs with standardized data
2. **Data Accuracy**: Correctness of standardization and classification
3. **Data Consistency**: Uniform formatting and classification across records
4. **Data Integrity**: Referential integrity and relationship consistency
5. **Data Freshness**: Timeliness of data updates and processing
6. **Data Relevance**: Business relevance and contextual appropriateness

## Data Quality Assets

### Available Validation Assets

| Asset | Purpose | Key Outputs |
|-------|---------|-----------|
| `stage_llm_data_quality_validation` | Core validation checks | Validation results with issue categorization |
| `stage_llm_quality_metrics` | KPI tracking and metrics | Quality metrics and trend data |
| `stage_llm_coverage_analysis` | Coverage gap analysis | Coverage reports by dimension |
| `stage_llm_confidence_monitoring` | Confidence distribution tracking | Quality distribution analysis |
| `stage_llm_manual_review_queue` | Review queue management | Prioritized items for human review |

### Key Tables for Data Governance

```sql
-- Primary tables for data governance queries
BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS    -- Validation issues and results
BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS               -- Current quality metrics
BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS_HISTORY       -- Historical metrics for trending
BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE           -- Items requiring human review
BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS             -- Coverage analysis results
BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_ANALYSIS           -- Confidence score analysis
```

## Finding Data Quality Issues

### 1. Quick Health Check

Start with this query to get an overall health snapshot:

```sql
-- Overall data quality health check - Most Recent Results
WITH latest_validation_run AS (
    SELECT MAX(CREATED_TIMESTAMP) as latest_run_time
    FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
),
quality_summary AS (
    SELECT
        qvr.VALIDATION_CATEGORY,
        COUNT(*) as total_checks,
        COUNT(CASE WHEN qvr.STATUS = 'PASSED' THEN 1 END) as passed_checks,
        COUNT(CASE WHEN qvr.STATUS = 'FAILED' THEN 1 END) as failed_checks,
        COUNT(CASE WHEN qvr.STATUS = 'WARNING' THEN 1 END) as warning_checks,
        ROUND((COUNT(CASE WHEN qvr.STATUS = 'PASSED' THEN 1 END)::FLOAT / COUNT(*)) * 100, 2) as pass_rate,
        MAX(qvr.CREATED_TIMESTAMP) as validation_run_time
    FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS qvr
    CROSS JOIN latest_validation_run lvr
    WHERE DATE(qvr.CREATED_TIMESTAMP) = DATE(lvr.latest_run_time)  -- Most recent validation run
    GROUP BY qvr.VALIDATION_CATEGORY
)
SELECT
    VALIDATION_CATEGORY,
    total_checks,
    passed_checks,
    failed_checks,
    warning_checks,
    pass_rate,
    validation_run_time,
    DATEDIFF(hour, validation_run_time, CURRENT_TIMESTAMP) as hours_since_validation,
    CASE
        WHEN pass_rate >= 95 THEN '🟢 EXCELLENT'
        WHEN pass_rate >= 85 THEN '🟡 GOOD'
        WHEN pass_rate >= 70 THEN '🟠 NEEDS ATTENTION'
        WHEN pass_rate = 0 AND failed_checks = 0 THEN '🟢 NO DATA'
        ELSE '🔴 CRITICAL'
    END as health_status,
    CASE
        WHEN DATEDIFF(hour, validation_run_time, CURRENT_TIMESTAMP) > 48 THEN '⚠️ STALE DATA'
        WHEN DATEDIFF(hour, validation_run_time, CURRENT_TIMESTAMP) > 24 THEN '⏰ AGING'
        ELSE '✅ FRESH'
    END as data_freshness
FROM quality_summary
ORDER BY pass_rate;

-- Alternative: Health check for last 7 days (for broader coverage)
/*

WITH quality_summary AS (
    SELECT
        VALIDATION_CATEGORY,
        COUNT(*) as total_checks,
        COUNT(CASE WHEN STATUS = 'PASSED' THEN 1 END) as passed_checks,
        COUNT(CASE WHEN STATUS = 'FAILED' THEN 1 END) as failed_checks,
        COUNT(CASE WHEN STATUS = 'WARNING' THEN 1 END) as warning_checks,
        ROUND((COUNT(CASE WHEN STATUS = 'PASSED' THEN 1 END)::FLOAT / COUNT(*)) * 100, 2) as pass_rate,
        MAX(CREATED_TIMESTAMP) as latest_validation
    FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
    WHERE CREATED_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP)
    GROUP BY VALIDATION_CATEGORY
)
SELECT * FROM quality_summary ORDER BY pass_rate;

*/
```

### 2. Active Issues Requiring Attention

Find current issues that need immediate attention:

```sql
-- Active issues requiring attention
SELECT
    VALIDATION_CATEGORY,
    ISSUE_TYPE,
    SEVERITY,
    ISSUE_COUNT,
    DESCRIPTION,
    FIRST_DETECTED,
    LAST_UPDATED,
    DATEDIFF(hour, FIRST_DETECTED, CURRENT_TIMESTAMP) as hours_open,
    CASE
        WHEN SEVERITY = 'HIGH' AND DATEDIFF(hour, FIRST_DETECTED, CURRENT_TIMESTAMP) > 4 THEN '🚨 URGENT'
        WHEN SEVERITY = 'MEDIUM' AND DATEDIFF(hour, FIRST_DETECTED, CURRENT_TIMESTAMP) > 24 THEN '⚠️ OVERDUE'
        WHEN SEVERITY = 'HIGH' THEN '🔥 HIGH PRIORITY'
        WHEN SEVERITY = 'MEDIUM' THEN '📋 MEDIUM PRIORITY'
        ELSE '📝 LOW PRIORITY'
    END as priority_flag
FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
WHERE STATUS IN ('ACTIVE') -- Issues exist
ORDER BY
    CASE SEVERITY WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    FIRST_DETECTED;
```

### 3. Coverage Analysis

Check data coverage across different dimensions:

```sql
-- Coverage analysis by category
WITH latest_analysis AS (
    SELECT MAX(CREATED_TIMESTAMP) as latest_analysis_time
    FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
)
SELECT
    ca.ANALYSIS_TYPE,
    ca.DIMENSION_NAME,
    ca.DIMENSION_VALUE,
    ca.TOTAL_JOBS,
    ca.JOBS_WITH_SKILLS,
    ca.JOBS_WITH_KEYWORDS,
    ca.JOBS_WITH_LOCATIONS,
    ca.SKILLS_COVERAGE_PCT,
    ca.KEYWORDS_COVERAGE_PCT,
    ca.LOCATIONS_COVERAGE_PCT,
    -- Overall coverage score (average of the three)
    ROUND((ca.SKILLS_COVERAGE_PCT + ca.KEYWORDS_COVERAGE_PCT + ca.LOCATIONS_COVERAGE_PCT) / 3, 2) as avg_coverage_pct,
    CASE
        WHEN (ca.SKILLS_COVERAGE_PCT + ca.KEYWORDS_COVERAGE_PCT + ca.LOCATIONS_COVERAGE_PCT) / 3 >= 85 THEN '🟢 EXCELLENT'
        WHEN (ca.SKILLS_COVERAGE_PCT + ca.KEYWORDS_COVERAGE_PCT + ca.LOCATIONS_COVERAGE_PCT) / 3 >= 75 THEN '🟡 GOOD'
        WHEN (ca.SKILLS_COVERAGE_PCT + ca.KEYWORDS_COVERAGE_PCT + ca.LOCATIONS_COVERAGE_PCT) / 3 >= 60 THEN '🟠 NEEDS IMPROVEMENT'
        ELSE '🔴 POOR'
    END as coverage_status,
    ca.GAP_ANALYSIS,
    ca.RECOMMENDATIONS,
    ca.CREATED_TIMESTAMP,
    DATEDIFF(hour, ca.CREATED_TIMESTAMP, CURRENT_TIMESTAMP) as hours_since_analysis
FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS ca
CROSS JOIN latest_analysis la
WHERE DATE(ca.CREATED_TIMESTAMP) = DATE(la.latest_analysis_time)  -- Most recent analysis
ORDER BY avg_coverage_pct DESC;

-- Alternative: Detailed breakdown by coverage type
/*
SELECT
    ANALYSIS_TYPE,
    DIMENSION_NAME,
    DIMENSION_VALUE,
    'Skills' as coverage_type,
    SKILLS_COVERAGE_PCT as coverage_pct,
    JOBS_WITH_SKILLS as jobs_covered,
    TOTAL_JOBS
FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
WHERE DATE(CREATED_TIMESTAMP) = (SELECT MAX(DATE(CREATED_TIMESTAMP)) FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS)

UNION ALL

SELECT
    ANALYSIS_TYPE,
    DIMENSION_NAME,
    DIMENSION_VALUE,
    'Keywords' as coverage_type,
    KEYWORDS_COVERAGE_PCT as coverage_pct,
    JOBS_WITH_KEYWORDS as jobs_covered,
    TOTAL_JOBS
FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
WHERE DATE(CREATED_TIMESTAMP) = (SELECT MAX(DATE(CREATED_TIMESTAMP)) FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS)

UNION ALL

SELECT
    ANALYSIS_TYPE,
    DIMENSION_NAME,
    DIMENSION_VALUE,
    'Locations' as coverage_type,
    LOCATIONS_COVERAGE_PCT as coverage_pct,
    JOBS_WITH_LOCATIONS as jobs_covered,
    TOTAL_JOBS
FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
WHERE DATE(CREATED_TIMESTAMP) = (SELECT MAX(DATE(CREATED_TIMESTAMP)) FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS)

ORDER BY coverage_type, coverage_pct DESC;
*/
```

### 4. Low Confidence Items

Identify items with low confidence scores that may need review:

```sql
-- Low confidence items by category
WITH latest_monitoring AS (
    SELECT MAX(CREATED_TIMESTAMP) as latest_monitoring_time
    FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING
)
SELECT
    cm.DATA_SOURCE,
    cm.CONFIDENCE_BUCKET,
    cm.ITEM_COUNT,
    cm.PERCENTAGE_OF_TOTAL,
    cm.AVG_CONFIDENCE_IN_BUCKET,
    cm.TREND_DIRECTION,
    cm.QUALITY_RATING,
    cm.ACTION_REQUIRED,
    CASE
        WHEN cm.QUALITY_RATING = 'poor' THEN '🔴 IMMEDIATE REVIEW'
        WHEN cm.QUALITY_RATING = 'needs_improvement' THEN '🟡 REVIEW SOON'
        WHEN cm.QUALITY_RATING = 'fair' THEN '📋 MONITOR'
        WHEN cm.QUALITY_RATING = 'good' THEN '🟢 GOOD'
        ELSE '❓ UNKNOWN'
    END as action_priority,
    CASE
        WHEN cm.TREND_DIRECTION = 'declining' THEN '📉 GETTING WORSE'
        WHEN cm.TREND_DIRECTION = 'improving' THEN '📈 GETTING BETTER'
        WHEN cm.TREND_DIRECTION = 'stable' THEN '➡️ STABLE'
        ELSE '❓ NO TREND DATA'
    END as trend_indicator,
    cm.CREATED_TIMESTAMP,
    DATEDIFF(hour, cm.CREATED_TIMESTAMP, CURRENT_TIMESTAMP) as hours_since_monitoring
FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING cm
CROSS JOIN latest_monitoring lm
WHERE DATE(cm.CREATED_TIMESTAMP) = DATE(lm.latest_monitoring_time)  -- Most recent monitoring
    AND cm.QUALITY_RATING IN ('poor', 'needs_improvement')  -- Focus on items needing attention
ORDER BY
    CASE cm.QUALITY_RATING
        WHEN 'poor' THEN 1
        WHEN 'needs_improvement' THEN 2
        ELSE 3
    END,
    cm.ITEM_COUNT DESC;

-- Alternative: All confidence buckets with details
/*
SELECT
    DATA_SOURCE,
    CONFIDENCE_BUCKET,
    ITEM_COUNT,
    PERCENTAGE_OF_TOTAL,
    AVG_CONFIDENCE_IN_BUCKET,
    QUALITY_RATING,
    ACTION_REQUIRED,
    TREND_DIRECTION
FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING
WHERE DATE(CREATED_TIMESTAMP) = (SELECT MAX(DATE(CREATED_TIMESTAMP)) FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING)
ORDER BY DATA_SOURCE, AVG_CONFIDENCE_IN_BUCKET;
*/
```

## Understanding Quality Metrics

### Key Performance Indicators (KPIs)

#### Coverage KPIs
- **Skills Coverage**: Percentage of jobs with normalized skills (Target: ≥85%)
- **Keywords Coverage**: Percentage of jobs with normalized keywords (Target: ≥80%)
- **Locations Coverage**: Percentage of jobs with normalized locations (Target: ≥75%)

#### Quality KPIs
- **High Confidence Rate**: Percentage of relationships with confidence ≥0.7 (Target: ≥80%)
- **Manual Review Rate**: Percentage requiring manual review (Target: ≤15%)
- **Standardization Success Rate**: Percentage successfully standardized (Target: ≥90%)

#### Operational KPIs
- **Processing Time**: Average time to complete normalization (Target: ≤30 minutes)
- **Error Rate**: Percentage of failed normalizations (Target: ≤2%)
- **Data Freshness**: Hours since last successful update (Target: ≤24 hours for jobs discovery, 7 days for full pipeline)

### Quality Metric Interpretation

```sql
-- Current quality metrics with interpretation
SELECT
    METRIC_CATEGORY,
    METRIC_NAME,
    METRIC_VALUE,
    METRIC_THRESHOLD_MIN,
    METRIC_THRESHOLD_MAX,
    METRIC_STATUS,
    CASE
        WHEN METRIC_STATUS = 'GREEN' THEN '✅ Meeting targets'
        WHEN METRIC_STATUS = 'YELLOW' THEN '⚠️ Below target but acceptable'
        WHEN METRIC_STATUS = 'RED' THEN '🚨 Critical - immediate attention required'
        ELSE '❓ Status unknown'
    END as interpretation,
    CASE
        WHEN METRIC_STATUS = 'RED' THEN 'Investigate root cause immediately'
        WHEN METRIC_STATUS = 'YELLOW' THEN 'Monitor closely and plan improvement'
        ELSE 'Continue current monitoring'
    END as recommended_action
FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
WHERE CALCULATION_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
ORDER BY
    CASE METRIC_STATUS WHEN 'RED' THEN 1 WHEN 'YELLOW' THEN 2 ELSE 3 END,
    METRIC_CATEGORY,
    METRIC_NAME;
```

## Manual Review Process

### 1. Accessing the Review Queue

The manual review queue prioritizes items that need human attention:

```sql
-- Current manual review queue with priorities
SELECT
    REVIEW_ID,
    ITEM_TYPE,
    ITEM_ID,
    REVIEW_REASON,
    CONFIDENCE_SCORE,
    PRIORITY_SCORE,
    REVIEW_STATUS,
    ASSIGNED_TO,
    CREATED_TIMESTAMP,
    CASE
        WHEN PRIORITY_SCORE >= 8 THEN '🔥 URGENT'
        WHEN PRIORITY_SCORE >= 6 THEN '📈 HIGH'
        WHEN PRIORITY_SCORE >= 4 THEN '📋 MEDIUM'
        ELSE '📝 LOW'
    END as priority_flag,
    DATEDIFF(day, CREATED_TIMESTAMP, CURRENT_DATE) as days_in_queue
FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
WHERE REVIEW_STATUS = 'PENDING'
ORDER BY PRIORITY_SCORE DESC, CREATED_TIMESTAMP;
```

### 2. Review Process Steps

#### Step 1: Claim an Item for Review
```sql
-- Claim an item for review (replace with your username)
UPDATE BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
SET
    REVIEW_STATUS = 'IN_REVIEW',
    ASSIGNED_TO = 'your_username',
    REVIEW_STARTED_DATE = CURRENT_TIMESTAMP
WHERE REVIEW_ID = 'review_id_to_review'
    AND REVIEW_STATUS = 'PENDING';
```

#### Step 2: Investigate the Issue
For different item types, use these queries:

**Skills Review:**
```sql
-- For skill standardization review
SELECT
    sn.SKILL_ID,
    sn.SKILL_NAME,
    sn.SKILL_NAME_CLEAN,
    sn.SKILL_CATEGORY,
    sn.ORIGINAL_VARIANTS,
    sn.CONFIDENCE_SCORE,
    sn.FREQUENCY_COUNT,
    -- Bridge relationships
    COUNT(jsb.BRIDGE_ID) as relationship_count,
    ARRAY_AGG(DISTINCT jsb.JOB_UID) as sample_jobs
FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn
LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
WHERE sn.SKILL_ID = 'skill_id_from_queue'
GROUP BY sn.SKILL_ID, sn.SKILL_NAME, sn.SKILL_NAME_CLEAN, sn.SKILL_CATEGORY,
         sn.ORIGINAL_VARIANTS, sn.CONFIDENCE_SCORE, sn.FREQUENCY_COUNT;
```

**Keywords Review:**
```sql
-- For keyword standardization review
SELECT
    kn.KEYWORD_ID,
    kn.KEYWORD_TEXT,
    kn.KEYWORD_TYPE,
    kn.KEYWORD_CATEGORY,
    kn.ORIGINAL_VARIANTS,
    kn.CONFIDENCE_SCORE,
    kn.FREQUENCY_COUNT,
    -- Bridge relationships
    COUNT(jkb.BRIDGE_ID) as relationship_count
FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED kn
LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON kn.KEYWORD_ID = jkb.KEYWORD_ID
WHERE kn.KEYWORD_ID = 'keyword_id_from_queue'
GROUP BY kn.KEYWORD_ID, kn.KEYWORD_TEXT, kn.KEYWORD_TYPE, kn.KEYWORD_CATEGORY,
         kn.ORIGINAL_VARIANTS, kn.CONFIDENCE_SCORE, kn.FREQUENCY_COUNT;
```

**Locations Review:**
```sql
-- For location standardization review
SELECT
    ln.LOCATION_ID,
    ln.LOCATION_NAME,
    ln.CITY,
    ln.STATE_PROVINCE,
    ln.COUNTRY,
    ln.LOCATION_TYPE,
    ln.ORIGINAL_VARIANTS,
    ln.CONFIDENCE_SCORE,
    ln.FREQUENCY_COUNT,
    -- Bridge relationships
    COUNT(jlb.BRIDGE_ID) as relationship_count
FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED ln
LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ln.LOCATION_ID = jlb.LOCATION_ID
WHERE ln.LOCATION_ID = 'location_id_from_queue'
GROUP BY ln.LOCATION_ID, ln.LOCATION_NAME, ln.CITY, ln.STATE_PROVINCE, ln.COUNTRY,
         ln.LOCATION_TYPE, ln.ORIGINAL_VARIANTS, ln.CONFIDENCE_SCORE, ln.FREQUENCY_COUNT;
```

#### Step 3: Complete the Review
```sql
-- Complete review with decision (replace with appropriate values)
UPDATE BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
SET
    REVIEW_STATUS = 'RESOLVED',  -- or 'NEEDS_CORRECTION' if changes needed
    REVIEW_DECISION = 'APPROVED',  -- or 'REJECTED', 'NEEDS_MODIFICATION'
    REVIEW_NOTES = 'Your detailed review notes here',
    REVIEWED_BY = 'your_username',
    REVIEW_COMPLETED_DATE = CURRENT_TIMESTAMP
WHERE REVIEW_ID = 'review_id_reviewed';
```

## Making Corrections

### 1. Correcting Skills Standardization

#### Adding New Standardization Rules
```sql
-- Add new skill standardization rule
INSERT INTO BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES (
    RULE_ID,
    PATTERN,
    STANDARDIZED_NAME,
    SKILL_CATEGORY,
    SKILL_SUBCATEGORY,
    CONFIDENCE_SCORE,
    RULE_TYPE,
    CREATED_TIMESTAMP
) VALUES (
    'rule_' || REPLACE(UUID_STRING(), '-', ''),
    'reactjs',  -- pattern to match
    'React',    -- standardized name
    'frameworks',
    'frontend_framework',
    0.95,
    'exact_match',
    CURRENT_TIMESTAMP
);
```

#### Updating Existing Skills
```sql
-- Update skill classification or naming
UPDATE BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
SET
    SKILL_CATEGORY = 'updated_category',
    SKILL_SUBCATEGORY = 'updated_subcategory',
    CONFIDENCE_SCORE = 0.9,
    MANUAL_REVIEW_FLAG = FALSE,
    APPROVED_BY_ADMIN = TRUE,
    UPDATED_TIMESTAMP = CURRENT_TIMESTAMP
WHERE SKILL_ID = 'skill_id_to_update';
```

#### Merging Duplicate Skills
```sql
-- Merge duplicate skills (transfer relationships to preferred skill)
BEGIN TRANSACTION;

-- Update bridge relationships to use preferred skill
UPDATE BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
SET
    SKILL_ID = 'preferred_skill_id',
    PROCESSING_METHOD = 'manual_merge'
WHERE SKILL_ID = 'duplicate_skill_id';

-- Update original variants in preferred skill
UPDATE BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
SET
    ORIGINAL_VARIANTS = ARRAY_CAT(
        ORIGINAL_VARIANTS,
        (SELECT ORIGINAL_VARIANTS FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED WHERE SKILL_ID = 'duplicate_skill_id')
    ),
    FREQUENCY_COUNT = FREQUENCY_COUNT + (SELECT FREQUENCY_COUNT FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED WHERE SKILL_ID = 'duplicate_skill_id')
WHERE SKILL_ID = 'preferred_skill_id';

-- Delete duplicate skill
DELETE FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
WHERE SKILL_ID = 'duplicate_skill_id';

COMMIT;
```

### 2. Correcting Keywords Standardization

#### Adding New Keyword Rules
```sql
-- Add new keyword standardization rule
INSERT INTO BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES (
    RULE_ID,
    PATTERN,
    STANDARDIZED_NAME,
    KEYWORD_TYPE,
    KEYWORD_CATEGORY,
    CONFIDENCE_SCORE,
    RULE_TYPE,
    CREATED_TIMESTAMP
) VALUES (
    'krule_' || REPLACE(UUID_STRING(), '-', ''),
    'fintech',
    'Financial Technology',
    'industry',
    'financial_services',
    0.95,
    'exact_match',
    CURRENT_TIMESTAMP
);
```

### 3. Correcting Location Standardization

#### Adding New Location Rules
```sql
-- Add new location standardization rule
INSERT INTO BETTERJOBS_DB.STAGE.LOCATION_STANDARDIZATION_RULES (
    RULE_ID,
    PATTERN,
    STANDARDIZED_NAME,
    CITY,
    STATE_PROVINCE,
    COUNTRY,
    LOCATION_TYPE,
    CONFIDENCE_SCORE,
    RULE_TYPE,
    CREATED_TIMESTAMP
) VALUES (
    'lrule_' || REPLACE(UUID_STRING(), '-', ''),
    'sf bay area',
    'San Francisco Bay Area',
    'San Francisco',
    'CA',
    'United States',
    'metro_area',
    0.9,
    'exact_match',
    CURRENT_TIMESTAMP
);
```

### 4. Refreshing Data After Corrections

After making corrections, trigger asset re-processing:

```python
# In Dagster UI or via API
# Re-run the affected standardization assets to apply new rules
```

## Monitoring and Alerting

### Setting Up Monitoring Queries

#### Daily Quality Report
```sql
-- Daily quality summary for monitoring
WITH daily_summary AS (
    SELECT
        CURRENT_DATE as report_date,
        -- Coverage metrics
        (SELECT METRIC_VALUE FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
         WHERE METRIC_NAME = 'skills_coverage_pct' AND DATA_SOURCE = 'overall') as skills_coverage,
        (SELECT METRIC_VALUE FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
         WHERE METRIC_NAME = 'keywords_coverage_pct' AND DATA_SOURCE = 'overall') as keywords_coverage,
        (SELECT METRIC_VALUE FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
         WHERE METRIC_NAME = 'locations_coverage_pct' AND DATA_SOURCE = 'overall') as locations_coverage,
        -- Quality metrics
        (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE WHERE REVIEW_STATUS = 'PENDING') as items_pending_review,
        (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
         WHERE ISSUE_TYPE IS NOT NULL AND STATUS = 'ACTIVE') as active_issues
)
SELECT
    report_date,
    skills_coverage,
    keywords_coverage,
    locations_coverage,
    items_pending_review,
    active_issues,
    CASE
        WHEN skills_coverage >= 85 AND keywords_coverage >= 80 AND locations_coverage >= 75
             AND items_pending_review <= 100 AND active_issues = 0 THEN '🟢 HEALTHY'
        WHEN active_issues > 0 OR items_pending_review > 500 THEN '🔴 NEEDS ATTENTION'
        ELSE '🟡 MONITORING'
    END as overall_status
FROM daily_summary;
```

#### Trend Analysis
```sql
-- Weekly trend analysis
SELECT
    DATE_TRUNC('week', CALCULATION_TIMESTAMP) as week_start,
    METRIC_ID,
    AVG(METRIC_VALUE) as avg_value,
    MIN(METRIC_VALUE) as min_value,
    MAX(METRIC_VALUE) as max_value,
    STDDEV(METRIC_VALUE) as std_deviation,
    LAG(AVG(METRIC_VALUE)) OVER (PARTITION BY METRIC_ID ORDER BY DATE_TRUNC('week', CALCULATION_TIMESTAMP)) as prev_week_avg,
    CASE
        WHEN LAG(AVG(METRIC_VALUE)) OVER (PARTITION BY METRIC_ID ORDER BY DATE_TRUNC('week', CALCULATION_TIMESTAMP)) IS NULL THEN 'NEW'
        WHEN AVG(METRIC_VALUE) > LAG(AVG(METRIC_VALUE)) OVER (PARTITION BY METRIC_ID ORDER BY DATE_TRUNC('week', CALCULATION_TIMESTAMP)) * 1.05 THEN 'IMPROVING'
        WHEN AVG(METRIC_VALUE) < LAG(AVG(METRIC_VALUE)) OVER (PARTITION BY METRIC_ID ORDER BY DATE_TRUNC('week', CALCULATION_TIMESTAMP)) * 0.95 THEN 'DECLINING'
        ELSE 'STABLE'
    END as trend
FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS_HISTORY
WHERE CALCULATION_TIMESTAMP >= DATEADD(week, -12, CURRENT_TIMESTAMP)
GROUP BY DATE_TRUNC('week', CALCULATION_TIMESTAMP), METRIC_ID
ORDER BY week_start DESC, METRIC_ID;
```

## Best Practices

### 1. Regular Monitoring Schedule

- **Daily**: Check overall health status and address critical issues
- **Weekly**: Review coverage trends and manual review queue
- **Monthly**: Analyze standardization rule effectiveness and update as needed
- **Quarterly**: Comprehensive quality assessment and process improvement

### 2. Issue Prioritization

**High Priority (Immediate Action)**:
- Failed validations affecting >5% of data
- Coverage drops below critical thresholds
- Processing failures or pipeline errors

**Medium Priority (Within 24 hours)**:
- Quality metrics in yellow status
- Manual review queue growing significantly
- Confidence score degradation trends

**Low Priority (Weekly review)**:
- Individual low-confidence items
- Minor standardization inconsistencies
- Process optimization opportunities

### 3. Documentation Standards

When making corrections or updates:
- Document the business reason for the change
- Include examples of the issue being addressed
- Test changes in a development environment first
- Monitor impact after implementing changes

### 4. Change Management Process

1. **Identify Issue**: Use monitoring queries to identify problems
2. **Investigate Root Cause**: Understand why the issue occurred
3. **Develop Solution**: Create appropriate standardization rules or corrections
4. **Test Solution**: Validate changes don't introduce new issues
5. **Implement**: Apply changes and monitor results
6. **Document**: Record changes and lessons learned

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue: Low Skills Coverage
**Symptoms**: Skills coverage metrics below 85%
**Investigation**:
```sql
-- Find jobs without skills
-- TODO: Look into potential bugs for when there's no company profile record for a company with jobs
SELECT
    ju.JOB_UID,
    ju.JOB_TITLE_CLEAN,
    ju.COMPANY_ID,
    cpr.COMPANY_NAME_STANDARDIZED,
    jle.TECHNICAL_SKILLS,
    jle.SOFT_SKILLS,
    jle.PRIMARY_KEYWORDS,
FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
LEFT JOIN BETTERJOBS_DB.STAGE.COMPANY_PROFILES cpr ON cpr.COMPANY_ID = ju.COMPANY_id
LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle ON ju.JOB_UID = jle.JOB_UID
WHERE jsb.JOB_UID IS NULL --AND ju.COMPANY_ID ='eb01d47c610f'
LIMIT 10;
```
**Solutions**:
- Check if LLM enrichment is working properly
- Verify extraction logic for VARIANT columns
- Add standardization rules for commonly missed skills

#### Issue: High Manual Review Queue
**Symptoms**: Manual review queue growing faster than resolution
**Investigation**:
```sql
-- Analyze review queue patterns
SELECT
    ITEM_TYPE,
    LEFT(REVIEW_REASON, 50) as issue_summary,
    COUNT(*) as occurrence_count,
    AVG(CONFIDENCE_SCORE) as avg_confidence,
    AVG(PRIORITY_SCORE) as avg_priority
FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
WHERE REVIEW_STATUS = 'PENDING'
GROUP BY ITEM_TYPE, LEFT(REVIEW_REASON, 50)
ORDER BY occurrence_count DESC;
```
**Solutions**:
- Identify patterns in review items and create standardization rules
- Adjust confidence thresholds if too many items are flagged
- Batch process similar issues together

#### Issue: Inconsistent Location Parsing
**Symptoms**: Same locations parsed differently
**Investigation**:
```sql
-- Find location parsing inconsistencies
SELECT
    LOCATION_NAME_CLEAN,
    COUNT(DISTINCT LOCATION_ID) as location_variants,
    ARRAY_AGG(DISTINCT CITY) as cities,
    ARRAY_AGG(DISTINCT STATE_PROVINCE) as states,
    ARRAY_AGG(DISTINCT COUNTRY) as countries
FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
GROUP BY LOCATION_NAME_CLEAN
HAVING COUNT(DISTINCT LOCATION_ID) > 1
ORDER BY location_variants DESC;
```
**Solutions**:
- Add specific location standardization rules
- Merge duplicate location entries
- Improve location parsing logic

### Performance Issues

#### Slow Quality Validation
If validation assets are running slowly:
1. Check table clustering and statistics
2. Optimize queries with proper indexing
3. Consider breaking large validations into smaller chunks
4. Monitor Snowflake warehouse usage and scaling

#### Memory Issues
If assets are failing due to memory:
1. Reduce batch sizes in processing
2. Use streaming operations instead of loading all data
3. Optimize SQL queries to reduce data transfer
4. Consider warehouse size adjustments

### Getting Help

For issues beyond this guide:
1. Check the Dagster asset logs for detailed error messages
2. Review the LLM standardization plan documentation
3. Contact the data engineering team with specific error details
4. Use Snowflake query history to investigate performance issues

---

## Appendix: Useful Queries

### Quick Status Checks

```sql
-- Overall pipeline health
SELECT 'Skills' as category, COUNT(DISTINCT jsb.JOB_UID) as jobs_covered,
       (COUNT(DISTINCT jsb.JOB_UID)::FLOAT / (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED)) * 100 as coverage_pct
FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb
UNION ALL
SELECT 'Keywords', COUNT(DISTINCT jkb.JOB_UID),
       (COUNT(DISTINCT jkb.JOB_UID)::FLOAT / (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED)) * 100
FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb
UNION ALL
SELECT 'Locations', COUNT(DISTINCT jlb.JOB_UID),
       (COUNT(DISTINCT jlb.JOB_UID)::FLOAT / (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED)) * 100
FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb;
```

### Data Quality Dashboard Query

```sql
-- Comprehensive dashboard query
SELECT
    CURRENT_TIMESTAMP as report_timestamp,
    -- Coverage
    (SELECT ROUND(AVG(CASE WHEN jsb.JOB_UID IS NOT NULL THEN 1 ELSE 0 END) * 100, 2)
     FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
     LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID) as skills_coverage_pct,

    -- Quality
    (SELECT ROUND(AVG(OVERALL_CONFIDENCE), 3)
     FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE) as avg_skills_confidence,

    -- Volume
    (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED) as total_skills,
    (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED) as total_keywords,
    (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED) as total_locations,

    -- Issues
    (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE WHERE REVIEW_STATUS = 'PENDING') as pending_reviews,
    (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
     WHERE ISSUE_TYPE IS NOT NULL AND STATUS = 'ACTIVE') as current_issues;
```

This guide provides comprehensive coverage of data governance activities for the LLM standardization pipeline. Regular use of these tools and processes will ensure high-quality, reliable standardized data for downstream analytics and business intelligence.