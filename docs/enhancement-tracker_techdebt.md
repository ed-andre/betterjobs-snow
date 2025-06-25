## TECH-DEBT-001: Review LLM Standardization Validation Thresholds and Manual Review Criteria

**Status:** 🔍 **Planned**
**Priority:** High
**Component:** LLM Data Standardization & Validation
**Date Identified:** 2025-06-15
**Estimated Effort:** 2-3 days
**Business Impact:** High - Affects data quality metrics and manual review workload

### Problem Statement
The current LLM standardization validation process is marking excessive amounts of data for manual review, potentially due to overly strict thresholds and validation criteria that don't account for legitimate data patterns.

**Specific Issues Identified:**
1. **Null Value Handling**: Jobs without location in raw data have LLM correctly setting location as NULL, but validation treats this as a quality issue
2. **Manual Review Flagging**: Large percentage of records flagged for manual review without clear business justification
3. **Threshold Sensitivity**: Current confidence thresholds may be too strict for operational use
4. **Validation Logic**: Validation criteria don't distinguish between data quality issues and legitimate data patterns

### Business Impact
- **Manual Review Overhead**: Unnecessary manual review burden reducing operational efficiency
- **False Quality Alerts**: Validation alerts for legitimate data patterns causing alert fatigue
- **Process Inefficiency**: Resources spent reviewing correctly processed data
- **Data Pipeline Delays**: Excessive manual review requirements slowing data availability

### Root Cause Analysis Areas

**1. Location Standardization**
- Issue: Jobs without location data in source → LLM sets NULL → Validation flags as quality issue
- Expected: NULL location should be acceptable when source data lacks location information
- Current Threshold: Location validation may require non-NULL values inappropriately

**2. Confidence Score Thresholds**
- Issue: Current thresholds may be calibrated for perfect data rather than operational use
- Investigate: Skills confidence threshold (0.7), Keywords confidence threshold (0.6), Locations confidence threshold (0.8)
- Expected: Thresholds should balance quality with operational efficiency

**3. Manual Review Criteria**
- Issue: Multiple validation criteria trigger manual review flags without prioritization
- Investigate: Confidence thresholds, null value handling, standardization failure rates
- Expected: Manual review should focus on genuine data quality issues

**4. Data Completeness vs. Data Quality**
- Issue: Validation may conflate missing source data with processing errors
- Investigate: Null value handling, optional field validation, source data availability
- Expected: Distinguish between missing source data and processing failures

### Proposed Investigation Plan

**Phase 1: Data Analysis (1 day)**
1. **Manual Review Analysis**:
   - Analyze current manual review queue composition
   - Identify most common reasons for manual review flagging
   - Calculate manual review rates by category (skills, keywords, locations)

2. **Threshold Impact Analysis**:
   - Run sensitivity analysis on current confidence thresholds
   - Measure impact of threshold adjustments on manual review rates
   - Identify optimal thresholds balancing quality and efficiency

3. **Null Value Pattern Analysis**:
   - Analyze correlation between source data availability and NULL values
   - Identify legitimate NULL patterns vs. processing errors
   - Assess impact of NULL values on downstream analytics

**Phase 2: Validation Logic Review (1 day)**
1. **Review Validation Criteria**:
   - Audit all validation rules in `stage_llm_data_quality_validation`
   - Identify rules that may be too strict for operational use
   - Document business justification for each validation rule

2. **Threshold Calibration**:
   - Test different confidence thresholds against historical data
   - Measure precision/recall of manual review flagging
   - Identify optimal thresholds for each standardization category

3. **Null Value Handling Strategy**:
   - Define clear criteria for when NULLs are acceptable vs. concerning
   - Update validation logic to distinguish between missing source data and processing errors
   - Implement source data availability tracking

**Phase 3: Implementation (0.5 days)**
1. **Update Validation Thresholds**:
   - Adjust confidence thresholds based on analysis results
   - Update manual review criteria to focus on genuine quality issues
   - Implement graduated severity levels for validation issues

2. **Improve Null Value Handling**:
   - Update validation logic to account for source data availability
   - Add validation rules that distinguish between missing source data and processing errors
   - Update documentation to reflect new null value handling approach

3. **Enhanced Validation Reporting**:
   - Add metrics for source data availability
   - Implement validation context tracking (source data vs. processing issue)
   - Update quality dashboards to reflect new validation approach

### Files to Investigate and Modify

**Validation Logic Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/data_quality.py`
- `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_standardization.py`

**Configuration Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/config/llm_standardization_config.py`
- `pipeline/sql/llm_standardization/insert_*_rules.sql`

**Documentation Files:**
- `pipeline/docs/dev/stage_layer_llm_data_standardization_plan.md`
- `docs/data_governance_guide.md`

### Success Criteria

**Quantitative Goals:**
- Reduce manual review rate by 40-60% while maintaining data quality
- Decrease false positive validation alerts by 70%
- Maintain >95% data accuracy in downstream analytics
- Reduce manual review processing time by 50%

**Qualitative Goals:**
- Clear documentation of validation criteria and business justification
- Improved operational efficiency with reduced manual intervention
- Enhanced confidence in automated standardization process
- Better alignment between validation logic and business requirements

### Risks and Mitigation

**Risk: Lowering Quality Standards**
- Mitigation: Implement gradual threshold adjustments with monitoring
- Validation: A/B test new thresholds against historical data
- Rollback: Maintain ability to revert to previous thresholds if quality degrades

**Risk: Missing Genuine Quality Issues**
- Mitigation: Implement comprehensive testing of new validation logic
- Monitoring: Enhanced quality monitoring to detect missed issues
- Validation: Compare new approach against manually validated sample data

**Risk: Increased False Negatives**
- Mitigation: Implement graduated severity levels for validation issues
- Monitoring: Track downstream analytics quality metrics
- Validation: Regular audits of automatically approved data

### Implementation Timeline

**Day 1: Data Analysis**
- Morning: Manual review queue analysis
- Afternoon: Threshold sensitivity analysis and null value pattern review

**Day 2: Validation Logic Review**
- Morning: Audit validation criteria and threshold calibration
- Afternoon: Null value handling strategy and documentation

**Day 3: Implementation and Testing**
- Morning: Update validation thresholds and logic
- Afternoon: Enhanced reporting and documentation updates

### Related Issues
- Links to data quality governance documentation
- Integration with existing monitoring and alerting systems
- Coordination with downstream analytics requirements