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

---

## TECH-DEBT-002: Implement Comprehensive Email Alerting System for Asset Failures and Critical Validation Results

**Status:** 🔍 **Planned**
**Priority:** High
**Component:** Monitoring & Alerting Infrastructure
**Date Identified:** 2025-06-26
**Estimated Effort:** 3-4 days
**Business Impact:** High - Critical for operational monitoring and incident response

### Problem Statement
The current data pipeline lacks a comprehensive email alerting system to notify stakeholders of asset failures and critical validation results. This creates blind spots in operational monitoring and delays incident response times.

**Specific Issues Identified:**
1. **Asset Failure Notifications**: No automated alerts when Dagster assets fail, leading to delayed incident detection
2. **Schema Drift Alerts**: Schema drift detection from `schema_drift_validation` asset needs immediate notification to prevent downstream issues
3. **Data Quality Alerts**: Critical data quality issues need escalated notifications to data stewards
4. **Alert Prioritization**: No system to differentiate between critical vs. warning level issues
5. **Stakeholder Targeting**: No mechanism to route alerts to appropriate teams based on issue type

### Business Impact
- **Delayed Incident Response**: Asset failures go unnoticed until manual checks or downstream systems break
- **Data Quality Risk**: Critical validation failures may impact analytics without immediate awareness
- **Operational Blindness**: Lack of proactive monitoring creates reactive operational posture
- **Stakeholder Communication**: No systematic way to inform business users of data availability issues
- **SLA Violations**: Delayed response to issues may breach data availability SLAs

### Root Cause Analysis Areas

**1. Dagster Asset Failure Notifications**
- Issue: No built-in email notifications for asset execution failures
- Current State: Failures only visible in Dagster UI or logs
- Impact: Critical pipeline failures may go unnoticed for hours

**2. Schema Drift Validation Alerts**
- Issue: `schema_drift_validation` asset detects drift but no automated notifications
- Current State: Drift detection results only logged, not actively communicated
- Impact: Schema changes breaking downstream systems without advance warning

**3. Data Quality Validation Alerts**
- Issue: LLM standardization and other data quality checks need escalated notifications
- Current State: Quality issues logged but not systematically communicated
- Impact: Data quality degradation may go undetected

**4. Alert Infrastructure**
- Issue: No centralized alerting infrastructure for email notifications
- Current State: Logging-based monitoring without proactive notifications
- Impact: Reactive rather than proactive operational monitoring

### Proposed Investigation Plan

**Phase 1: Alert Infrastructure Setup (1.5 days)**
1. **Email Service Configuration**:
   - Set up SMTP configuration for email delivery
   - Configure email templates for different alert types
   - Implement email service abstraction layer

2. **Alert Routing System**:
   - Design stakeholder mapping for different alert types
   - Implement alert severity levels (Critical, Warning, Info)
   - Create distribution list management system

3. **Dagster Integration**:
   - Implement Dagster hooks for asset failure notifications
   - Configure success/failure callbacks for critical assets
   - Add email notification to asset execution context

**Phase 2: Validation Alert Implementation (1 day)**
1. **Schema Drift Alerts**:
   - Integrate email notifications into `schema_drift_validation` asset
   - Configure immediate alerts for schema drift detection
   - Add drift details and impact assessment to alert content

2. **Data Quality Alerts**:
   - Implement email notifications for LLM validation failures
   - Configure thresholds for different quality metrics
   - Add quality trend analysis to alert content

3. **Alert Prioritization**:
   - Implement alert severity classification
   - Configure different notification frequencies for different severities
   - Add alert suppression for recurring issues

**Phase 3: Monitoring and Optimization (1.5 days)**
1. **Alert Dashboard**:
   - Create monitoring dashboard for alert delivery status
   - Implement alert delivery tracking and retry logic
   - Add alert frequency and response time metrics

2. **Testing and Validation**:
   - Comprehensive testing of alert delivery for all scenarios
   - Validate alert content and routing accuracy
   - Test alert suppression and escalation logic

3. **Documentation and Training**:
   - Document alert configuration and maintenance procedures
   - Create stakeholder guide for alert interpretation
   - Implement alert acknowledgment and tracking system

### Files to Investigate and Modify

**Core Infrastructure Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/resources.py` - Add email service resource
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/` - New alerting utilities module
- `pipeline/dagster_betterjobs/dagster_betterjobs/hooks/` - New hooks module for asset callbacks

**Asset Modification Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/schema_validation.py` - Add email alerts
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/data_quality.py` - Add quality alerts
- `pipeline/dagster_betterjobs/dagster_betterjobs/definitions.py` - Register alert hooks

**Configuration Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/config/` - New alerting configuration
- Environment variables for SMTP configuration
- Stakeholder distribution lists configuration

**Template Files:**
- `pipeline/dagster_betterjobs/templates/` - New directory for email templates
- Asset failure notification template
- Schema drift alert template
- Data quality alert template

### Success Criteria

**Quantitative Goals:**
- 100% of critical asset failures trigger immediate email notifications
- Schema drift detection alerts delivered within 5 minutes
- Data quality issues above critical threshold trigger alerts within 10 minutes
- 95% alert delivery success rate
- <2 minutes average alert delivery time

**Qualitative Goals:**
- Clear, actionable alert content with context and remediation steps
- Appropriate stakeholder routing based on alert type and severity
- Reduced mean time to detection (MTTD) for critical issues
- Improved operational confidence and proactive monitoring
- Enhanced stakeholder communication and transparency

### Risks and Mitigation

**Risk: Alert Fatigue**
- Mitigation: Implement alert prioritization and frequency controls
- Validation: Monitor alert volume and response rates
- Rollback: Configurable alert thresholds and suppression rules

**Risk: Email Delivery Failures**
- Mitigation: Implement multiple delivery channels and retry logic
- Monitoring: Track delivery success rates and failure reasons
- Backup: Alternative notification channels (Slack, webhooks)

**Risk: Sensitive Data Exposure**
- Mitigation: Sanitize alert content and avoid including raw data
- Validation: Review all alert templates for data exposure risks
- Security: Implement secure email transmission and access controls

**Risk: Over-Alerting on False Positives**
- Mitigation: Implement smart thresholds and trend analysis
- Monitoring: Track alert accuracy and false positive rates
- Tuning: Continuous refinement of alert criteria based on feedback

### Implementation Timeline

**Day 1: Infrastructure Setup**
- Morning: SMTP configuration and email service implementation
- Afternoon: Alert routing system and stakeholder mapping

**Day 2: Dagster Integration**
- Morning: Asset failure hooks and callback implementation
- Afternoon: Core alerting utilities and template system

**Day 3: Validation Alert Implementation**
- Morning: Schema drift and data quality alert integration
- Afternoon: Alert prioritization and suppression logic

**Day 4: Testing and Optimization**
- Morning: Comprehensive testing and validation
- Afternoon: Documentation, monitoring dashboard, and deployment

### Related Issues
- Integration with existing logging and monitoring systems
- Coordination with Dagster UI and observability tools
- Alignment with incident response procedures and SLAs
- Future integration with Slack/Teams notifications
- Connection to data governance and quality standards

---