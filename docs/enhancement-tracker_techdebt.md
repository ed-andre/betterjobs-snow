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

## TECH-DEBT-003: Implement Safe Table Schema Change Management for Production Data

**Status:** 🔍 **Planned**
**Priority:** Medium
**Component:** Schema-as-Code Infrastructure - Table Evolution
**Date Identified:** 2025-06-26
**Estimated Effort:** 5-7 days (across 3 phases)
**Business Impact:** High - Prevents data loss and downtime during schema evolution

### Problem Statement
The current schema-as-code system handles view updates well but lacks a safe mechanism for table schema changes. Table modifications (column additions, removals, renames, type changes) require careful handling to prevent data loss, minimize downtime, and avoid costly full ETL reruns.

**Specific Issues Identified:**
1. **Dangerous Recreation Pattern**: Current approach would use `DROP TABLE` and recreate, causing immediate data loss
2. **No Migration Framework**: No systematic way to handle schema evolution while preserving data
3. **Downtime Risk**: Schema changes could require pipeline downtime and full data reprocessing
4. **Version Control Gap**: No tracking of table schema versions or change history
5. **Rollback Complexity**: No clean way to revert problematic schema changes

### Business Impact
- **Data Loss Risk**: Accidental table recreation could destroy historical data
- **Operational Downtime**: Schema changes requiring full ETL reruns (hours/days)
- **Development Friction**: Developers avoiding necessary schema improvements due to complexity
- **Production Instability**: Unsafe schema changes causing pipeline failures
- **Recovery Costs**: Expensive data recovery and pipeline rebuilding after schema issues

### Root Cause Analysis Areas

**1. Current View vs. Table Handling**
- Views: Safe `CREATE OR REPLACE` pattern or Hash-Based View Update Management (ENHANCEMENT-029) works perfectly
- Tables: No equivalent safe pattern - `DROP TABLE` causes data loss
- Impact: Asymmetric handling creates operational risks

**2. Schema Evolution Patterns**
- Issue: No framework for common schema changes (add/drop/rename columns)
- Current State: Manual schema changes outside of automated pipeline
- Impact: Schema drift between code and database

**3. Data Preservation Requirements**
- Issue: Production tables contain valuable historical data
- Current State: No automated data preservation during schema changes
- Impact: Risk of losing months/years of collected data

### Proposed 3-Phase Solution Strategy

**Phase 1: Immediate - Column Addition/Modification Strategy (1-2 days)**
*Handle safe, non-breaking schema changes*

**Scope**: Column additions, column constraint modifications, index changes
**Approach**: Direct ALTER statements for safe operations
**Risk Level**: Low - No data loss potential

**Implementation**:
1. **Safe Operation Detection**:
   - Identify schema changes that can be safely applied via ALTER statements
   - Column additions (new columns with DEFAULT values)
   - Column constraint relaxation (removing NOT NULL, increasing length)
   - Index additions/removals
   - Comment modifications

2. **Enhanced Hash-Based Detection**:
   - Extend view hash system to detect table schema changes
   - Parse SQL table definitions to identify safe vs. unsafe changes
   - Generate appropriate ALTER statements for safe changes

3. **Change Validation**:
   - Pre-flight checks to ensure ALTER operations won't break existing data
   - Dependency analysis to prevent breaking downstream views/tables
   - Rollback planning for each change type

**Phase 2: Medium-term - Migration-Based Approach (2-3 days)**
*Handle complex schema changes with data preservation*

**Scope**: Column renames, type changes, complex restructuring
**Approach**: Staged migration with temporary tables and data copying
**Risk Level**: Medium - Requires careful data migration

**Implementation**:
1. **Migration Framework**:
   - Create table migration tracking system (`STAGE.TABLE_MIGRATIONS`)
   - Migration script generation based on schema differences
   - Automated data copying with transformation support

2. **Staged Migration Process**:
   ```sql
   -- Step 1: Create new table with updated schema
   CREATE TABLE ANALYTICS.FACT_JOB_POSTINGS_V2 AS SELECT ... FROM ANALYTICS.FACT_JOB_POSTINGS;

   -- Step 2: Data validation and testing
   -- Step 3: Atomic swap (rename operations)
   ALTER TABLE ANALYTICS.FACT_JOB_POSTINGS RENAME TO ANALYTICS.FACT_JOB_POSTINGS_OLD;
   ALTER TABLE ANALYTICS.FACT_JOB_POSTINGS_V2 RENAME TO ANALYTICS.FACT_JOB_POSTINGS;

   -- Step 4: Drop old table after validation period
   ```

3. **Data Validation Pipeline**:
   - Automated row count validation
   - Data integrity checks during migration
   - Rollback triggers if validation fails

**Phase 3: Long-term - Blue-Green Deployment (2-3 days)**
*Zero-downtime schema changes for production*

**Scope**: Production-grade schema changes with zero downtime
**Approach**: Parallel schema deployment with traffic switching
**Risk Level**: High complexity, Low operational risk

**Implementation**:
1. **Blue-Green Infrastructure**:
   - Dual schema deployment (ANALYTICS_BLUE, ANALYTICS_GREEN)
   - Traffic routing configuration
   - Automated schema synchronization

2. **Deployment Process**:
   ```
   Current State: ANALYTICS → ANALYTICS_BLUE (active)
   Deployment: Deploy changes to ANALYTICS_GREEN (inactive)
   Testing: Validate ANALYTICS_GREEN with production data
   Switch: Route traffic from BLUE to GREEN
   Cleanup: ANALYTICS_BLUE becomes inactive, ready for next deployment
   ```

3. **Automated Rollback**:
   - Instant traffic switching back to previous schema
   - Health checks and automatic rollback triggers
   - Zero-downtime rollback capability

### Files to Investigate and Modify

**Phase 1 Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` - Extend table creation logic
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/schema_utils.py` - Add schema change detection
- `pipeline/sql/schema_setup/` - Add table migration tracking

**Phase 2 Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/table_migrations.py` - New migration asset
- `pipeline/dagster_betterjobs/dagster_betterjobs/transformations/migration_utilities.py` - Migration logic
- `pipeline/sql/migrations/` - New directory for migration scripts

**Phase 3 Files:**
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/blue_green_deployment.py` - Deployment asset
- `pipeline/dagster_betterjobs/dagster_betterjobs/resources.py` - Blue-green resource configuration
- `pipeline/sql/blue_green/` - Blue-green deployment SQL templates

### Success Criteria

**Phase 1 Success Criteria:**
- ✅ Safe column additions applied automatically without data loss
- ✅ Schema change detection works reliably for table modifications
- ✅ Rollback capability for safe schema changes
- ✅ Zero false positives in safe vs. unsafe change detection

**Phase 2 Success Criteria:**
- ✅ Complex schema changes (renames, type changes) handled safely with data preservation
- ✅ Migration validation prevents data corruption
- ✅ Rollback capability for complex migrations
- ✅ <30 minute downtime for complex schema changes

**Phase 3 Success Criteria:**
- ✅ Zero-downtime schema deployments in production
- ✅ Instant rollback capability (<1 minute)
- ✅ Automated health checks and rollback triggers
- ✅ Production-grade reliability and monitoring

### Risks and Mitigation

**Risk: Data Loss During Migration**
- Mitigation: Comprehensive data validation at each step
- Backup: Automated backups before any schema change
- Testing: Extensive testing on non-production data

**Risk: Migration Complexity**
- Mitigation: Start with simple cases, gradually handle complex scenarios
- Validation: Thorough testing of migration logic
- Rollback: Always maintain rollback capability

**Risk: Downtime During Transition**
- Mitigation: Phase 3 blue-green deployment eliminates downtime
- Monitoring: Real-time health checks during migrations
- Automation: Automated rollback on failure detection

**Risk: Dependency Breakage**
- Mitigation: Comprehensive dependency analysis before changes
- Testing: Validate downstream views and assets after changes
- Communication: Clear notification of breaking changes

### Implementation Timeline

**Phase 1 (Immediate): 1-2 days**
- Day 1: Safe schema change detection and ALTER statement generation
- Day 2: Testing and validation of safe schema changes

**Phase 2 (Medium-term): 2-3 days**
- Day 1: Migration framework and temporary table strategy
- Day 2: Data validation and staged migration process
- Day 3: Testing complex schema changes and rollback procedures

**Phase 3 (Long-term): 2-3 days**
- Day 1: Blue-green infrastructure setup
- Day 2: Traffic routing and automated deployment process
- Day 3: Health checks, monitoring, and rollback automation

### Related Issues
- Integration with existing schema-as-code system (ENHANCEMENT-029)
- Coordination with data governance and change management processes
- Alignment with CI/CD pipeline and deployment procedures
- Future integration with database versioning and audit trails
- Connection to disaster recovery and backup strategies

---

## TECH-DEBT-004: Database-Level View Versioning for Operational Diagnostics

**Status:** 🔍 **Planned**
**Priority:** Low
**Component:** Schema-as-Code Infrastructure - View Management Enhancement
**Date Identified:** 2025-06-26
**Estimated Effort:** 1.5 days
**Business Impact:** Low-Medium - Operational convenience without impacting core functionality

### Problem Statement
While ENHANCEMENT-029 implements hash-based view update management with git serving as the primary version control system, there's potential operational value in maintaining lightweight database-level versioning for deployed view definitions. This would complement git by providing immediate operational visibility into what's actually deployed and enabling quick runtime diagnostics.

**Current State:**
- Hash-based change detection works well for determining when to update views
- Git provides comprehensive source control and development history
- No database-level visibility into view deployment history or quick rollback capability

**Potential Operational Gaps:**
1. **Deployment Tracking**: Can't easily see what view version is deployed without git access
2. **Runtime Diagnostics**: When a view breaks, can't immediately see the previous working definition
3. **Cross-Environment Visibility**: No easy way to compare view versions across dev/staging/prod
4. **Operational Rollback**: No database-level rollback capability for quick fixes during incidents

### Business Justification
- **Operational Convenience**: Quick access to deployment history without requiring git knowledge
- **Incident Response**: Faster diagnostics during view-related production issues
- **Environment Management**: Better visibility into what's deployed where
- **Compliance**: Some environments may require database-level audit trails
- **Future Capabilities**: Foundation for automated rollback features

**Note**: This is a nice-to-have enhancement that complements rather than replaces git versioning. The core hash-based update management in ENHANCEMENT-029 provides the essential functionality.

### Technical Approach

**Lightweight Versioning Strategy**:
- Extend existing `VIEW_VERSION_TRACKING` table to store 2-3 previous versions
- Store minimal operational metadata, not full version history
- Focus on deployment tracking and quick diagnostics
- Configurable retention (can be disabled if not needed)

**Enhanced Tracking Table**:
```sql
-- Enhanced version of stage_view_version_tracking.sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.VIEW_VERSION_TRACKING (
    VIEW_NAME STRING,
    VERSION_NUMBER INTEGER,
    CONTENT_HASH STRING NOT NULL,

    -- Deployment tracking
    DEPLOYED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    ENVIRONMENT STRING DEFAULT 'development',
    DEPLOYED_BY STRING DEFAULT 'dagster_pipeline',

    -- Git integration (optional)
    GIT_COMMIT_HASH STRING,
    GIT_BRANCH STRING,

    -- Lightweight versioning (new addition)
    VIEW_DEFINITION TEXT,  -- Store actual SQL for quick access
    IS_CURRENT_VERSION BOOLEAN DEFAULT TRUE,
    ROLLBACK_REASON STRING,

    -- Metadata
    FILE_PATH STRING,
    UPDATE_REASON STRING DEFAULT 'content_changed',

    PRIMARY KEY (VIEW_NAME, VERSION_NUMBER)
) CLUSTER BY (VIEW_NAME, DEPLOYED_TIMESTAMP);
```

**Configuration Options**:
```python
VIEW_VERSIONING_CONFIG = {
    "enabled": False,  # Disabled by default
    "max_versions_per_view": 3,
    "store_view_definition": True,
    "include_git_metadata": True,
    "auto_cleanup_old_versions": True,
    "enable_runtime_queries": True  # Operational query utilities
}
```

### Implementation Plan

**Phase 1: Enhanced Tracking Infrastructure (0.5 days)**
1. **Extend Existing Table**:
   - Add versioning columns to current tracking table
   - Implement version number sequence generation
   - Add configurable view definition storage

2. **Configuration System**:
   - Make versioning optional and configurable
   - Environment-based configuration (disabled in dev, optional in prod)
   - Storage optimization settings

**Phase 2: Integration with Hash-Based System (0.5 days)**
1. **Extend View Update Logic**:
   - Integrate versioning into existing hash-based update process
   - Version creation only when hash changes (leverage existing logic)
   - Automatic cleanup of old versions based on retention policy

2. **Operational Utilities**:
   - Helper functions for quick version queries
   - View history and comparison utilities
   - Environment deployment status queries

**Phase 3: Operational Features (0.5 days)**
1. **Diagnostic Queries**:
   - Quick view deployment status queries
   - Cross-environment version comparison
   - Recent changes and deployment history

2. **Documentation and Testing**:
   - Operational runbook for using versioning features
   - Testing versioning with mock view updates
   - Performance validation for version storage

### Files to be Modified/Created

**New Files**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/view_versioning_utils.py` - Optional versioning utilities
- `pipeline/dagster_betterjobs/dagster_betterjobs/config/view_versioning_config.py` - Configuration management

**Modified Files**:
- `pipeline/sql/objects/tables/stage_view_version_tracking.sql` - Enhanced table schema
- `pipeline/dagster_betterjobs/dagster_betterjobs/utils/view_version_utils.py` - Add optional versioning
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/snowflake_setup.py` - Integrate with views_setup

### Success Criteria

**Functional Goals**:
- ✅ Configurable versioning that doesn't impact core functionality when disabled
- ✅ Minimal storage overhead (2-3 versions per view maximum)
- ✅ Quick operational queries for deployment status and history
- ✅ Seamless integration with existing hash-based update system

**Operational Goals**:
- ✅ <10 second response time for deployment status queries
- ✅ Clear operational visibility into what's deployed across environments
- ✅ Foundation for future rollback capabilities (if needed)
- ✅ Zero impact on development workflow when disabled

### Benefits

**Operational Advantages**:
- ✅ **Quick Diagnostics**: Immediate access to previous working view definitions
- ✅ **Deployment Tracking**: Clear visibility into what's deployed where
- ✅ **Incident Response**: Faster troubleshooting during view-related issues
- ✅ **Environment Management**: Easy comparison of versions across environments

**Implementation Advantages**:
- ✅ **Optional**: Can be completely disabled without affecting core functionality
- ✅ **Lightweight**: Minimal storage and performance impact
- ✅ **Complementary**: Works with git, doesn't replace it
- ✅ **Future-Ready**: Foundation for enhanced operational features

### Risks and Mitigation

**Risk: Storage Overhead**
- Mitigation: Configurable retention limits (2-3 versions maximum)
- Monitoring: Track storage usage and cleanup effectiveness
- Configuration: Can be disabled entirely if not needed

**Risk: Development Complexity**
- Mitigation: Keep versioning completely optional and separate from core logic
- Implementation: Simple extension of existing hash-based system
- Testing: Comprehensive testing with versioning both enabled and disabled

**Risk: Feature Creep**
- Mitigation: Clear scope limitation (operational diagnostics only)
- Focus: Complement git, don't compete with it
- Implementation: Simple configuration to enable/disable entire feature

### Implementation Priority

**Low Priority Justification**:
- Not critical for core pipeline functionality
- Git already provides comprehensive version control
- Operational benefit is convenience, not necessity
- Can be implemented after higher-priority work is complete

**When to Implement**:
- After ENHANCEMENT-029 is complete and stable
- When better deployment visibility is needed
- During production hardening phase
- As part of broader operational excellence initiatives

### Related Issues
- Builds on ENHANCEMENT-029 hash-based view update management
- Complements existing git-based version control
- Future integration with automated rollback capabilities
- Potential integration with broader deployment tracking systems

---

## TECH-DEBT-005: Replace Inline AI Skill Categorization CTE with Maintainable Mapping

**Status:** 🔍 **Planned**
**Priority:** Medium
**Component:** Skills Normalization Pipeline (`stage_skills_normalized`, rules/mapping tables)
**Date Identified:** 2025-07-04
**Estimated Effort:** 1 day
**Business Impact:** Medium – improves maintainability and consistency of skill categorization logic.

### Problem Statement
ENHANCEMENT-036 introduced an inline CTE (`ai_skill_category`) inside `stage_skills_normalized` to quickly bucket AI-related skill names under the `Artificial Intelligence` category and appropriate subcategories. While efficient, hard-coding business logic in Python SQL strings breaks separation-of-concerns and requires code changes for updates.

### Proposed Fix
1. Create dedicated rows in `SKILL_STANDARDIZATION_RULES` or a new `SKILL_CATEGORY_MAPPING` table to capture AI patterns and desired categories/subcategories.
2. Remove the CTE from `skills_normalization.py`, replacing it with a LEFT JOIN to the mapping table (similar to `stage_skill_family_mapping`).
3. Provide a data-population SQL file (`insert_ai_category_mappings.sql`) for easy updates by non-engineers.

### Success Criteria
• No AI logic remains in code; all pattern→category mappings live in tables.
• Updating AI pattern list requires only SQL changes, no pipeline redeploy.
• Unit tests confirm identical categorization results before/after migration.

### Risks & Mitigation
• Risk of category drift during migration – validate counts before and after.
• Performance impact – ensure join to mapping table is indexed.

---

## TECH-DEBT-006: Lightcast Taxonomy Update & Maintenance Process

**Status:** 🔍 Planned
**Priority:** Low
**Component:** Taxonomy Infrastructure (`lightcast_taxonomy` assets and tables)
**Date Identified:** 2025-07-04
**Estimated Effort:** 1 day
**Business Impact:** Automates ingestion of future Lightcast taxonomy releases, reducing manual maintenance and ensuring data freshness.

### Problem Statement
ENHANCEMENT-038 introduces the initial Lightcast taxonomy tables and loaders but lacks an automated mechanism to detect and ingest subsequent taxonomy releases. Without this process, the taxonomy risks becoming outdated, impacting data accuracy and analytical relevancy.

### Proposed Fix
1. Implement a `lightcast_taxonomy_version_check` asset to detect new CSV releases and manage version flags.
2. Add utilities for referential integrity validation and audit trail maintenance.
3. Schedule the check to run weekly and alert on validation failures.

### Success Criteria
• New taxonomy versions can be incorporated without code changes.
• `LATEST_VERSION` flags accurately reflect the active release.
• Validation passes with no referential integrity errors.

### Risks & Mitigation
• CSV schema drift – enforce pre-load schema validation.
• Large diff ingest time – load incrementally to avoid long locks.

---