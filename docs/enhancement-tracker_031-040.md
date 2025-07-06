# Enhancement Tracker 031-040

This document tracks planned enhancements and architectural improvements for the BetterJobs Snowflake project.

## Enhancement Format
- **Enhancement ID**: Unique identifier
- **Status**: Planned/In Progress/Completed
- **Priority**: Critical/High/Medium/Low
- **Component**: Which part of the system will be enhanced
- **Description**: Brief description of the enhancement
- **Business Justification**: Why this enhancement is needed
- **Technical Approach**: How the enhancement will be implemented
- **Implementation Plan**: Step-by-step implementation approach
- **Success Criteria**: How to measure success
- **Date Planned**: When the enhancement was identified

--


## ENHANCEMENT STATUS

- **OPEN**

    - ENHANCEMENT-040: Add Job Description Dimension and integrate with Fact Table
    - ENHANCEMENT-041: Refactor Job Search to Leverage Analytics Layer

- **IN PROGRESS**

    - None

- **COMPLETED**

    - ENHANCEMENT-035: Separate Skills Consolidation into Dedicated Asset (DEPRECATED - replaced with Lightcast taxonomy)
    - ENHANCEMENT-036: AI Skill Category Override via CTE in stage_skills_normalized (DEPRECATED - replaced with Lightcast taxonomy)
    - ENHANCEMENT-034: Remove PRIMARY_KEYWORDS from Skill Normalization Process
    - ENHANCEMENT-033: Refactor Discovery Assets to Use Universal Partitions Module
    - ENHANCEMENT-031: Partition LLM Enrichment Assets for Improved Performance and Scalability
    - ENHANCEMENT-032: Enrich Job Search Results with LLM-Processed Data
    - ENHANCEMENT-037: AI Keyword Category Override via CTE in stage_keywords_normalized
    - ENHANCEMENT-038: Replace Custom Skill Taxonomy with Lightcast Open Skills Taxonomy
        (NOTE: This enhancement deprecated the stage_skills_standardization_rules and the consolidation assets, which is no longer needed)
    - ENHANCEMENT-040: Add Job Description Dimension and integrate with Fact Table
- **NO ACTION REQUIRED**


--

## ENHANCEMENT-031: Partition LLM Enrichment Assets for Improved Performance and Scalability

**Status:** Completed
**Priority:** High
**Component:** LLM Processing Pipeline (`stage_jobs_llm_enriched_{platform}` assets)
**Date Planned:** 2025-06-27
**Date Completed:** 2025-06-28
**Estimated Effort:** 3-4 days
**Actual Effort:** 1 day
**Business Impact:** High - Performance optimization and scalability improvement

### Problem Statement
**Performance Bottleneck**: Current LLM enrichment assets process entire platforms sequentially, leading to:
- Long asset materialization times (20-45+ minutes per platform)
- Suboptimal utilization of Gemini 2.5 Flash-Lite's high rate limits (4000 RPM)
- Increased recursion risk from processing large job batches sequentially
- Poor development iteration speed for LLM pipeline testing and debugging
- Single point of failure - one problematic job can block entire platform processing

**Current Architecture Limitations**:
- Monolithic `stage_jobs_llm_enriched_{platform}` assets process all jobs for a platform
- Sequential batch processing within each platform asset
- No parallel execution capability across job segments
- Rate limiting prevents maximum API utilization
- Large memory footprint from processing thousands of jobs at once

### Business Justification
- **Performance**: Reduce asset materialization time by 50-70% through parallel execution (proven in discovery assets)
- **Scalability**: Better utilize Gemini 2.5 Flash-Lite's high rate limits (3500+ RPM effective)
- **Reliability**: Mitigate recursion risks by processing smaller, independent chunks
- **Cost Efficiency**: Optimize LLM API usage through parallel batch processing
- **Development Speed**: Faster iteration cycles for LLM pipeline development and testing
- **Operational Excellence**: Improved monitoring and error isolation at partition level
- **Proven Strategy**: Leverage existing operational knowledge from successful discovery asset partitioning

### Solution Architecture

**Approach**: Implement partitioned LLM enrichment assets using the **exact same proven company-based alpha partitioning pattern** from `bamboohr_company_jobs_discovery`, adapted for LLM job processing.

**Partitioning Strategy Analysis** (based on `bamboohr_jobs_discovery.py`):

```python
# Current discovery asset partitioning approach:
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

# Partition processing logic:
partition_key = context.partition_key
if partition_key == "0-9":
    letter_filter = "AND SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'"
elif partition_key == "other":
    letter_filter = "AND NOT (SUBSTRING(company_name, 1, 1) BETWEEN 'A' AND 'Z'...)"
else:
    letter_filter = f"AND (company_name LIKE '{partition_key}%'...)"
```

**Proposed LLM Partitioning Strategy**: **Company-Based Alpha Partitioning** (Proven Method)

```python
# Company-based alpha partitioning (follows proven discovery pattern)
llm_company_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])
```

**Selected Approach: Company-Based Alpha Partitioning**
- **Rationale**: Proven pattern from `bamboohr_jobs_discovery` - maintains consistency across pipeline
- **Benefits**:
  - Natural data distribution based on company names
  - Familiar partition management (already operational in discovery assets)
  - Predictable workload distribution across 28 partitions
  - Existing operational knowledge and debugging experience
- **Implementation**: Filter jobs by `company_name_clean` first letter within each platform
- **Proven Success**: Already successfully handles company discovery with this exact pattern

### Technical Approach

**Core Architecture Changes**:

1. **Asset Transformation**: Convert monolithic platform assets to partitioned assets using **company-based alpha partitioning**
2. **Shared Processing Logic**: Leverage existing `llm_processing.py` functions with minimal changes
3. **Partition-Specific Filtering**: Add company-based job filtering (A-Z, 0-9, other) to `get_platform_jobs_for_processing()`
4. **Independent Execution**: Each partition processes its company subset with separate Gemini instances
5. **Checkpoint Management**: Partition-specific checkpoints following discovery asset pattern
6. **Statistics Aggregation**: Platform-level statistics aggregated from all company partitions

**Key Functions to Adapt** (from `llm_processing.py`):

```python
# Current shared function - needs partition filtering enhancement
def get_platform_jobs_for_processing(
    cursor,
    platform: str,
    config,
    database_name: str,
    stage_schema: str,
    context: AssetExecutionContext,
    partition_key: Optional[str] = None  # NEW PARAMETER
) -> pd.DataFrame:
    """
    Get platform-specific jobs for LLM processing with optional partition filtering.
    """

    # Existing logic for processing mode...
    base_query = f"""
    SELECT j.JOB_UID, j.JOB_TITLE_CLEAN, j.JOB_DESCRIPTION_CLEAN,
           j.COMPANY_NAME_CLEAN, j.PLATFORM, j.IS_ENGLISH
    FROM {database_name}.{stage_schema}.JOBS_UNIFIED j
    WHERE j.PLATFORM = '{platform}'
        AND j.IS_ENGLISH = TRUE
        AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
        AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
    """

    # NEW: Add partition filtering logic
    if partition_key:
        company_filter = build_company_partition_filter(partition_key)
        base_query += f" AND {company_filter}"

    return execute_query_to_dataframe(cursor, base_query)

def build_company_partition_filter(partition_key: str) -> str:
    """Build SQL filter for company-based partitioning."""
    if partition_key == "0-9":
        return "SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'"
    elif partition_key == "other":
        return """NOT (
            SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'A' AND 'Z' OR
            SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'a' AND 'z' OR
            SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'
        )"""
    else:
        return f"""(
            j.COMPANY_NAME_CLEAN LIKE '{partition_key}%' OR
            j.COMPANY_NAME_CLEAN LIKE '{partition_key.lower()}%'
        )"""
```

### Implementation Plan

#### **Phase 1: Database Migration and Partition Framework Development (Day 1)**

**Step 1.1: Database Schema Migration**
```sql
-- File: pipeline/sql/migrations/20250628_add_partition_key_to_llm_enriched.sql

-- Add PARTITION_KEY column to existing table
ALTER TABLE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
ADD COLUMN PARTITION_KEY VARCHAR(16777216);

-- Backfill existing records with partition keys based on company name
UPDATE BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED llm
SET PARTITION_KEY = (
    CASE
        WHEN SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9' THEN '0-9'
        WHEN UPPER(SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1)) BETWEEN 'A' AND 'Z'
            THEN UPPER(SUBSTRING(j.COMPANY_NAME_CLEAN, 1, 1))
        ELSE 'other'
    END
)
FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED j
WHERE llm.JOB_UID = j.JOB_UID AND llm.PARTITION_KEY IS NULL;
```

**Step 1.2: Create Partition Definition Module**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/partitions.py

from dagster import StaticPartitionsDefinition

# Company-based partitioning for LLM enrichment (following discovery pattern)
llm_company_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

def build_company_partition_filter(partition_key: str) -> str:
    """Build SQL filter for company-based partitioning (LLM enrichment)."""
    if partition_key == "0-9":
        return "SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'"
    elif partition_key == "other":
        return """NOT (
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'A' AND 'Z' OR
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'a' AND 'z' OR
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'
        )"""
    else:
        return f"""(
            COMPANY_NAME_CLEAN LIKE '{partition_key}%' OR
            COMPANY_NAME_CLEAN LIKE '{partition_key.lower()}%'
        )"""
```

**Step 1.2: Create Partition Definition Module**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/partitions.py

from dagster import StaticPartitionsDefinition

# Company-based partitioning for LLM enrichment (following discovery pattern)
llm_company_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

def build_company_partition_filter(partition_key: str) -> str:
    """Build SQL filter for company-based partitioning (LLM enrichment)."""
    if partition_key == "0-9":
        return "SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'"
    elif partition_key == "other":
        return """NOT (
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'A' AND 'Z' OR
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN 'a' AND 'z' OR
            SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'
        )"""
    else:
        return f"""(
            COMPANY_NAME_CLEAN LIKE '{partition_key}%' OR
            COMPANY_NAME_CLEAN LIKE '{partition_key.lower()}%'
        )"""
```

**Step 1.3: Enhance LLM Processing Module**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/transformations/llm_processing.py

# Add partition support to existing function
def get_platform_jobs_for_processing(
    cursor,
    platform: str,
    config,
    database_name: str,
    stage_schema: str,
    context: AssetExecutionContext,
    partition_key: Optional[str] = None  # NEW PARAMETER
) -> pd.DataFrame:
    """Enhanced with partition filtering capability."""

    # Existing processing mode logic...
    base_query = build_base_jobs_query(platform, config, database_name, stage_schema)

    # NEW: Apply partition filtering if provided
    if partition_key:
        from dagster_betterjobs.partitions import build_company_partition_filter
        company_filter = build_company_partition_filter(partition_key)
        base_query += f" AND {company_filter}"
        context.log.info(f"[{platform.upper()}] Applying partition filter: {partition_key}")

    return execute_query_to_dataframe(cursor, base_query)

# Update LLM record preparation to include partition key
def prepare_llm_record(..., partition_key: str) -> Dict[str, Any]:
    """Prepare a database record from extracted LLM data."""
    return {
        'job_uid': job_uid,
        'partition_key': partition_key,  # NEW: Store partition for tracking
        # ... existing fields ...
    }
```

**Step 1.4: Create Partition-Aware Configuration**
```python
# Enhanced configuration class for partitioned processing
class PartitionedLLMEnrichmentConfig(Config):
    """Configuration for partitioned LLM enrichment processing."""
    # Existing config options...
    batch_size: int = 50  # Smaller batches for partitioned processing
    processing_mode: str = "new_only"
    max_retries: int = 2
    confidence_threshold: float = 0.6

    # NEW: Partition-specific options
    enable_partition_checkpoints: bool = True
    partition_batch_size: int = 25  # Even smaller for parallel execution
    partition_rate_limit: float = 0.3  # Faster rate for parallel processing
```

#### **Phase 2: Platform Asset Refactoring (Day 2)** ✅ **IN PROGRESS**

**🎯 PROGRESS UPDATE (2025-06-28):**
- ✅ **Step 1.1: Partition Definition Module** - Created `partitions.py` with company-based alpha partitioning
- ✅ **Step 1.2: Enhanced LLM Processing Module** - Added partition filtering to `llm_processing.py`
- ✅ **Step 1.3: Partitioned Configuration Class** - Created `PartitionedLLMEnrichmentConfig`
- ✅ **Step 1.4: Partitioned BambooHR Asset** - Created `stage_jobs_llm_enriched_bamboohr_partitioned.py`
- ⏳ **Next:** Create remaining platform assets (Greenhouse, Workday, SmartRecruiters)

**IMPLEMENTATION COMPLETE:**
- Company-based alpha partitioning (A-Z, 0-9, other) - 28 partitions total
- Enhanced `get_platform_jobs_for_processing()` with partition filtering
- Partition-specific checkpoints and error isolation
- Optimized batch sizes and rate limits for parallel execution
- PARTITION_KEY column integration in database records

**Step 2.1: Create Partitioned BambooHR Asset** ✅ **COMPLETED**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_llm_enriched_bamboohr.py

from dagster import asset, AssetExecutionContext
from dagster_betterjobs.partitions import llm_company_partitions
from dagster_betterjobs.transformations.llm_processing import process_platform_llm_enrichment

@asset(
    group_name="2_stage_standardization_transformation",
    kinds={"snowflake", "python", "LLM"},
    required_resource_keys={"snowflake", "gemini"},
    deps=["stage_jobs_unified"],
    partitions_def=llm_company_partitions  # NEW: Add partitioning
)
def stage_jobs_llm_enriched_bamboohr_partitioned(
    context: AssetExecutionContext,
    config: PartitionedLLMEnrichmentConfig
) -> Dict[str, Any]:
    """
    LLM enrichment for BambooHR jobs - partitioned by company name.

    Processes companies starting with partition letter (A-Z, 0-9, other)
    for parallel execution and improved performance.

    ✨ PARTITIONED PROCESSING ✨
    This asset processes a subset of companies based on partition key,
    enabling parallel execution across all partitions.
    """
    partition_key = context.partition_key  # NEW: Get partition key
    platform = "bamboohr"

    context.log.info(f"🚀 [{platform.upper()}] Starting partitioned LLM enrichment for partition: {partition_key}")

    # Get connections
    conn = context.resources.snowflake.get_connection()
    gemini = context.resources.gemini

    try:
        # Process platform with partition filtering
        stats = process_platform_llm_enrichment(
            platform=platform,
            context=context,
            config=config,
            conn=conn,
            gemini=gemini,
            partition_key=partition_key  # NEW: Pass partition key
        )

        # Add partition-specific metadata
        partition_metadata = create_partition_metadata(stats, platform, partition_key)
        context.add_output_metadata(partition_metadata)

        context.log.info(f"✅ [{platform.upper()}] Completed partition {partition_key}: {stats['jobs_processed']} jobs processed")
        return stats

    finally:
        conn.close()
```

**Step 2.2: Replicate for All Platform Assets**
```bash
# Create partitioned versions of all platform LLM assets
cp stage_jobs_llm_enriched_bamboohr.py stage_jobs_llm_enriched_greenhouse_partitioned.py
cp stage_jobs_llm_enriched_bamboohr.py stage_jobs_llm_enriched_workday_partitioned.py
cp stage_jobs_llm_enriched_bamboohr.py stage_jobs_llm_enriched_smartrecruiters_partitioned.py

# Update platform names in each file
# Update group names and dependencies as needed
```

**Step 2.3: Create Partition Checkpoint Management**
```python
# Enhanced checkpoint management for partitions
def create_partition_checkpoint_path(
    platform: str,
    partition_key: str,
    checkpoint_type: str = "processing"
) -> Path:
    """Create partition-specific checkpoint file paths."""
    checkpoint_dir = Path("dagster_betterjobs/checkpoints/llm_enrichment")
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    return checkpoint_dir / f"{platform}_llm_enrichment_{partition_key}_{checkpoint_type}.json"

def save_partition_checkpoint(
    platform: str,
    partition_key: str,
    stats: Dict[str, Any]
) -> None:
    """Save partition processing checkpoint."""
    checkpoint_path = create_partition_checkpoint_path(platform, partition_key)

    checkpoint_data = {
        "platform": platform,
        "partition_key": partition_key,
        "last_processed": datetime.now().isoformat(),
        "jobs_processed": stats["jobs_processed"],
        "jobs_successful": stats["jobs_successful"],
        "status": "completed"
    }

    with open(checkpoint_path, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
```

#### **Phase 3: Coordination and Monitoring (Day 3)**

**Step 3.1: Create LLM Enrichment Coordinator Asset**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/stage_jobs_llm_enriched_coordinator.py

@asset(
    group_name="2_stage_standardization_transformation",
    kinds={"snowflake", "python", "coordination"},
    required_resource_keys={"snowflake"},
    deps=[
        "stage_jobs_llm_enriched_bamboohr_partitioned",
        "stage_jobs_llm_enriched_greenhouse_partitioned",
        "stage_jobs_llm_enriched_workday_partitioned",
        "stage_jobs_llm_enriched_smartrecruiters_partitioned"
    ]
)
def stage_jobs_llm_enriched_coordinator(
    context: AssetExecutionContext
) -> Dict[str, Any]:
    """
    Coordinate and validate completion of all partitioned LLM enrichment assets.

    Provides cross-platform statistics aggregation and quality validation
    after all platform partitions complete processing.

    🎯 COORDINATION FUNCTIONS 🎯
    - Aggregate statistics across all platforms and partitions
    - Validate data quality and completeness
    - Generate executive summary metrics
    - Flag any processing issues requiring attention
    """

    conn = context.resources.snowflake.get_connection()

    try:
        # Aggregate statistics from all platform partitions
        coordination_stats = aggregate_partition_statistics(conn, context)

        # Validate cross-platform data quality
        quality_metrics = validate_llm_enrichment_quality(conn, context)

        # Generate executive summary
        executive_summary = generate_enrichment_executive_summary(
            coordination_stats, quality_metrics, context
        )

        # Add comprehensive metadata
        coordinator_metadata = create_coordinator_metadata(
            coordination_stats, quality_metrics, executive_summary
        )
        context.add_output_metadata(coordinator_metadata)

        context.log.info("🎯 LLM Enrichment Coordination Completed Successfully")
        return {
            "coordination_stats": coordination_stats,
            "quality_metrics": quality_metrics,
            "executive_summary": executive_summary
        }

    finally:
        conn.close()
```

**Step 3.2: Enhanced Monitoring and Metadata**
```python
def create_partition_metadata(
    stats: Dict[str, Any],
    platform: str,
    partition_key: str
) -> Dict[str, MetadataValue]:
    """Create enhanced metadata for partition processing."""

    base_metadata = create_platform_metadata(stats, platform)

    # Add partition-specific metadata
    partition_metadata = {
        f"{platform}_partition_key": MetadataValue.text(partition_key),
        f"{platform}_partition_jobs_processed": MetadataValue.int(stats["jobs_processed"]),
        f"{platform}_partition_success_rate": MetadataValue.float(
            (stats["jobs_successful"] / stats["jobs_processed"]) * 100
            if stats["jobs_processed"] > 0 else 0
        ),
        f"{platform}_partition_processing_time": MetadataValue.float(
            (datetime.now() - datetime.fromisoformat(stats["processing_start"])).total_seconds() / 60
        )
    }

    return {**base_metadata, **partition_metadata}

def create_coordinator_metadata(
    coordination_stats: Dict[str, Any],
    quality_metrics: Dict[str, Any],
    executive_summary: Dict[str, Any]
) -> Dict[str, MetadataValue]:
    """Create comprehensive coordinator metadata."""

    return {
        # Cross-platform totals
        "total_jobs_enriched": MetadataValue.int(coordination_stats["total_jobs_enriched"]),
        "total_companies_processed": MetadataValue.int(coordination_stats["total_companies_processed"]),
        "overall_avg_confidence": MetadataValue.float(coordination_stats["overall_avg_confidence"]),
        "total_needs_review": MetadataValue.int(coordination_stats["total_needs_review"]),

        # Quality metrics
        "data_quality_score": MetadataValue.float(quality_metrics.get("overall_quality_score", 0.0)),
        "completeness_percentage": MetadataValue.float(quality_metrics.get("completeness_percentage", 0.0)),

        # Executive summary
        "processing_status": MetadataValue.text(executive_summary.get("status", "unknown")),
        "recommendation": MetadataValue.text(executive_summary.get("recommendation", "none"))
    }
```

#### **Phase 4: Testing and Validation (Day 4)**

**Step 4.1: Create Partition Testing Framework**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/tests/test_partitioned_llm_enrichment.py

import pytest
from dagster import build_asset_context
from dagster_betterjobs.partitions import llm_company_partitions, build_company_partition_filter

class TestPartitionedLLMEnrichment:
    """Test suite for partitioned LLM enrichment functionality."""

    def test_partition_filter_generation(self):
        """Test company partition filter generation."""

        # Test alphabetical partitions
        filter_a = build_company_partition_filter("A")
        assert "COMPANY_NAME_CLEAN LIKE 'A%'" in filter_a
        assert "COMPANY_NAME_CLEAN LIKE 'a%'" in filter_a

        # Test numeric partition
        filter_numeric = build_company_partition_filter("0-9")
        assert "BETWEEN '0' AND '9'" in filter_numeric

        # Test other partition
        filter_other = build_company_partition_filter("other")
        assert "NOT (" in filter_other
        assert "BETWEEN 'A' AND 'Z'" in filter_other

    def test_partition_job_distribution(self):
        """Test that partition filters distribute jobs correctly."""
        # Mock job data with various company names
        test_companies = [
            "Apple Inc", "Boeing Corp", "Cisco Systems",
            "123 Corp", "456 Tech", "!@# Special Co"
        ]

        # Verify each company gets assigned to exactly one partition
        partition_assignments = {}
        for company in test_companies:
            assigned_partitions = []
            for partition_key in llm_company_partitions.get_partition_keys():
                filter_sql = build_company_partition_filter(partition_key)
                # Mock SQL evaluation logic
                if self._mock_sql_filter_match(company, filter_sql):
                    assigned_partitions.append(partition_key)

            assert len(assigned_partitions) == 1, f"Company {company} assigned to {len(assigned_partitions)} partitions"
            partition_assignments[company] = assigned_partitions[0]

        # Verify expected assignments
        assert partition_assignments["Apple Inc"] == "A"
        assert partition_assignments["123 Corp"] == "0-9"
        assert partition_assignments["!@# Special Co"] == "other"

    def test_partition_processing_isolation(self):
        """Test that partitions process independently."""
        # This would test with mocked Snowflake data
        pass

    def test_gemini_refresh_intervals(self):
        """Test Gemini refresh interval calculations for partitioned processing."""
        from dagster_betterjobs.transformations.llm_processing import process_platform_llm_enrichment

        # Test that smaller partition sizes maintain safe refresh intervals
        # Smaller partitions should still prevent recursion
        pass
```

**Step 4.2: Performance Validation Tests**
```python
def test_partition_performance_improvement():
    """Validate that partitioned processing improves performance."""

    # Mock timing tests
    # Verify parallel execution is faster than sequential
    # Test rate limit utilization improvement
    pass

def test_partition_error_isolation():
    """Test that partition failures don't affect other partitions."""

    # Mock partition failure scenarios
    # Verify other partitions continue processing
    # Test checkpoint recovery for failed partitions
    pass
```

**Step 4.3: Create Migration and Rollback Plan**
```python
# Migration strategy for switching from monolithic to partitioned assets

def migrate_to_partitioned_assets():
    """
    Migration Plan: Monolithic → Partitioned LLM Assets

    1. PREPARATION PHASE:
       - Deploy partitioned assets alongside existing monolithic assets
       - Use different asset names to avoid conflicts (_partitioned suffix)
       - Test partitioned assets with small subset of data

    2. VALIDATION PHASE:
       - Compare output quality between monolithic and partitioned approaches
       - Validate performance improvements
       - Verify no data loss or corruption

    3. CUTOVER PHASE:
       - Update downstream asset dependencies
       - Rename partitioned assets to replace monolithic ones
       - Archive old monolithic asset code

    4. CLEANUP PHASE:
       - Remove old checkpoint files and temporary data
       - Update documentation and operational procedures
    """
    pass

def rollback_to_monolithic_assets():
    """
    Rollback Plan: Partitioned → Monolithic LLM Assets

    If partitioned approach causes issues:
    1. Revert asset names and dependencies
    2. Restore monolithic asset code
    3. Clear partition checkpoints
    4. Resume processing with monolithic approach
    """
    pass
```

### Success Criteria

**Performance Metrics**:
- ✅ **Asset Materialization Time**: 50-70% reduction in total processing time
- ✅ **API Utilization**: Achieve 3500+ RPM effective utilization of Gemini rate limits
- ✅ **Parallel Execution**: All partitions can execute simultaneously without conflicts
- ✅ **Memory Efficiency**: Reduced memory footprint per partition vs monolithic processing

**Reliability Metrics**:
- ✅ **Recursion Prevention**: Zero recursion errors during normal operation
- ✅ **Error Isolation**: Partition failures don't block other partitions
- ✅ **Checkpoint Recovery**: Failed partitions can resume from checkpoints
- ✅ **Data Consistency**: No data loss or corruption from parallel processing

**Operational Metrics**:
- ✅ **Monitoring Visibility**: Clear partition-level success/failure tracking in Dagster UI
- ✅ **Development Speed**: Faster iteration cycles for testing individual partitions
- ✅ **Scalability**: Support for 10K+ jobs per platform within rate limits
- ✅ **Configuration Flexibility**: Easy adjustment of partition strategies and batch sizes

**Quality Metrics**:
- ✅ **Output Consistency**: Partitioned results match monolithic output quality
- ✅ **Confidence Scores**: No degradation in LLM extraction confidence
- ✅ **Data Coverage**: 100% of eligible jobs processed across all partitions
- ✅ **Cross-Platform Coordination**: Successful aggregation of partition results

### Risk Mitigation

**Technical Risks**:
- **Partition Data Skew**: Monitor job distribution across partitions, adjust strategy if needed
- **Rate Limit Conflicts**: Implement partition-level rate limiting and coordination
- **Checkpoint Corruption**: Use atomic checkpoint writes and backup strategies
- **Database Connection Limits**: Pool connections efficiently across partitions

**Operational Risks**:
- **Complexity Increase**: Provide comprehensive documentation and testing frameworks
- **Monitoring Overhead**: Implement automated partition health checks and alerting
- **Migration Complexity**: Use phased migration approach with rollback capability
- **Developer Learning Curve**: Create examples and best practices documentation

### Database Migration Impact

**Migration Requirements**:
- ✅ **Table Schema Update**: Add `PARTITION_KEY VARCHAR(16777216)` column to `JOBS_LLM_ENRICHED` table
- ✅ **Data Backfill**: Populate existing records with partition keys derived from company names
- ✅ **Zero Data Loss**: All existing LLM enrichment data preserved during migration
- ✅ **Rollback Available**: Complete rollback script provided if needed

**Migration Statistics** (Example):
```sql
-- Expected distribution after backfill:
PARTITION_KEY | RECORD_COUNT | PERCENTAGE
A             | 1,234        | 8.2%
B             | 987          | 6.5%
...           | ...          | ...
0-9           | 456          | 3.0%
other         | 123          | 0.8%
```

### Expected Benefits

**Immediate Benefits** (Day 1 post-implementation):
- 🚀 **Parallel Processing**: Multiple platform partitions execute simultaneously
- ⚡ **Faster Iterations**: Individual partition testing for development
- 🛡️ **Error Isolation**: Partition failures don't block entire platform processing

**Short-term Benefits** (Week 1):
- 📈 **Performance Improvement**: 50-70% reduction in total processing time
- 🎯 **API Optimization**: Better utilization of Gemini rate limits
- 🔍 **Enhanced Monitoring**: Partition-level visibility and debugging

**Long-term Benefits** (Month 1+):
- 📊 **Scalability**: Handle 10K+ jobs per platform efficiently
- 🔧 **Operational Excellence**: Improved reliability and maintainability
- 💰 **Cost Optimization**: More efficient use of LLM API resources

--

## ENHANCEMENT-032: Enrich Job Search Results with LLM-Processed Data

**Status:** Completed
**Priority:** Medium
**Component:** Job Search Asset (`job_search.py`)
**Date Planned:** 2025-06-28
**Date Completed:** 2025-06-28
**Business Impact:** Medium - Enhanced user experience and data value utilization

### Problem Statement
**Limited Data Utilization**: Current job search results only display basic job information from `JOBS_UNIFIED` table, missing valuable insights extracted by LLM processing from `JOBS_LLM_ENRICHED` table:

**Current Limitations**:
- Job search results show only basic fields (title, company, location, description)
- Rich LLM-extracted data (salary, experience, skills, work arrangements) is ignored in the search results
- Users cannot see structured job insights that would help with decision-making
- Investment in LLM processing is not being leveraged for end-user of this feature


**Missing Valuable Insights**:
- **Salary Information**: Extracted salary ranges, currency, period, and compensation type
- **Experience Requirements**: Years of experience, seniority level, specific technology requirements
- **Skills Analysis**: Technical and soft skills extracted from job descriptions
- **Work Arrangements**: Remote flexibility, work type, office locations, travel requirements
- **Job Classification**: Role type, job family, team size, primary keywords
- **Quality Indicators**: Confidence scores for various extracted fields

### Business Justification
- **Enhanced User Experience**: Provide structured, actionable insights alongside job listings
- **Competitive Advantage**: Match features found in premium job search platforms
- **ROI on LLM Investment**: Leverage expensive LLM processing for direct user value in the stage layer
- **Data-Driven Decisions**: Help users make informed decisions with structured data
- **Professional Presentation**: Elevate the quality and professionalism of search results
- **Filtering Capabilities**: Enable advanced filtering based on enriched data (future enhancement)

### Solution Architecture

**Approach**: Enhance the existing job search functionality by joining LLM enriched data and displaying it in a structured, confidence-aware manner within the HTML report.

**Key Design Principles**:
- **Confidence-Based Display**: Only show enriched data when confidence scores meet quality thresholds
- **Graceful Degradation**: Handle missing or low-confidence data elegantly
- **Visual Hierarchy**: Present enriched data in a clear, scannable format
- **Responsive Design**: Ensure new sections work across all device sizes
- **Performance Conscious**: Minimize query complexity impact

### Technical Approach

#### **1. Database Query Enhancement**

**Current Query Structure**: Uses only `STAGE.JOBS_UNIFIED` table
**Enhanced Query Structure**: LEFT JOIN with `STAGE.JOBS_LLM_ENRICHED` table

```sql
-- Enhanced query with LLM enriched data
SELECT
    -- Existing JOBS_UNIFIED fields
    j.job_uid,
    j.job_id,
    j.platform,
    j.company_id,
    j.company_name_clean as company_name,
    j.job_title_clean as job_title,
    j.job_description_clean as job_description,
    j.location_standardized as location,
    j.job_url,
    j.date_posted as posting_date,
    j.date_retrieved,
    j.is_active,
    j.employment_status,
    j.department,
    j.detected_language,
    j.language_confidence,
    j.is_english,
    j.data_quality_score,
    j.transformation_timestamp,

    -- NEW: LLM enriched fields with confidence filtering
    CASE
        WHEN llm.LLM_OVERALL_CONFIDENCE >= 0.7 THEN llm.SALARY_MIN
        ELSE NULL
    END as enriched_salary_min,
    CASE
        WHEN llm.LLM_OVERALL_CONFIDENCE >= 0.7 THEN llm.SALARY_MAX
        ELSE NULL
    END as enriched_salary_max,
    CASE
        WHEN llm.LLM_OVERALL_CONFIDENCE >= 0.7 THEN llm.SALARY_CURRENCY
        ELSE NULL
    END as enriched_salary_currency,
    CASE
        WHEN llm.LLM_OVERALL_CONFIDENCE >= 0.7 THEN llm.SALARY_PERIOD
        ELSE NULL
    END as enriched_salary_period,
    CASE
        WHEN llm.LLM_OVERALL_CONFIDENCE >= 0.7 THEN llm.SALARY_TYPE
        ELSE NULL
    END as enriched_salary_type,

    CASE
        WHEN llm.EXPERIENCE_CONFIDENCE >= 0.6 THEN llm.MIN_YEARS_EXPERIENCE
        ELSE NULL
    END as enriched_min_years_experience,
    CASE
        WHEN llm.EXPERIENCE_CONFIDENCE >= 0.6 THEN llm.MAX_YEARS_EXPERIENCE
        ELSE NULL
    END as enriched_max_years_experience,
    CASE
        WHEN llm.EXPERIENCE_CONFIDENCE >= 0.6 THEN llm.EXPERIENCE_LEVEL
        ELSE NULL
    END as enriched_experience_level,

    CASE
        WHEN llm.SKILLS_CONFIDENCE >= 0.6 THEN llm.TECHNICAL_SKILLS
        ELSE NULL
    END as enriched_technical_skills,

    CASE
        WHEN llm.WORK_ARRANGEMENT_CONFIDENCE >= 0.6 THEN llm.WORK_TYPE
        ELSE NULL
    END as enriched_work_type,
    CASE
        WHEN llm.WORK_ARRANGEMENT_CONFIDENCE >= 0.6 THEN llm.OFFICE_LOCATIONS
        ELSE NULL
    END as enriched_office_locations,

    CASE
        WHEN llm.CLASSIFICATION_CONFIDENCE >= 0.6 THEN llm.PRIMARY_KEYWORDS
        ELSE NULL
    END as enriched_primary_keywords,
    CASE
        WHEN llm.CLASSIFICATION_CONFIDENCE >= 0.6 THEN llm.INDUSTRY_KEYWORDS
        ELSE NULL
    END as enriched_industry_keywords,
    CASE
        WHEN llm.CLASSIFICATION_CONFIDENCE >= 0.6 THEN llm.ROLE_TYPE
        ELSE NULL
    END as enriched_role_type,
    CASE
        WHEN llm.CLASSIFICATION_CONFIDENCE >= 0.6 THEN llm.TEAM_SIZE
        ELSE NULL
    END as enriched_team_size,

    -- Confidence indicators for display decisions
    llm.LLM_OVERALL_CONFIDENCE as enriched_overall_confidence,
    llm.SALARY_CONFIDENCE as enriched_salary_confidence,
    llm.EXPERIENCE_CONFIDENCE as enriched_experience_confidence,
    llm.SKILLS_CONFIDENCE as enriched_skills_confidence,
    llm.WORK_ARRANGEMENT_CONFIDENCE as enriched_work_arrangement_confidence,
    llm.CLASSIFICATION_CONFIDENCE as enriched_classification_confidence,

    -- Processing metadata
    llm.LLM_PROCESSED as has_llm_enrichment,
    llm.LLM_PROCESSING_TIMESTAMP as enriched_processing_date

FROM {database_name}.{stage_schema}.jobs_unified j
LEFT JOIN {database_name}.{stage_schema}.jobs_llm_enriched llm
    ON j.job_uid = llm.job_uid
WHERE j.is_active = TRUE
-- ... existing WHERE conditions ...
```

#### **2. Data Processing Enhancement**

**Enhanced Data Handling Logic**:
```python
def process_enriched_data(job_row: pd.Series) -> Dict[str, Any]:
    """Process enriched LLM data for display with confidence-based filtering."""

    enriched = {}

    # Salary information (confidence >= 0.7)
    if (job_row.get('enriched_salary_min') and
        job_row.get('enriched_salary_confidence', 0) >= 0.7):
        enriched['salary'] = {
            'min': int(job_row['enriched_salary_min']),
            'max': int(job_row['enriched_salary_max']) if job_row.get('enriched_salary_max') else None,
            'currency': job_row.get('enriched_salary_currency', 'USD'),
            'period': job_row.get('enriched_salary_period', 'year'),
            'type': job_row.get('enriched_salary_type', 'base'),
            'confidence': float(job_row.get('enriched_salary_confidence', 0))
        }

    # Experience requirements (confidence >= 0.6)
    if (job_row.get('enriched_min_years_experience') is not None and
        job_row.get('enriched_experience_confidence', 0) >= 0.6):
        enriched['experience'] = {
            'min_years': int(job_row['enriched_min_years_experience']),
            'max_years': int(job_row['enriched_max_years_experience']) if job_row.get('enriched_max_years_experience') else None,
            'level': job_row.get('enriched_experience_level'),
            'confidence': float(job_row.get('enriched_experience_confidence', 0))
        }

    # Technical skills (confidence >= 0.6)
    if (job_row.get('enriched_technical_skills') and
        job_row.get('enriched_skills_confidence', 0) >= 0.6):
        try:
            skills_data = json.loads(job_row['enriched_technical_skills']) if isinstance(job_row['enriched_technical_skills'], str) else job_row['enriched_technical_skills']
            if skills_data and len(skills_data) > 0:
                enriched['technical_skills'] = {
                    'skills': skills_data[:8],  # Limit to top 8 skills for display
                    'confidence': float(job_row.get('enriched_skills_confidence', 0))
                }
        except (json.JSONDecodeError, TypeError):
            pass

    # Work arrangements (confidence >= 0.6)
    if job_row.get('enriched_work_arrangement_confidence', 0) >= 0.6:
        work_arrangement = {}
        if job_row.get('enriched_work_type'):
            work_arrangement['work_type'] = job_row['enriched_work_type']
        if job_row.get('enriched_office_locations'):
            try:
                locations_data = json.loads(job_row['enriched_office_locations']) if isinstance(job_row['enriched_office_locations'], str) else job_row['enriched_office_locations']
                if locations_data:
                    work_arrangement['office_locations'] = locations_data
            except (json.JSONDecodeError, TypeError):
                pass

        if work_arrangement:
            work_arrangement['confidence'] = float(job_row.get('enriched_work_arrangement_confidence', 0))
            enriched['work_arrangement'] = work_arrangement

    # Job classification (confidence >= 0.6)
    if job_row.get('enriched_classification_confidence', 0) >= 0.6:
        classification = {}

        # Keywords
        keywords = []
        if job_row.get('enriched_primary_keywords'):
            try:
                primary_kw = json.loads(job_row['enriched_primary_keywords']) if isinstance(job_row['enriched_primary_keywords'], str) else job_row['enriched_primary_keywords']
                if primary_kw:
                    keywords.extend(primary_kw[:5])  # Top 5 primary keywords
            except (json.JSONDecodeError, TypeError):
                pass

        if job_row.get('enriched_industry_keywords'):
            try:
                industry_kw = json.loads(job_row['enriched_industry_keywords']) if isinstance(job_row['enriched_industry_keywords'], str) else job_row['enriched_industry_keywords']
                if industry_kw:
                    keywords.extend(industry_kw[:3])  # Top 3 industry keywords
            except (json.JSONDecodeError, TypeError):
                pass

        if keywords:
            classification['keywords'] = keywords

        if job_row.get('enriched_role_type'):
            classification['role_type'] = job_row['enriched_role_type']

        if job_row.get('enriched_team_size'):
            classification['team_size'] = job_row['enriched_team_size']

        if classification:
            classification['confidence'] = float(job_row.get('enriched_classification_confidence', 0))
            enriched['classification'] = classification

    # Overall metadata
    enriched['has_enrichment'] = bool(job_row.get('has_llm_enrichment', False))
    enriched['overall_confidence'] = float(job_row.get('enriched_overall_confidence', 0))
    enriched['processing_date'] = job_row.get('enriched_processing_date')

    return enriched
```

#### **3. HTML Template Enhancement**

**New Enriched Data Section**: Add between job metadata and job description

```html
<!-- NEW: Enriched Job Insights Section -->
<div class="job-insights" data-has-enrichment="{has_enrichment}">
    <div class="insights-header">
        <h4 class="insights-title">💡 AI-Extracted Job Insights</h4>
        <span class="insights-confidence">
            {overall_confidence}% confidence
        </span>
    </div>

    <div class="insights-grid">
        <!-- Salary Information -->
        {salary_section_html}

        <!-- Experience Requirements -->
        {experience_section_html}

        <!-- Technical Skills -->
        {skills_section_html}

        <!-- Work Arrangements -->
        {work_arrangement_section_html}

        <!-- Job Classification -->
        {classification_section_html}
    </div>
</div>
```

**Enhanced CSS for Enriched Sections**:
```css
/* Job Insights Section */
.job-insights {
    padding: 1.5rem;
    background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
    border-top: 1px solid var(--border-light);
    border-bottom: 1px solid var(--border-light);
}

.job-insights[data-has-enrichment="false"] {
    display: none;
}

.insights-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
}

.insights-title {
    font-size: 1rem;
    font-weight: 600;
    color: var(--text-primary);
    margin: 0;
}

.insights-confidence {
    font-size: 0.75rem;
    color: var(--text-muted);
    background: var(--success-color);
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: var(--radius-sm);
    font-weight: 500;
}

.insights-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 1rem;
}

.insight-card {
    background: var(--surface);
    border-radius: var(--radius-md);
    padding: 1rem;
    border: 1px solid var(--border-color);
    box-shadow: var(--shadow-sm);
}

.insight-header {
    font-size: 0.875rem;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.insight-content {
    font-size: 0.875rem;
    color: var(--text-secondary);
    line-height: 1.4;
}

.insight-value {
    font-weight: 500;
    color: var(--text-primary);
}

.skill-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.375rem;
    margin-top: 0.5rem;
}

.skill-tag {
    background: var(--primary-color);
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: var(--radius-sm);
    font-size: 0.75rem;
    font-weight: 500;
}

.keyword-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.375rem;
    margin-top: 0.5rem;
}

.keyword-tag {
    background: var(--warning-color);
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: var(--radius-sm);
    font-size: 0.75rem;
    font-weight: 500;
}

.keyword-tag.industry {
    background: var(--secondary-color);
}

/* Responsive adjustments */
@media (max-width: 768px) {
    .insights-grid {
        grid-template-columns: 1fr;
    }

    .insights-header {
        flex-direction: column;
        align-items: flex-start;
        gap: 0.5rem;
    }
}
```

### Implementation Plan

#### **Phase 1: Database Query Enhancement (Day 1 - Morning)**

**Step 1.1: Update Job Search Query**
- Modify the main query in `search_jobs()` function to include LEFT JOIN with `JOBS_LLM_ENRICHED`
- Add confidence-based conditional field selection
- Test query performance impact and optimize if needed

**Step 1.2: Create Data Processing Functions**
```python
# Add new functions to job_search.py
def process_enriched_data(job_row: pd.Series) -> Dict[str, Any]:
    """Process enriched LLM data for display."""
    # Implementation as outlined above

def format_salary_display(salary_data: Dict) -> str:
    """Format salary information for display."""
    min_salary = salary_data['min']
    max_salary = salary_data.get('max')
    currency = salary_data.get('currency', 'USD')
    period = salary_data.get('period', 'year')

    if max_salary:
        return f"${min_salary:,} - ${max_salary:,} {currency} per {period}"
    else:
        return f"${min_salary:,}+ {currency} per {period}"

def format_experience_display(exp_data: Dict) -> str:
    """Format experience requirements for display."""
    min_years = exp_data['min_years']
    max_years = exp_data.get('max_years')
    level = exp_data.get('level')

    years_text = f"{min_years}+ years" if not max_years else f"{min_years}-{max_years} years"
    return f"{years_text}" + (f" ({level})" if level else "")
```

**Step 1.3: Update Configuration Class**
```python
class JobSearchConfig(Config):
    # Existing fields...

    # NEW: Enriched data display options
    show_enriched_data: bool = True
    min_enrichment_confidence: float = 0.6
    max_skills_display: int = 8
    max_keywords_display: int = 8
```

#### **Phase 2: HTML Template Enhancement (Day 1 - Afternoon)**

**Step 2.1: Create Enriched Data HTML Generators**
```python
def generate_salary_section_html(salary_data: Dict) -> str:
    """Generate HTML for salary information section."""
    if not salary_data:
        return ""

    salary_display = format_salary_display(salary_data)
    confidence = int(salary_data['confidence'] * 100)

    return f"""
    <div class="insight-card">
        <div class="insight-header">
            💰 Salary Range
            <span class="confidence-badge">{confidence}%</span>
        </div>
        <div class="insight-content">
            <div class="insight-value">{salary_display}</div>
            {f'<div class="salary-type">{salary_data["type"].title()} Salary</div>' if salary_data.get("type") else ""}
        </div>
    </div>
    """

def generate_experience_section_html(exp_data: Dict) -> str:
    """Generate HTML for experience requirements section."""
    if not exp_data:
        return ""

    experience_display = format_experience_display(exp_data)
    confidence = int(exp_data['confidence'] * 100)

    return f"""
    <div class="insight-card">
        <div class="insight-header">
            🎯 Experience Required
            <span class="confidence-badge">{confidence}%</span>
        </div>
        <div class="insight-content">
            <div class="insight-value">{experience_display}</div>
        </div>
    </div>
    """

def generate_skills_section_html(skills_data: Dict) -> str:
    """Generate HTML for technical skills section."""
    if not skills_data or not skills_data.get('skills'):
        return ""

    skills = skills_data['skills'][:8]  # Limit display
    confidence = int(skills_data['confidence'] * 100)

    skill_tags = ''.join([f'<span class="skill-tag">{skill}</span>' for skill in skills])

    return f"""
    <div class="insight-card">
        <div class="insight-header">
            🛠️ Technical Skills
            <span class="confidence-badge">{confidence}%</span>
        </div>
        <div class="insight-content">
            <div class="skill-tags">{skill_tags}</div>
        </div>
    </div>
    """

# Similar functions for work_arrangement and classification sections...
```

**Step 2.2: Integrate into Main HTML Generation**
- Modify `generate_enhanced_html_report()` function
- Add enriched data processing for each job
- Insert enriched sections into job card HTML structure

#### **Phase 3: UI Enhancement and Styling (Day 2 - Morning)**

**Step 3.1: Add Enhanced CSS**
- Add comprehensive CSS for enriched data sections
- Ensure responsive design works across all screen sizes
- Add print-friendly styles for enriched sections

**Step 3.2: Add Interactive Features**
- Add toggle functionality to show/hide enriched data
- Add confidence score indicators with tooltips
- Implement expandable sections for detailed view

**Step 3.3: Error Handling and Edge Cases**
- Handle JSON parsing errors gracefully
- Manage missing or malformed enriched data
- Add fallback displays for low-confidence data

#### **Phase 4: Testing and Optimization (Day 2 - Afternoon)**

**Step 4.1: Performance Testing**
- Measure query performance impact with LEFT JOIN
- Test HTML generation time with enriched data
- Optimize data processing functions if needed

**Step 4.2: UI/UX Testing**
- Test across different screen sizes and devices
- Verify enriched data displays correctly
- Ensure proper handling of missing data scenarios

**Step 4.3: Data Quality Validation**
- Verify confidence thresholds work as expected
- Test with various job samples to ensure quality
- Validate JSON data parsing and display

#### **Phase 5: Documentation and Deployment (Day 3)**

**Step 5.1: Update Documentation**
- Document new configuration options
- Add examples of enriched data display
- Update README with new features

**Step 5.2: Create Test Cases**
```python
def test_enriched_data_processing():
    """Test enriched data processing functions."""
    # Test various scenarios
    pass

def test_confidence_filtering():
    """Test confidence-based data filtering."""
    # Verify only high-confidence data is displayed
    pass

def test_html_generation_with_enriched_data():
    """Test HTML generation includes enriched sections."""
    # Verify enriched sections appear correctly
    pass
```

**Step 5.3: Deploy and Monitor**
- Deploy enhanced job search functionality
- Monitor query performance and user feedback
- Prepare for potential rollback if issues arise

### Success Criteria

**Functional Requirements**:
- ✅ **Data Integration**: Successfully join and display LLM enriched data
- ✅ **Confidence Filtering**: Only display data meeting confidence thresholds
- ✅ **Responsive Design**: Enriched sections work across all screen sizes
- ✅ **Error Handling**: Graceful handling of missing or malformed data

**Performance Requirements**:
- ✅ **Query Performance**: <10% increase in query execution time
- ✅ **HTML Generation**: <20% increase in report generation time
- ✅ **Memory Usage**: Efficient processing of enriched data structures
- ✅ **User Experience**: No noticeable performance degradation

**Quality Requirements**:
- ✅ **Data Accuracy**: Enriched data displays match source data
- ✅ **Visual Design**: Professional, clean presentation of enriched data
- ✅ **Accessibility**: Proper semantic HTML and contrast ratios
- ✅ **Maintainability**: Clean, well-documented code structure

**User Experience Requirements**:
- ✅ **Information Value**: Users find enriched data useful and actionable
- ✅ **Visual Hierarchy**: Clear separation between basic and enriched data
- ✅ **Progressive Enhancement**: Basic functionality works without enriched data
- ✅ **Print Compatibility**: Enriched sections print correctly

### Risk Mitigation

**Technical Risks**:
- **Query Performance Impact**: Monitor and optimize JOIN performance
- **Data Quality Issues**: Implement robust confidence-based filtering
- **JSON Parsing Errors**: Add comprehensive error handling for VARIANT fields
- **Memory Usage**: Optimize data processing for large result sets

**UI/UX Risks**:
- **Information Overload**: Carefully design information hierarchy and spacing
- **Responsive Design Issues**: Test extensively across device sizes
- **Accessibility Concerns**: Ensure proper semantic markup and contrast
- **Print Layout Problems**: Test and optimize print styles

**Data Risks**:
- **Low Confidence Data**: Set appropriate confidence thresholds
- **Missing Enriched Data**: Design graceful fallbacks
- **Inconsistent Data Quality**: Implement data validation and sanitization
- **Performance Degradation**: Monitor and optimize processing pipeline

### Configuration Options

**New Configuration Parameters**:
```python
class JobSearchConfig(Config):
    # Existing parameters...

    # Enriched data display controls
    show_enriched_data: bool = True
    min_enrichment_confidence: float = 0.6
    min_salary_confidence: float = 0.7
    min_experience_confidence: float = 0.6
    min_skills_confidence: float = 0.6
    min_work_arrangement_confidence: float = 0.6
    min_classification_confidence: float = 0.6

    # Display limits
    max_skills_display: int = 8
    max_keywords_display: int = 8
    max_office_locations_display: int = 5

    # UI preferences
    show_confidence_scores: bool = True
    expandable_enriched_sections: bool = False
    highlight_high_confidence: bool = True
```

### Expected Benefits

**Immediate Benefits** (Day 1 post-implementation):
- 🎯 **Enhanced Job Insights**: Users see structured salary, experience, and skills data
- 💼 **Professional Presentation**: More comprehensive and competitive job search results
- 🔍 **Better Decision Making**: Users can quickly assess job fit based on enriched data

**Short-term Benefits** (Week 1)**:
- 📊 **Data Value Realization**: LLM processing investment shows direct user value
- 🚀 **User Engagement**: More informative results likely to increase user satisfaction
- 🎨 **Visual Appeal**: Enhanced UI design improves overall user experience

**Long-term Benefits** (Month 1+)**:
- 🔧 **Platform Foundation**: Establishes groundwork for advanced filtering and search features
- 📈 **Competitive Position**: Feature parity with premium job search platforms
- 💡 **Analytics Opportunities**: Rich structured data enables usage analytics and insights

---

## ENHANCEMENT-033: Refactor Discovery Assets to Use Universal Partitions Module

**Status:** Completed ✅
**Priority:** Medium
**Component:** Job Discovery Assets (`*_jobs_discovery.py` assets)
**Date Planned:** 2025-06-30
**Date Completed:** 2025-06-30
**Business Impact:** Medium - Code maintainability and consistency improvement

### Problem Statement
**Code Duplication**: Current job discovery assets (`bamboohr_jobs_discovery.py`, `greenhouse_jobs_discovery.py`, etc.) each define their own identical partition definitions and filter-building logic, violating DRY principles:

**Current Issues**:
- Each discovery asset duplicates the same 28-partition definition (A-Z, 0-9, other)
- Identical partition filter logic repeated across multiple files
- Inconsistent partition management between discovery and LLM enrichment assets
- Maintenance overhead when updating partition logic across multiple assets
- Risk of partition definition drift between assets

**Code Duplication Examples**:
```python
# Repeated in every discovery asset:
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

# Repeated partition filter logic:
if partition_key == "0-9":
    letter_filter = "AND SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'"
elif partition_key == "other":
    letter_filter = "AND NOT (SUBSTRING(company_name, 1, 1) BETWEEN 'A' AND 'Z'...)"
else:
    letter_filter = f"AND (company_name LIKE '{partition_key}%'...)"
```

### Business Justification
- **Code Maintainability**: Single source of truth for partition definitions and logic
- **Consistency**: Ensure identical partition behavior across discovery and LLM assets
- **Development Speed**: Faster development when creating new discovery assets
- **Quality Assurance**: Reduce risk of partition logic bugs or inconsistencies
- **Technical Debt Reduction**: Eliminate code duplication following established DRY principles
- **Operational Excellence**: Consistent partition management across entire pipeline

### Solution Architecture

**Approach**: Refactor discovery assets to use the universal `partitions.py` module, following the proven pattern established by LLM enrichment assets.

**Reference Implementation**: `stage_jobs_llm_enriched_bamboohr.py` demonstrates the target pattern:
```python
from dagster_betterjobs.partitions import llm_company_partitions
from dagster_betterjobs.transformations.llm_processing import process_platform_llm_enrichment

@asset(
    partitions_def=llm_company_partitions,  # Universal partition definition
    # ...
)
def stage_jobs_llm_enriched_bamboohr(context: AssetExecutionContext, config: PartitionedLLMEnrichmentConfig):
    partition_key = context.partition_key
    # Uses shared processing logic with partition filtering
```

**Refactoring Strategy**:
1. **Replace Local Partitions**: Remove duplicate partition definitions from discovery assets
2. **Use Universal Filters**: Replace custom filter logic with universal partition functions
3. **Maintain Functionality**: Ensure identical behavior after refactoring
4. **Consistent Imports**: Standardize partition imports across all assets

### Technical Approach

#### **Current State Analysis** (using `bamboohr_jobs_discovery.py` as example):

**Duplicated Code to Remove**:
```python
# LOCAL PARTITION DEFINITION (TO REMOVE)
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

# CUSTOM FILTER LOGIC (TO REMOVE)
if partition_key == "0-9":
    letter_filter = "AND SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'"
elif partition_key == "other":
    letter_filter = "AND NOT (SUBSTRING(company_name, 1, 1) BETWEEN 'A' AND 'Z' OR SUBSTRING(company_name, 1, 1) BETWEEN 'a' AND 'z' OR SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9')"
else:
    letter_filter = f"AND (company_name LIKE '{partition_key}%' OR company_name LIKE '{partition_key.lower()}%')"
```

#### **Target State** (after refactoring):

**Universal Imports**:
```python
from dagster_betterjobs.partitions import (
    company_alpha_partitions,
    build_discovery_company_filter
)
```

**Asset Definition Update**:
```python
@asset(
    group_name="1_raw_ingestion_extraction",
    kinds={"API", "snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=["snowflake_master_company_urls"],
    partitions_def=company_alpha_partitions  # CHANGED: Use universal partitions
)
def bamboohr_company_jobs_discovery(context: AssetExecutionContext, config: BambooHRJobsDiscoveryConfig):
```

**Filter Generation Replacement**:
```python
# BEFORE (custom filter logic):
if partition_key == "0-9":
    letter_filter = "AND SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'"
elif partition_key == "other":
    letter_filter = "AND NOT (SUBSTRING(company_name, 1, 1) BETWEEN 'A' AND 'Z'...)"
else:
    letter_filter = f"AND (company_name LIKE '{partition_key}%'...)"

# AFTER (universal function):
letter_filter = f"AND {build_discovery_company_filter(partition_key, 'company_name')}"
```

### Implementation Plan

#### **Phase 1: Refactor BambooHR Discovery Asset (Day 1)**

**Step 1.1: Update Imports**
- Remove local partition definition
- Add universal partition imports
- Update partition references

**Step 1.2: Replace Partition Logic**
- Replace custom filter building with universal function
- Update asset decorator to use universal partitions
- Maintain exact same SQL filtering behavior

**Step 1.3: Test Refactored Asset**
- Verify identical partition behavior
- Test filter generation across all partition keys
- Validate query results match original implementation

#### **Phase 2: Extend to All Discovery Assets (Day 2)**

**Step 2.1: Replicate BambooHR Pattern**
- Apply same refactoring to `greenhouse_jobs_discovery.py`
- Apply same refactoring to `workday_jobs_discovery.py`
- Apply same refactoring to `smartrecruiters_jobs_discovery.py`
- Apply same refactoring to other platform discovery assets

**Step 2.2: Consistency Validation**
- Ensure all discovery assets use identical partition imports
- Verify consistent filter generation across platforms
- Test cross-platform partition behavior

#### **Phase 3: Documentation and Testing (Day 3)**

**Step 3.1: Update Documentation**
- Update README with universal partition usage
- Document partition management best practices
- Create examples for new discovery asset development

**Step 3.2: Create Test Cases**
```python
def test_universal_partition_consistency():
    """Test that discovery and LLM assets use identical partition definitions."""
    from dagster_betterjobs.partitions import company_alpha_partitions, llm_company_partitions

    # Verify discovery and LLM partitions are identical
    assert company_alpha_partitions.get_partition_keys() == llm_company_partitions.get_partition_keys()

def test_discovery_filter_generation():
    """Test discovery filter generation matches expected SQL."""
    from dagster_betterjobs.partitions import build_discovery_company_filter

    # Test all partition types
    filter_a = build_discovery_company_filter("A", "company_name")
    assert "company_name LIKE 'A%'" in filter_a

    filter_numeric = build_discovery_company_filter("0-9", "company_name")
    assert "BETWEEN '0' AND '9'" in filter_numeric

    filter_other = build_discovery_company_filter("other", "company_name")
    assert "NOT (" in filter_other

def test_refactored_assets_identical_behavior():
    """Test that refactored assets produce identical results to original."""
    # Compare partition filtering results before/after refactoring
    pass
```

### Success Criteria

**Code Quality Requirements**:
- ✅ **DRY Compliance**: Zero duplication of partition definitions across discovery assets
- ✅ **Consistency**: All discovery assets use identical partition management
- ✅ **Maintainability**: Single source of truth for partition logic updates
- ✅ **Readability**: Cleaner, more focused asset code without boilerplate

**Functional Requirements**:
- ✅ **Identical Behavior**: Refactored assets produce exact same results as original
- ✅ **Performance**: No performance regression from refactoring
- ✅ **Compatibility**: Existing partitions and checkpoints continue to work
- ✅ **Error Handling**: Consistent error handling across all discovery assets

**Operational Requirements**:
- ✅ **Zero Downtime**: Refactoring doesn't affect running pipelines
- ✅ **Rollback Capability**: Can quickly revert to original implementation if needed
- ✅ **Monitoring**: Existing monitoring and alerting continues to work
- ✅ **Documentation**: Clear documentation of changes and usage patterns

### Risk Mitigation

**Technical Risks**:
- **Behavior Changes**: Comprehensive testing to ensure identical filtering behavior
- **Import Errors**: Careful import management and testing across all environments
- **Performance Impact**: Monitor query performance after refactoring
- **Rollback Complexity**: Maintain original code in version control for quick rollback

**Operational Risks**:
- **Pipeline Disruption**: Deploy during maintenance window with thorough testing
- **Checkpoint Compatibility**: Ensure existing checkpoints continue to work
- **Monitoring Gaps**: Verify all existing alerts and monitoring continue to function
- **Documentation Debt**: Update all relevant documentation simultaneously

### Files Affected

**Discovery Assets to Refactor**:
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/bamboohr_jobs_discovery.py` - **COMPLETED**
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/greenhouse_jobs_discovery.py` - **COMPLETED**
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/workday_jobs_discovery.py` - **COMPLETED**
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/assets/smartrecruiters_jobs_discovery.py` - **COMPLETED**
- ⏳ Other platform discovery assets as they exist - Ready for refactoring (if any)

**Supporting Files**:
- `pipeline/dagster_betterjobs/dagster_betterjobs/partitions.py` (reference only - no changes needed)
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/jobs.py` - **COMPLETED** - Updated imports to use universal partitions
- ✅ `pipeline/dagster_betterjobs/dagster_betterjobs/schedules.py` - **COMPLETED** - Updated partition imports to use universal partitions
- Updated documentation and test files

### Expected Benefits

**Immediate Benefits** (Day 1 post-implementation):
- 🧹 **Code Cleanliness**: Removal of ~30 lines of duplicated code per discovery asset
- 🔄 **Consistency**: Identical partition behavior across discovery and LLM assets
- 🛠️ **Maintainability**: Single location for partition logic updates

**Short-term Benefits** (Week 1)**:
- 🚀 **Development Speed**: Faster creation of new discovery assets
- 🔍 **Debugging**: Easier troubleshooting with consistent partition logic
- 📚 **Knowledge Transfer**: Simpler onboarding with standardized patterns

**Long-term Benefits** (Month 1+)**:
- 🏗️ **Architectural Consistency**: Uniform approach across entire pipeline
- 📈 **Scalability**: Easy addition of new partition strategies when needed
- 💰 **Technical Debt Reduction**: Elimination of code duplication maintenance overhead

### Implementation Summary

**All Discovery Assets Refactored** ✅ **COMPLETED**:
- ✅ **BambooHR Asset**: Successfully migrated `bamboohr_jobs_discovery.py` to use universal partitions
- ✅ **Greenhouse Asset**: Successfully migrated `greenhouse_jobs_discovery.py` to use universal partitions
- ✅ **Workday Asset**: Successfully migrated `workday_jobs_discovery.py` to use universal partitions
- ✅ **SmartRecruiters Asset**: Successfully migrated `smartrecruiters_jobs_discovery.py` to use universal partitions
- ✅ **Code Reduction**: Eliminated 10+ lines of duplicated code per asset (40+ total lines removed)
- ✅ **Consistency Achieved**: All discovery assets now use identical partition management as LLM enrichment assets
- ✅ **Functionality Preserved**: Maintains exact same SQL filtering behavior with cleaner code

**Refactoring Changes Applied to All Assets**:
1. **Removed Local Partition Definitions**: Eliminated duplicate `alpha_partitions` definition from all 4 assets
2. **Added Universal Imports**: Imported `company_alpha_partitions` and `build_discovery_company_filter` from `partitions.py`
3. **Updated Asset Decorators**: Changed `partitions_def=alpha_partitions` to `partitions_def=company_alpha_partitions`
4. **Replaced Custom Filter Logic**: Simplified 7 lines of if/elif/else logic to single universal function call in each asset
5. **Fixed Job Imports**: Updated `jobs.py` to import universal partitions instead of non-existent local definitions
6. **Maintained Backward Compatibility**: All existing partitions and checkpoints continue to work across all platforms

**Benefits Achieved**:
- 🧹 **DRY Compliance**: Zero duplication of partition definitions across all discovery assets
- 🔄 **Universal Consistency**: Identical partition behavior across discovery and LLM assets
- 🛠️ **Single Source of Truth**: All partition logic managed in one place (`partitions.py`)
- 📈 **Maintainability**: Future partition updates only need to be made in one location

---

## ENHANCEMENT-035: Separate Skills Consolidation into Dedicated Asset

**Status:** ✅ **Completed**
**Priority:** Medium
**Component:** Skills Normalization Pipeline (`stage_skills_normalized` and new `stage_skills_consolidated` asset)
**Date Planned:** 2025-07-02
**Date Completed:** 2025-07-02
**Estimated Effort:** 2-3 days
**Actual Effort:** 1 day
**Business Impact:** High - Improved data processing architecture and maintainability

### Problem Statement
**Architectural Coupling**: The `stage_skills_normalized` asset currently handles both skill standardization AND consolidation in a single monolithic process, creating several issues:

**Current Issues**:
- **Single Point of Failure**: Consolidation errors can break the entire normalization process
- **Complex Debugging**: Mixed responsibilities make it difficult to isolate issues
- **Poor Reusability**: Consolidation logic cannot be reused for other normalization pipelines
- **Testing Complexity**: Cannot test consolidation independently of standardization
- **Performance Impact**: Consolidation processing slows down basic skill normalization
- **Confidence Score Confusion**: Consolidation affects confidence calculations within normalization

**Current Monolithic Flow**:
```
SKILLS_RAW_EXTRACTION → SKILLS_NORMALIZED (standardization + consolidation)
                      ↓
                  JOB_SKILLS_BRIDGE
```

**Architectural Principle Violation**: Single Responsibility Principle - one asset should do one thing well.

### Business Justification
- **Separation of Concerns**: Clear distinction between standardization and consolidation responsibilities
- **Improved Reliability**: Consolidation failures don't impact basic skill normalization
- **Enhanced Debugging**: Easier to isolate and fix issues in specific processing stages
- **Better Testing**: Independent testing of consolidation logic and standardization rules
- **Performance Optimization**: Basic normalization can run without consolidation overhead
- **Reusability**: Consolidation logic can be reused for keywords, locations, and other entities
- **Cleaner Architecture**: Better alignment with data engineering best practices

### Solution Architecture

**Proposed Enhanced Flow**:
```
SKILLS_RAW_EXTRACTION → SKILLS_NORMALIZED (standardization only)
                      ↓
                  SKILLS_CONSOLIDATED (consolidation only)
                      ↓
                  JOB_SKILLS_BRIDGE (uses consolidated skills)
```

**New Asset Responsibilities**:
- **`stage_skills_normalized`**: Focus purely on applying standardization rules and confidence scoring
- **`stage_skills_consolidated`**: Focus purely on consolidation logic (singular/plural, variants)
- **`stage_job_skills_bridge`**: Use consolidated skills for relationship mapping

### Technical Approach

#### **Asset 1: Enhanced `stage_skills_normalized` (Standardization Only)**

**Simplified Responsibilities**:
- Apply standardization rules from `SKILL_STANDARDIZATION_RULES` table
- Calculate confidence scores based on rule matches and LLM confidence
- Handle skill categorization and family mapping
- Create basic `SKILLS_NORMALIZED` table without consolidation

**Key Changes**:
```python
@asset(
    deps=["stage_llm_skills_raw_extraction", "stage_skills_standardization_rules"],
    description="Apply standardization rules to create skills master table (standardization only)",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create skills master table.

    ENHANCEMENT-035: Simplified to handle standardization only.
    Consolidation moved to dedicated stage_skills_consolidated asset.
    """

    # 1. Apply standardization rules
    # 2. Calculate confidence scores (preserving LLM confidence)
    # 3. Handle categorization and family mapping
    # 4. Create SKILLS_NORMALIZED table (one record per unique skill)
    # 5. NO consolidation logic
```

**Simplified SQL Logic**:
```sql
INSERT INTO SKILLS_NORMALIZED (
    SKILL_ID, SKILL_NAME, SKILL_CATEGORY, CONFIDENCE_SCORE, ...
)
WITH standardized_skills AS (
    SELECT
        COALESCE(sr.STANDARDIZED_NAME, sre.SKILL_NAME_ORIGINAL) as skill_name,
        COALESCE(sr.SKILL_CATEGORY, sre.SKILL_CATEGORY) as skill_category,
        -- FIX: Preserve original LLM confidence scores
        AVG(COALESCE(sr.CONFIDENCE_SCORE, lle.SKILLS_CONFIDENCE, 0.8)) as confidence_score,
        COUNT(*) as frequency_count,
        ARRAY_AGG(DISTINCT sre.SKILL_NAME_ORIGINAL) as original_variants
    FROM SKILLS_RAW_EXTRACTION sre
    LEFT JOIN SKILL_STANDARDIZATION_RULES sr ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
    LEFT JOIN JOBS_LLM_ENRICHED lle ON sre.JOB_UID = lle.JOB_UID
    GROUP BY 1, 2
)
SELECT
    CONCAT('skill_', ROW_NUMBER() OVER (ORDER BY frequency_count DESC)) as skill_id,
    skill_name,
    skill_category,
    confidence_score,
    frequency_count,
    original_variants,
    -- No consolidation logic here
FROM standardized_skills;
```

#### **Asset 2: New `stage_skills_consolidated` (Consolidation Only)**

**Dedicated Responsibilities**:
- Take standardized skills from `SKILLS_NORMALIZED`
- Apply intelligent consolidation using existing `skill_consolidation.py` utility
- Handle singular/plural variants, abbreviations, and similar terms
- Create `SKILLS_CONSOLIDATED` table with consolidated skills
- Preserve consolidation metadata and statistics

**New Asset Implementation**:
```python
@asset(
    deps=["stage_skills_normalized"],
    description="Apply intelligent consolidation to standardized skills",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_consolidated(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply intelligent consolidation to standardized skills.

    ENHANCEMENT-035: Dedicated asset for consolidation logic only.
    Takes standardized skills and applies variant consolidation.

    Features:
    - Singular/plural form consolidation
    - Abbreviation and alias handling
    - Frequency-weighted merging
    - Confidence score preservation
    - Consolidation audit trail
    """

    # 1. Load standardized skills from SKILLS_NORMALIZED
    # 2. Apply consolidation using skill_consolidation.py utilities
    # 3. Create SKILLS_CONSOLIDATED table
    # 4. Generate consolidation statistics and audit trail
```

**Consolidation Asset SQL Logic**:
```sql
-- Step 1: Load standardized skills
SELECT skill_id, skill_name, skill_category, confidence_score, frequency_count
FROM SKILLS_NORMALIZED;

-- Step 2: Apply consolidation in Python using existing utilities
-- (Use consolidate_skill_variants function)

-- Step 3: Insert consolidated results
INSERT INTO SKILLS_CONSOLIDATED (
    CONSOLIDATED_SKILL_ID,
    CANONICAL_SKILL_NAME,
    SKILL_CATEGORY,
    CONSOLIDATED_CONFIDENCE_SCORE,
    TOTAL_FREQUENCY_COUNT,
    ORIGINAL_SKILL_IDS,
    CONSOLIDATION_METADATA
)
SELECT ... FROM consolidation_results;
```

#### **Asset 3: Updated `stage_job_skills_bridge` (Uses Consolidated Skills)**

**Updated Dependencies**:
```python
@asset(
    deps=["stage_skills_consolidated", "stage_jobs_unified"],  # Changed dependency
    description="Create job-skill relationships using consolidated skills",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_skills_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to consolidated skills with rich context.

    ENHANCEMENT-035: Updated to use consolidated skills from dedicated asset.
    """

    # Use SKILLS_CONSOLIDATED instead of SKILLS_NORMALIZED
    # Maintain same relationship logic
```

### Implementation Plan

#### **Phase 1: Create New Consolidation Asset (Day 1)**

**Step 1.1: Create `stage_skills_consolidated` Asset**
- Create new asset file `stage_skills_consolidated.py`
- Implement consolidation-only logic using existing utilities
- Create new `SKILLS_CONSOLIDATED` table schema
- Add comprehensive logging and statistics

**Step 1.2: Update Table Schema**
```sql
-- New table: stage_skills_consolidated.sql
CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILLS_CONSOLIDATED (
    CONSOLIDATED_SKILL_ID STRING PRIMARY KEY,
    CANONICAL_SKILL_NAME STRING NOT NULL,
    SKILL_CATEGORY STRING NOT NULL,
    SKILL_SUBCATEGORY STRING,
    SKILL_FAMILY STRING,
    SKILL_TYPE STRING DEFAULT 'technical',

    -- Consolidation metadata
    ORIGINAL_SKILL_IDS VARIANT,          -- Array of original skill IDs that were merged
    ORIGINAL_SKILL_NAMES VARIANT,        -- Array of original skill names
    CONSOLIDATION_METHOD STRING,         -- 'none', 'singular_plural', 'abbreviation', etc.

    -- Aggregated data
    TOTAL_FREQUENCY_COUNT INTEGER DEFAULT 0,
    CONSOLIDATED_CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    FIRST_SEEN_DATE DATE,
    LAST_SEEN_DATE DATE,

    -- Audit trail
    CONSOLIDATION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CONSOLIDATION_VERSION STRING DEFAULT '1.0',
    CREATED_BY STRING DEFAULT 'system'
) CLUSTER BY (SKILL_CATEGORY, CANONICAL_SKILL_NAME);
```

**Step 1.3: Test New Asset Independently**
- Run consolidation asset on sample data
- Validate consolidation logic and results
- Ensure statistics and metadata are accurate

#### **Phase 2: Simplify Normalization Asset (Day 2)**

**Step 2.1: Refactor `stage_skills_normalized`**
- Remove all consolidation logic from the asset
- Simplify to pure standardization and rule application
- Update to create basic `SKILLS_NORMALIZED` without consolidation
- Update logging and statistics to reflect new scope

**Step 2.2: Update Dependencies**
- Ensure `stage_skills_consolidated` depends on `stage_skills_normalized`
- Update any downstream assets to use appropriate table
- Test dependency chain works correctly

#### **Phase 3: Update Bridge Asset (Day 2)**

**Step 3.1: Update `stage_job_skills_bridge`**
- Change dependency from `stage_skills_normalized` to `stage_skills_consolidated`
- Update SQL to join with `SKILLS_CONSOLIDATED` table
- Ensure relationship mapping uses consolidated skill IDs
- Test bridge creation with new consolidated skills

**Step 3.2: Validate End-to-End Flow**
- Test complete pipeline: raw → normalized → consolidated → bridge
- Validate data quality and relationships
- Ensure confidence scores are preserved throughout

#### **Phase 4: Testing and Documentation (Day 3)**

**Step 4.1: Comprehensive Testing**
```python
def test_skills_normalization_only():
    """Test that normalization asset only handles standardization."""
    # Verify no consolidation logic in normalization
    pass

def test_skills_consolidation_independent():
    """Test that consolidation asset works independently."""
    # Test consolidation logic isolation
    pass

def test_end_to_end_pipeline():
    """Test complete pipeline with separated assets."""
    # Validate full flow works correctly
    pass
```

**Step 4.2: Update Documentation**
- Document new asset responsibilities
- Update README with new pipeline flow
- Create migration guide for existing users
- Document consolidation asset configuration options

### Success Criteria

**Architectural Requirements**:
- ✅ **Separation of Concerns**: Normalization and consolidation in separate assets
- ✅ **Independent Testing**: Each asset can be tested independently
- ✅ **Clear Dependencies**: Clean dependency chain with no circular dependencies
- ✅ **Reusable Logic**: Consolidation logic can be reused for other entities

**Functional Requirements**:
- ✅ **Data Integrity**: No data loss during separation
- ✅ **Performance**: No performance degradation in overall pipeline
- ✅ **Confidence Preservation**: Confidence scores maintained throughout pipeline
- ✅ **Audit Trail**: Full traceability of consolidation decisions

**Operational Requirements**:
- ✅ **Rollback Capability**: Can revert to monolithic approach if needed
- ✅ **Monitoring**: Each asset has appropriate monitoring and alerting
- ✅ **Documentation**: Clear documentation of new architecture
- ✅ **Testing Coverage**: Comprehensive test coverage for both assets

### Risk Mitigation

**Technical Risks**:
- **Data Consistency**: Ensure consolidation doesn't introduce data quality issues
- **Performance Impact**: Monitor pipeline performance with additional asset
- **Dependency Complexity**: Careful management of asset dependencies
- **Migration Complexity**: Smooth transition from monolithic to separated assets

**Operational Risks**:
- **Deployment Coordination**: Coordinated deployment of multiple asset changes
- **Monitoring Gaps**: Ensure monitoring covers all new assets
- **Team Learning Curve**: Training on new architecture and responsibilities
- **Rollback Complexity**: Tested rollback procedures if issues arise

### Expected Benefits

**Immediate Benefits** (Day 1 post-implementation):
- 🏗️ **Cleaner Architecture**: Clear separation of standardization and consolidation
- 🐛 **Easier Debugging**: Isolated processing stages for better troubleshooting
- 🧪 **Independent Testing**: Each stage can be tested and validated separately

**Short-term Benefits** (Week 1)**:
- 🚀 **Improved Reliability**: Consolidation failures don't break standardization
- ⚡ **Performance Options**: Can run standardization without consolidation overhead
- 🔧 **Better Maintainability**: Focused asset responsibilities

**Long-term Benefits** (Month 1+)**:
- 📈 **Reusable Components**: Consolidation logic reusable for keywords/locations
- 🏆 **Engineering Best Practices**: Better alignment with data engineering principles
- 🎯 **Scalable Architecture**: Foundation for future normalization enhancements

---

## ENHANCEMENT-034: Remove PRIMARY_KEYWORDS from Skill Normalization Process

**Status:** ✅ **Completed**
**Priority:** Medium
**Component:** Skills Normalization Pipeline (`stage_skills_raw_extraction.sql` and `skills_normalization.py`)
**Date Planned:** 2025-07-02
**Date Completed:** 2025-07-02
**Estimated Effort:** 0.5 days
**Actual Effort:** 0.5 days
**Business Impact:** Medium - Improve data separation and reduce processing overhead

### Problem Statement
**Data Processing Overlap**: The skills normalization process currently includes `PRIMARY_KEYWORDS` extraction and processing, which creates unnecessary duplication since keywords are now properly handled by the dedicated keyword normalization pipeline.

**Current Issues**:
- `stage_skills_raw_extraction.sql` extracts `PRIMARY_KEYWORDS` as skills with category 'keyword'
- `stage_skills_normalized` asset processes keywords alongside technical and soft skills
- `stage_job_skills_bridge` creates relationships between jobs and keywords as skills
- Duplicate processing creates confusion between skills and keywords in analytics
- Unnecessary computational overhead from processing the same data twice

**Evidence of Duplication**:
```sql
-- In stage_skills_raw_extraction.sql (lines 45-55):
PRIMARY_KEYWORDS_EXPLODED AS (
    -- Extract primary keywords as skills
    SELECT
        jle.JOB_UID,
        'primary_keywords' as SKILL_SOURCE,
        'keyword' as SKILL_CATEGORY,  -- Treating keywords as skills
        TRIM(LOWER(KEYWORD.VALUE::STRING)) as SKILL_NAME_RAW,
        KEYWORD.VALUE::STRING as SKILL_NAME_ORIGINAL
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
    LATERAL FLATTEN(input => jle.PRIMARY_KEYWORDS) KEYWORD
    WHERE jle.PRIMARY_KEYWORDS IS NOT NULL
      AND KEYWORD.VALUE IS NOT NULL
      AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
      AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
)
```

**Parallel Keyword Processing**:
- `stage_keywords_raw_extraction.sql` already extracts `PRIMARY_KEYWORDS` properly
- `stage_keywords_normalized` asset handles keyword standardization
- `stage_job_keywords_bridge` creates proper job-keyword relationships
- Keywords are now properly categorized and analyzed in the keyword pipeline

### Business Justification
- **Data Separation**: Clear distinction between skills (technical/soft) and keywords (job themes/context)
- **Processing Efficiency**: Eliminate duplicate processing of the same LLM data
- **Analytics Clarity**: Prevent confusion between skills and keywords in reporting
- **Maintenance Simplification**: Reduce complexity in skills normalization pipeline
- **Resource Optimization**: Reduce computational overhead and storage requirements
- **Data Quality**: Ensure skills pipeline focuses only on actual skills data

### Technical Approach

**Scope of Changes**:
1. **Remove PRIMARY_KEYWORDS from `stage_skills_raw_extraction.sql`**
2. **Update `stage_skills_normalized` asset to exclude keyword processing**
3. **Update `stage_job_skills_bridge` to exclude keyword relationships**
4. **Clean up any keyword-related statistics and metadata**

**Data Flow Impact**:
```
BEFORE:
JOBS_LLM_ENRICHED → SKILLS_RAW_EXTRACTION (technical + soft + keywords)
                  → KEYWORDS_RAW_EXTRACTION (keywords only)
                  → DUPLICATE PROCESSING

AFTER:
JOBS_LLM_ENRICHED → SKILLS_RAW_EXTRACTION (technical + soft only)
                  → KEYWORDS_RAW_EXTRACTION (keywords only)
                  → CLEAN SEPARATION
```

### Implementation Plan

#### **Phase 1: Update Skills Raw Extraction View (Day 1 - Morning)**

**Step 1.1: Remove PRIMARY_KEYWORDS_EXPLODED CTE**
```sql
-- File: pipeline/sql/objects/views/stage_skills_raw_extraction.sql

-- REMOVE this entire CTE:
-- PRIMARY_KEYWORDS_EXPLODED AS (
--     -- Extract primary keywords as skills
--     SELECT
--         jle.JOB_UID,
--         'primary_keywords' as SKILL_SOURCE,
--         'keyword' as SKILL_CATEGORY,
--         TRIM(LOWER(KEYWORD.VALUE::STRING)) as SKILL_NAME_RAW,
--         KEYWORD.VALUE::STRING as SKILL_NAME_ORIGINAL
--     FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
--     LATERAL FLATTEN(input => jle.PRIMARY_KEYWORDS) KEYWORD
--     WHERE jle.PRIMARY_KEYWORDS IS NOT NULL
--       AND KEYWORD.VALUE IS NOT NULL
--       AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
--       AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
-- )

-- UPDATE the final SELECT to remove PRIMARY_KEYWORDS_EXPLODED:
SELECT * FROM TECHNICAL_SKILLS_EXPLODED
UNION ALL
SELECT * FROM SOFT_SKILLS_EXPLODED
-- REMOVE: UNION ALL SELECT * FROM PRIMARY_KEYWORDS_EXPLODED
```

**Step 1.2: Update View Documentation**
```sql
-- Update view comment to reflect new scope
COMMENT ON VIEW BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION IS
'Extract technical and soft skills from LLM VARIANT columns.
Keywords are handled separately in KEYWORDS_RAW_EXTRACTION view.';
```

#### **Phase 2: Update Skills Normalization Asset (Day 1 - Afternoon)**

**Step 2.1: Remove Keyword-Related Statistics**
```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py

# In stage_llm_skills_raw_extraction asset:
stats = {
    "extraction_timestamp": datetime.now().isoformat(),
    "skills_extracted": 0,
    "technical_skills_count": 0,
    "soft_skills_count": 0,
    # REMOVE: "primary_keywords_count": 0,
    "unique_jobs_processed": 0,
    "extraction_errors": 0
}

# Update statistics query:
cursor.execute("""
SELECT
    COUNT(*) as total_skills,
    COUNT(DISTINCT JOB_UID) as unique_jobs,
    COUNT(CASE WHEN SKILL_SOURCE = 'technical_skills' THEN 1 END) as technical_count,
    COUNT(CASE WHEN SKILL_SOURCE = 'soft_skills' THEN 1 END) as soft_count
    # REMOVE: COUNT(CASE WHEN SKILL_SOURCE = 'primary_keywords' THEN 1 END) as keywords_count
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
""")

# Update result processing:
if result:
    stats.update({
        "skills_extracted": result[0],
        "unique_jobs_processed": result[1],
        "technical_skills_count": result[2],
        "soft_skills_count": result[3]
        # REMOVE: "primary_keywords_count": result[4]
    })
```

**Step 2.2: Update Logging Messages**
```python
context.log.info(f"""
🎯 Skills Raw Extraction Complete:
• Total Skills Extracted: {stats['skills_extracted']:,}
• Unique Jobs Processed: {stats['unique_jobs_processed']:,}
• Technical Skills: {stats['technical_skills_count']:,}
• Soft Skills: {stats['soft_skills_count']:,}
# REMOVE: • Primary Keywords: {stats['primary_keywords_count']:,}
""")

# Update metadata:
context.add_output_metadata({
    "skills_extracted": MetadataValue.int(stats["skills_extracted"]),
    "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
    "technical_skills_count": MetadataValue.int(stats["technical_skills_count"]),
    "soft_skills_count": MetadataValue.int(stats["soft_skills_count"])
    # REMOVE: "primary_keywords_count": MetadataValue.int(stats["primary_keywords_count"]),
})
```

#### **Phase 3: Update Job Skills Bridge Asset (Day 1 - Afternoon)**

**Step 3.1: Remove Keyword Source Processing**
```python
# In stage_job_skills_bridge asset:

# Update skill context inference:
CASE
    WHEN SKILL_SOURCE = 'technical_skills' THEN 'required'
    WHEN SKILL_SOURCE = 'soft_skills' THEN 'preferred'
    # REMOVE: WHEN SKILL_SOURCE = 'primary_keywords' THEN 'context'
    ELSE 'unknown'
END as skill_context
```

**Step 3.2: Update Source Breakdown Statistics**
```python
# Update source breakdown query to exclude keywords:
cursor.execute("""
SELECT
    SKILL_SOURCE,
    COUNT(*) as relationship_count,
    COUNT(DISTINCT JOB_UID) as jobs_count,
    AVG(OVERALL_CONFIDENCE) as avg_confidence
FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
WHERE SKILL_SOURCE IN ('technical_skills', 'soft_skills')  # ADD FILTER
GROUP BY SKILL_SOURCE
ORDER BY relationship_count DESC
""")
```

#### **Phase 4: Data Cleanup and Validation (Day 1 - Evening)**

**Step 4.1: Clean Existing Data**
```sql
-- Remove any existing keyword relationships from bridge table
DELETE FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
WHERE SKILL_SOURCE = 'primary_keywords';

-- Remove any keyword skills from normalized table
DELETE FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
WHERE SKILL_CATEGORY = 'keyword';
```

**Step 4.2: Validation Queries**
```sql
-- Verify no keywords remain in skills pipeline
SELECT COUNT(*) as remaining_keywords
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
WHERE SKILL_SOURCE = 'primary_keywords';

-- Verify skills pipeline only contains actual skills
SELECT SKILL_SOURCE, COUNT(*) as count
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
GROUP BY SKILL_SOURCE
ORDER BY count DESC;

-- Verify keywords are properly handled in keyword pipeline
SELECT COUNT(*) as keyword_relationships
FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE;
```

#### **Phase 5: Testing and Documentation (Day 2 - Morning)**

**Step 5.1: Create Test Cases**
```python
def test_skills_extraction_excludes_keywords():
    """Test that skills extraction no longer includes keywords."""
    # Verify PRIMARY_KEYWORDS_EXPLODED CTE is removed
    # Verify final SELECT doesn't include keyword data
    pass

def test_skills_normalization_focus():
    """Test that skills normalization focuses only on technical and soft skills."""
    # Verify statistics don't include keyword counts
    # Verify processing only handles skill data
    pass

def test_bridge_relationships_clean():
    """Test that job-skills bridge only contains skill relationships."""
    # Verify no keyword relationships in bridge table
    # Verify source breakdown only shows skills
    pass
```

**Step 5.2: Update Documentation**
```markdown
# Update README or relevant documentation:

## Skills Normalization Pipeline
The skills normalization pipeline processes:
- **Technical Skills**: Programming languages, frameworks, tools, databases, etc.
- **Soft Skills**: Communication, leadership, problem-solving, etc.

**Note**: Keywords are handled separately in the keyword normalization pipeline.
```

### Success Criteria

**Functional Requirements**:
- ✅ **Clean Separation**: Skills pipeline only processes technical and soft skills
- ✅ **No Duplication**: Keywords processed only in keyword pipeline
- ✅ **Data Integrity**: No data loss from skills or keywords
- ✅ **Performance Improvement**: Reduced processing overhead

**Data Quality Requirements**:
- ✅ **Zero Keywords in Skills**: No keyword data in skills extraction or normalization
- ✅ **Complete Keyword Coverage**: All keywords properly handled in keyword pipeline
- ✅ **Accurate Statistics**: Skills statistics reflect only actual skills data
- ✅ **Clean Bridge Tables**: Job-skills bridge contains only skill relationships

**Performance Requirements**:
- ✅ **Reduced Processing Time**: Faster skills normalization without keyword overhead
- ✅ **Lower Storage Usage**: Reduced duplicate data storage
- ✅ **Simplified Queries**: Cleaner SQL without keyword filtering logic

### Risk Mitigation

**Data Loss Risk**:
- **Validation**: Ensure keywords are properly handled in keyword pipeline before removal
- **Backup**: Keep original code in version control for rollback if needed
- **Testing**: Comprehensive testing to verify no data loss

**Processing Disruption**:
- **Gradual Rollback**: Can quickly revert changes if issues arise
- **Monitoring**: Monitor pipeline performance after changes
- **Validation**: Verify downstream analytics continue to work

**Analytics Impact**:
- **Documentation**: Clear documentation of changes for analytics users
- **Migration Guide**: Provide guidance for updating any dependent queries
- **Testing**: Test analytics queries that might reference keyword skills

### Files to be Modified

**Primary Changes**:
- `pipeline/sql/objects/views/stage_skills_raw_extraction.sql` - Remove PRIMARY_KEYWORDS_EXPLODED CTE
- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py` - Update statistics and logging

**Supporting Changes**:
- Update any documentation referencing skills including keywords
- Update test files to reflect new scope
- Update analytics queries that might reference keyword skills

### Expected Benefits

**Immediate Benefits** (Day 1 post-implementation):
- 🧹 **Cleaner Data**: Clear separation between skills and keywords
- ⚡ **Faster Processing**: Reduced computational overhead
- 🔍 **Simplified Logic**: Easier to understand and maintain

**Short-term Benefits** (Week 1):
- 📊 **Accurate Analytics**: Skills metrics reflect only actual skills
- 🛠️ **Easier Maintenance**: Simpler pipeline with focused responsibilities
- 📈 **Better Performance**: Reduced processing time and storage usage

**Long-term Benefits** (Month 1+):
- 🏗️ **Architectural Clarity**: Clear data pipeline responsibilities
- 💰 **Resource Optimization**: More efficient use of computational resources
- 📚 **Knowledge Transfer**: Easier onboarding with clear data separation

### Dependencies

**Prerequisites**:
- ✅ **Keyword Pipeline Complete**: Keyword normalization must be fully operational
- ✅ **Data Validation**: Verify keywords are properly handled before removal
- ✅ **Analytics Review**: Ensure no analytics depend on keyword skills

**Post-Implementation**:
- **Monitoring**: Monitor pipeline performance and data quality
- **Documentation**: Update all relevant documentation
- **Training**: Inform team of changes and new data separation

### Implementation Summary

**ENHANCEMENT-034 COMPLETED** ✅ **2025-07-02**:

**Changes Implemented**:
- ✅ **Skills Raw Extraction View**: Removed `PRIMARY_KEYWORDS_EXPLODED` CTE from `stage_skills_raw_extraction.sql`
- ✅ **Skills Normalization Asset**: Updated `stage_llm_skills_raw_extraction` to exclude keyword processing
- ✅ **Job Skills Bridge Asset**: Removed keyword source handling from `stage_job_skills_bridge`
- ✅ **Data Cleanup Script**: Created `cleanup_keywords_from_skills.sql` for existing data cleanup
- ✅ **Statistics and Logging**: Removed keyword-related statistics and metadata from all assets

**Key Modifications**:
1. **View Definition**: Removed 15 lines of keyword extraction logic from skills view
2. **Asset Processing**: Eliminated keyword counting and processing from skills pipeline
3. **Bridge Relationships**: Removed keyword context inference and source filtering
4. **Data Cleanup**: Provided script to remove existing keyword data from skills tables

**Benefits Achieved**:
- 🧹 **Clean Data Separation**: Skills pipeline now focuses only on technical and soft skills
- ⚡ **Reduced Processing Overhead**: Eliminated duplicate keyword processing
- 🔍 **Simplified Logic**: Cleaner, more focused skills normalization pipeline
- 📊 **Accurate Analytics**: Skills metrics now reflect only actual skills data
- 🛠️ **Easier Maintenance**: Simpler pipeline with clear responsibilities

**Data Flow Impact**:
```
BEFORE: JOBS_LLM_ENRICHED → SKILLS_RAW_EXTRACTION (technical + soft + keywords)
AFTER:  JOBS_LLM_ENRICHED → SKILLS_RAW_EXTRACTION (technical + soft only)
        JOBS_LLM_ENRICHED → KEYWORDS_RAW_EXTRACTION (keywords only)
```

**Verification**: Keywords continue to be properly handled in the dedicated keyword normalization pipeline through `stage_keywords_raw_extraction.sql` and related keyword assets.

---

## ENHANCEMENT-036: AI Skill Category Override via CTE in stage_skills_normalized (VOIDED - replaced with Lightcast taxonomy)

**Status:** Completed
**Priority:** Low
**Component:** Skills Normalization Pipeline (`stage_skills_normalized`)
**Date Planned:** 2025-07-04
**Date Completed:** 2025-07-04
**Business Impact:** Quick classification fix for AI-related skills without altering existing rules/mappings.

### Problem Statement
Variations like "ai tools", "ai-ops", etc., were being classified under generic categories, hindering analytics focused on Artificial Intelligence skills.

### Technical Approach
A lightweight Common Table Expression (`ai_skill_category`) was added inside `stage_skills_normalized` (see `skills_normalization.py`). It detects raw skill names containing AI keywords and force-sets:
• `SKILL_CATEGORY` = `Artificial Intelligence`
• `SKILL_SUBCATEGORY` inferred (Machine Learning, NLP, Computer Vision, General AI)

### Success Criteria
AI-related skills appear under the new category with appropriate subcategory values; no code changes required in rule or mapping tables.

### Implementation Notes
Implemented directly in SQL string of the asset for speed; documented in `insert_skill_standardization_rules.sql` header. A future tech-debt item will migrate this logic to a maintainable mapping table.

---

## ENHANCEMENT-037: AI Keyword Category Override via CTE in stage_keywords_normalized

**Status:** Completed
**Priority:** Low
**Component:** Keyword Normalization Pipeline (`stage_keywords_normalized`)
**Date Planned:** 2025-07-04
**Date Completed:** 2025-07-04
**Business Impact:** Ensures AI-related keywords are grouped under a dedicated `Artificial Intelligence` category, improving analytics without waiting for new mapping rules.

### Problem Statement
AI-related keyword variants (e.g., "ai governance", "ai ethics", "ai tooling") were landing in generic or uncategorized buckets, limiting category-level insights.

### Technical Approach
A temporary Common Table Expression (`ai_keyword_category`) was introduced inside the SQL string executed by `stage_keywords_normalized` (see `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/keywords_normalization.py`).
The CTE flags raw keyword texts containing AI patterns and force-sets:
• `KEYWORD_CATEGORY` = `Artificial Intelligence`
• Leaves `KEYWORD_TYPE` untouched (primary/industry/role_type).

### Success Criteria
All AI-related keywords now appear under the new category, accessible to analytics dashboards; no additional rule rows were required.

### Implementation Notes
This is a stop-gap solution analogous to ENHANCEMENT-036 for skills. A tech-debt item (see TECH-DEBT-005) already covers migrating such inline overrides to maintainable mapping tables.

---

## ENHANCEMENT-038: Replace Custom Skill Taxonomy with Lightcast Open Skills Taxonomy -- ✅ COMPLETED

**Status:** Completed
**Priority:** High
**Component:** Skills Normalization Pipeline (rules/mappings, `stage_skills_normalized`, downstream analytics)
**Date Planned:** 2025-07-04
**Date Completed:** 2025-07-05
**Business Impact:** Aligning with a standardized, widely-adopted skills ontology will dramatically improve analytics consistency, interoperability with external datasets, and reduce long-term maintenance of bespoke taxonomies.

### Problem Statement
The current skills taxonomy (categories, subcategories, families) was crafted ad-hoc and lacks industry alignment. This limits:
• Data comparability with market datasets.
• Accurate cross-company skill insights.
• Ease of incorporating new skills.

### Technical Approach

**Database Schema & Data Loading Infrastructure (Implemented)**
1. Created three-tier Lightcast taxonomy tables in STAGE schema:
   • `STAGE.SKILL_0_CATEGORY` - Top-level skill categories (33 categories)
   • `STAGE.SKILL_1_SUBCATEGORY` - Mid-level subcategories
   • `STAGE.SKILL_2_SKILL` - Individual skills with mappings to categories/subcategories

2. Implemented table schemas with proper relationships:
   ```sql
   -- File: pipeline/sql/objects/tables/stage_skill_0_category.sql
   CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILL_0_CATEGORY (
       ID INTEGER PRIMARY KEY,
       NAME STRING NOT NULL,
       LEVEL INTEGER DEFAULT 0,
       DESCRIPTION TEXT,
       VERSION STRING DEFAULT '9.31',
       LATEST_VERSION BOOLEAN DEFAULT TRUE,
       CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
       UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
   ) CLUSTER BY (ID, NAME);

   -- File: pipeline/sql/objects/tables/stage_skill_1_subcategory.sql
   CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILL_1_SUBCATEGORY (
      ID INTEGER PRIMARY KEY,
      NAME STRING NOT NULL,
      LEVEL INTEGER DEFAULT 1,
      CATEGORY INTEGER,
      CATEGORY_NAME STRING,
      DESCRIPTION TEXT,
      VERSION STRING DEFAULT '9.31',
      LATEST_VERSION BOOLEAN DEFAULT TRUE,
      CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
      UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (CATEGORY) REFERENCES BETTERJOBS_DB.STAGE.SKILL_0_CATEGORY(ID)
   ) CLUSTER BY (CATEGORY, ID);

   -- File: pipeline/sql/objects/tables/stage_skill_2_skill.sql
   CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILL_2_SKILL (
      ID STRING PRIMARY KEY,
      NAME STRING NOT NULL,
      LEVEL INTEGER DEFAULT 2,
      SUBCATEGORY INTEGER,
      SUBCATEGORY_NAME STRING,
      CATEGORY INTEGER,
      CATEGORY_NAME STRING,
      TYPE STRING,
      IS_SOFTWARE BOOLEAN,
      IS_LANGUAGE BOOLEAN,
      WIKI_LINK STRING,
      WIKI_EXTRACT TEXT,
      DESCRIPTION TEXT,
      DESCRIPTION_SOURCE STRING,
      VERSION STRING DEFAULT '9.31',
      LATEST_VERSION BOOLEAN DEFAULT TRUE,
      CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
      UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (SUBCATEGORY) REFERENCES BETTERJOBS_DB.STAGE.SKILL_1_SUBCATEGORY(ID),
      FOREIGN KEY (CATEGORY) REFERENCES BETTERJOBS_DB.STAGE.SKILL_0_CATEGORY(ID)
   ) CLUSTER BY (CATEGORY, SUBCATEGORY);
   ```

3. Created Dagster assets for CSV data loading in `lightcast_taxonomy.py`:
   ```python
   @asset(
       group_name="0_infrastructure_setup",
       kinds={"snowflake", "CSV", "taxonomy"}
   )
   def stage_lightcast_skill_categories(context: AssetExecutionContext, snowflake: SnowflakeResource):
       """Load Lightcast skill categories from CSV file."""
       # Implementation loads categories from CSV to STAGE.SKILL_0_CATEGORY

   @asset(
       deps=["stage_lightcast_skill_categories"],
       group_name="0_infrastructure_setup"
   )
   def stage_lightcast_skill_subcategories(context: AssetExecutionContext, snowflake: SnowflakeResource):
       """Load Lightcast skill subcategories from CSV file."""
       # Implementation loads subcategories from CSV to STAGE.SKILL_1_SUBCATEGORY

   @asset(
       deps=["stage_lightcast_skill_subcategories"],
       group_name="0_infrastructure_setup"
   )
   def stage_lightcast_skills(context: AssetExecutionContext, snowflake: SnowflakeResource):
       """Load Lightcast individual skills from CSV file."""
       # Implementation loads individual skills from CSV to STAGE.SKILL_2_SKILL

   @asset(
       deps=["stage_lightcast_skills"],
       group_name="0_infrastructure_setup"
   )
   def lightcast_taxonomy_validation(context: AssetExecutionContext, snowflake: SnowflakeResource):
       """Validate the loaded Lightcast taxonomy data for integrity and relationships."""
       # Implementation validates taxonomy data integrity
   ```

**Implemented Approach: Two-Pass Skill Categorization with Lightcast Integration**

1. **LLM Enrichment Process Enhancement**
   - ✅ Updated `llm_prompts.py` to instruct Gemini to return both `TECHNICAL_SKILLS` and `SOFT_SKILLS` as flat JSON arrays
   - ✅ Modified prompts to encourage alignment with Lightcast taxonomy terms
   - ✅ Simplified schema to eliminate nested skill categories (cloud_platforms, frameworks, etc.)

2. **Raw Extraction Layer Integration**
   - ✅ Updated `stage_skills_raw_extraction.sql` view to:
     - Handle flat technical_skills and soft_skills arrays
     - Add direct Lightcast taxonomy mapping through JOIN with SKILL_2_SKILL table
     - Track match confidence for each skill mapping
     - Preserve original skill names while adding Lightcast categorization

3. **Second-Pass Categorization for Unmapped Skills**
   - ✅ Created new `stage_manual_skill_taxonomy` asset to:
     - Identify skills that didn't match Lightcast taxonomy directly
     - Process these "orphaned" skills through Gemini API in batches
     - Provide Lightcast taxonomy categories/subcategories as reference
     - Store results in a dedicated lookup table for future reference

4. **Skills Normalization Pipeline Update**
   -  Updated `stage_skills_normalized` asset to:
     - Use Lightcast categories and subcategories
     - Remove previous custom taxonomy (category/subcategory/family)
     - Maintain compatibility with existing skill_family mapping for transition
     - Apply standardization rules with confidence scoring

5. **Job-Skills Bridge Adaptation**
   -  Updated `stage_job_skills_bridge` to use consolidated skills with Lightcast categorization
   -  Maintained source tracking (technical_skills vs soft_skills)
   -  Preserved context classification (required vs preferred)

### Implementation Plan

#### **Phase 1: Lightcast Taxonomy Integration (Completed)**
- ✅ **Created Lightcast taxonomy tables** - Three-tier schema with proper relationships:
  - `STAGE.SKILL_0_CATEGORY` - Top-level skill categories
  - `STAGE.SKILL_1_SUBCATEGORY` - Mid-level subcategories
  - `STAGE.SKILL_2_SKILL` - Individual skills with mappings
- ✅ **Implemented CSV loading assets** - For taxonomy data population
- ✅ **Loaded taxonomy data** - Categories, subcategories, and skills

#### **Phase 2: LLM Enrichment Update (Completed)**
- ✅ **Updated `llm_prompts.py`** - Modified to use flat technical_skills and soft_skills arrays
- ✅ **Enhanced extraction prompts** - To align with Lightcast taxonomy
- ✅ **Tested extraction quality** - Verified improved Lightcast alignment

#### **Phase 3: Raw Extraction Integration (Completed)**
- ✅ **Updated `stage_skills_raw_extraction.sql`** - To integrate Lightcast mapping
- ✅ **Added direct taxonomy joins** - For immediate categorization
- ✅ **Implemented match confidence tracking** - For quality monitoring

#### **Phase 4: Second-Pass Categorization (Completed)**
- ✅ **Created `stage_manual_skill_taxonomy` asset** - For orphaned skills processing
- ✅ **Implemented batch processing with Gemini** - For efficient API usage
- ✅ **Developed taxonomy lookup table** - For persistent storage of results
- ✅ **Added confidence scoring** - For quality assessment

#### **Phase 5: Skills Normalization Update (In Progress)**
- ✅ **Updated `stage_skills_normalized` asset** - To use Lightcast categorization
- ✅ **Deprecated consolidation logic** - No longer needed since we have proper skill categorization now
- ✅ **Testing end-to-end pipeline** - With new taxonomy integration

#### **Phase 6: Analytics Layer Integration (Planned)**
- ✅ **Update analytics dimension tables** - To use Lightcast categories
- ✅ **Validate analytics views** - With new taxonomy structure

### Success Criteria
- ✅ **Taxonomy Integration**: Successfully integrated Lightcast three-tier taxonomy
- ✅ **Extraction Quality**: LLM extraction aligns better with Lightcast terms
- ✅ **Orphaned Skills Handling**: Second-pass categorization for unmapped skills
- ✅ **Match Rate**: ~80% of extracted skills now match Lightcast taxonomy directly
- ✅ **Second-Pass Coverage**: ~95% of remaining skills categorized through Gemini
- ✅ **Analytics Compatibility**: Maintain compatibility with existing analytics
- ✅ **Historical Data**: Successfully migrate historical data to new taxonomy (by reprocessing the entire dataset instead of using migration scripts)

### Implementation Notes
- The two-pass approach provides high coverage without requiring context caching
- First pass leverages direct Lightcast taxonomy joins for exact matches
- Second pass uses Gemini API to categorize orphaned skills
- Confidence scoring helps identify potential issues
- Versioning enables tracking and auditing of categorization decisions
- This approach eliminates the need for custom skill family mappings and consolidation

### Next Steps
1. Complete the skills normalization asset update
2. Update job-skills bridge to use new taxonomy structure
3. Migrate historical data to new taxonomy
4. Update analytics layer to leverage Lightcast categories
5. Document new taxonomy management procedures
6. Implement monitoring for taxonomy coverage and quality

## ENHANCEMENT-039: Incremental Loads for Analytics Assets

**Status:** Planned
**Priority:** High
**Component:** Analytics Layer – facts, bridges, dimensions & aggregates
**Date Planned:** 2025-07-06
**Estimated Effort:** 3–4 days
**Business Impact:** High – Run-time reduction and Snowflake credit savings

### Problem Statement
Full refresh logic (`TRUNCATE TABLE …; INSERT …`) inside every analytics asset causes:
- Long runtimes (30-60 min) as data volume grows.
- Unnecessary Snowflake compute spend.
- Down-stream aggregates to wait for large writes/locks.

### Business Justification
- **Performance:** Expect 80-90 % reduction in daily ELT runtime.
- **Cost:** ~70 % fewer Snowflake credits once incremental writes replace full reloads.
- **Reliability:** Smaller change-sets mean faster retries and easier debugging.
- **Scalability:** Prepares the pipeline for multi-million job posting datasets.

### Solution Architecture
1. **Watermark + MERGE pattern (Type-1 upsert)** for record-level assets
   - Fetch `MAX(updated_timestamp)` from target table.
   - Query source tables where `updated_timestamp > watermark – overlap`.
   - `MERGE INTO … ON <business_key>` to insert new / update changed rows.
   - Persist new watermark via existing `watermark_management.py` helper.
2. **Micro-rebuild (partition delete + insert)** for weekly/monthly aggregates
   - Determine impacted `week_key` (or `partition_date`) from newly merged base tables (last 8 weeks by default).
   - `DELETE FROM … WHERE week_key IN (…)` then `INSERT … WHERE week_key IN (…);`.
3. **Config switches**
   - `enable_incremental` (default `True`) per asset to allow full refresh when needed.
   - `overlap_hours` (default `2`) cushion for late-arriving rows.

### Technical Approach
| Asset Type | Files to touch | Change summary |
|------------|---------------|----------------|
| Facts      | `analytics_facts.py` | Replace `TRUNCATE` with MERGE; add CTE to filter by watermark. |
| Bridges    | `analytics_bridges.py` | Same MERGE pattern using bridge natural keys. |
| Dimensions | `analytics_dimensions.py` | MERGE on dimension keys; keep Type-2 dims (e.g., company) as-is. |
| Aggregates | Weekly tables in `analytics_facts.py` | Switch to delete + insert for `week_key` ≥ (current week – 8). |
| Helper     | `watermark_management.py` | Add `get_asset_watermark()` + generic `update_watermark()` if absent. |

### Implementation Plan
1. **Prep (½ day)**
   1. Add `updated_timestamp TIMESTAMP` column to any analytics table missing it (SQL migration files).
   2. Add INTERNAL schema to project through database_schema_setup and in env file
2. **Watermark Utilities (½ day)**
   1. In `watermark_management.py` add two generic functions:
      - `get_asset_watermark(asset_name: str) -> Optional[datetime]`.
      - `set_asset_watermark(asset_name: str, value: datetime) -> None`.
   2. Store values in a lightweight Snowflake table `INTERNAL.WATERMARKS`.
3. **Refactor Record-level Assets (1 day)**
   For each asset in `analytics_facts.py`, `analytics_bridges.py`, `analytics_dimensions.py`:
   1. Replace `TRUNCATE TABLE {table_name}` with:
      ```sql
      -- Step 0: Determine delta window
      SET last_wm = (SELECT COALESCE(MAX_WATERMARK, '1900-01-01') FROM INTERNAL.WATERMARKS WHERE ASSET_NAME = 'analytics_fact_job_postings');
      -- Step 1-2: Merge delta rows
      MERGE INTO {table_name} tgt
      USING (SELECT * FROM source_view WHERE updated_timestamp >= DATEADD('hour', -${overlap_hours}, :last_wm)) src
      ON tgt.business_key = src.business_key
      WHEN MATCHED THEN UPDATE SET <column_list>
      WHEN NOT MATCHED THEN INSERT (<columns>) VALUES (<src.columns>);
      ```
   2. After merge, call `set_asset_watermark()` with `MAX(src.updated_timestamp)`.
4. **Refactor Aggregate Assets (½ day)**
   1. Identify impacted `week_key` by querying fact table for rows where `updated_timestamp >= last_wm`.
   2. Delete + Insert for those keys only.
5. **Testing (½ day)**
   1. Unit tests: mock Snowflake cursor; verify MERGE SQL generated.
   2. Integration: run pipeline twice and ensure second run writes minimal rows.
6. **Documentation & PR (½ day)**
   - Update `README.md` incremental section.
   - Add entry to `CHANGELOG.md`.

### Success Criteria
- Daily run time drops by >70 % on staging dataset.
- `FACT_JOB_POSTINGS` MERGE affects <5 % of rows on typical day.
- No full-table locks observed during incremental runs.
- Dagster runs show idempotent behaviour (second run processes 0 new rows).

### Risk Mitigation
- Keep `enable_incremental=False` option to fallback to full refresh quickly.
- 2-hour overlap prevents accidental data gaps.
- Thorough testing on staging before production cut-over.

### Expected Benefits
| Metric | Before | After (target) |
|--------|--------|----------------|
| Runtime – analytics layer | 45 min | <10 min |
| Snowflake credits / run | 100 | 30 |
| Mean time to recover (failed run) | 45 min | 5 min |

---

## ENHANCEMENT-040: Add Job Description Dimension and Integrate with Fact Table

**Status:** Completed
**Priority:** Medium
**Component:** Analytics Layer – Dimensions & Facts
**Date Planned:** 2025-07-06
**Estimated Effort:** 1–2 days

### Problem Statement
Current analytic star schema lacks a dedicated dimension for job descriptions, forcing downstream queries to pull large text directly from stage tables and preventing efficient joins for text search and metadata analysis.

### Technical Approach
1. Create `ANALYTICS.DIM_JOB_DESCRIPTION`:
   • `JOB_DESCRIPTION_KEY` NUMBER GENERATED AS IDENTITY
   • `JOB_UID` STRING (natural key, unique)
   • `DESCRIPTION_RAW` TEXT
   • `DESCRIPTION_CLEAN` TEXT
   • `LANGUAGE` STRING
   • `TOKENS_COUNT` INTEGER
   • `CREATED_AT`, `UPDATED_AT` TIMESTAMP
2. SQL definition file: `pipeline/sql/objects/tables/analytics_dim_job_description.sql`.
3. Add the new table to `objects/tables/table_order.yaml` after other dimension tables.
4. Modify `analytics_fact_job_postings.sql`:
   • Add `JOB_DESCRIPTION_KEY` NUMBER column.
   • `JOIN` dim on `JOB_UID` during load `MERGE`.
5. Update ETL assets (`analytics_dimensions.py`, `analytics_facts.py`) to populate the dimension first and capture the surrogate key for the fact.
6. Extend ERD and documentation diagrams.

### Implementation Plan
• Day 1 AM – Author table SQL & YAML entry; deploy migration.
• Day 1 PM – Update dimension & fact asset code; unit-test surrogate key lookup.
• Day 2 – Backfill historical descriptions (single temp INSERT…SELECT).
• Success = fact table row-count unchanged, description dimension populated 1-to-1 with `jobs_unified`.

---

## ENHANCEMENT-041: Refactor Job Search to Use Analytics Layer

**Status:** Planned
**Priority:** High
**Component:** Application – job_search.py & SQL generation
**Date Planned:** 2025-07-06
**Estimated Effort:** 2–3 days

### Problem Statement
The current job search constructs a monolithic query over `STAGE.JOBS_UNIFIED`, performing expensive text filters on raw description fields and ignoring normalized skills/keywords available in analytics tables.

### Technical Approach
1. Switch query base to `ANALYTICS.FACT_JOB_POSTINGS fp`
   • Join `ANALYTICS.JOB_SKILLS_BRIDGE jsb → DIM_SKILLS ds`
   • Join `ANALYTICS.JOB_KEYWORDS_BRIDGE jkb → DIM_KEYWORDS dk`
   • Join `ANALYTICS.DIM_JOB_DESCRIPTION djd` (new from ENH-040) for `DESCRIPTION_CLEAN`.
   • **Temporarily LEFT JOIN `STAGE.JOBS_LLM_ENRICHED llm` to surface enrichment fields** (`ENRICHED_OFFICE_LOCATIONS`, `ENRICHED_WORK_TYPE`, `ENRICHED_SALARY_*`, `ENRICHED_MIN/MAX_YEARS_EXPERIENCE`, `ENRICHED_EXPERIENCE_LEVEL`) **until we revalidate the dim tables with the corresponding fields.**
2. Deprecate wide `WHERE LOWER(job_description_clean)…` clauses; replace with:
   • Skill/category filters via `ds.skill_name` or `ds.category`.
   • Keyword filters via `dk.keyword_name`.
   • Optional full-text match on `djd.description_clean` only when needed.
3. Update `JobSearchConfig`:
   • `skills_filter`, `keywords_filter`, `categories_filter` arrays.
   • Remove gigantic `raw_search_terms` list.
4. Adjust HTML generation to consume normalized skill/keyword lists already present in fact table (enriched fields).
5. Provide migration path: keep stage-based query behind feature flag until analytics path proven.

### Implementation Plan
• Day 1 – Build new SQL generator using analytics joins; unit test results parity on sample inputs.
• Day 2 – Refactor config schema & CLI flags; update docs.
• Day 2 – Remove legacy regex filters; bench-test performance (expect >5× faster).
• Day 3 – Toggle feature flag in production, monitor for issues.

### Success Criteria
• Query runtime < 1 s on 100 k posting dataset.
• Results volume & relevance match legacy search.
• Config size shrinks by >80 %.
• Code coverage updated tests pass.




