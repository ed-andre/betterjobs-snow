"""
Universal Partitioning Module

Centralized partition definitions and utilities for all asset types in the BetterJobs pipeline.
Provides consistent partitioning strategies across discovery, LLM enrichment, analytics, and other assets.

PARTITIONING STRATEGIES:
- Company-based alpha partitioning (A-Z, 0-9, other) - Primary strategy for parallel processing
- Platform-based partitioning (bamboohr, greenhouse, etc.) - For platform-specific processing
- Extensible framework for additional partition types


"""

from dagster import StaticPartitionsDefinition
from typing import Dict, List, Optional


# ====================================================================
# COMPANY-BASED ALPHA PARTITIONING
# Universal company partitioning by first letter/character
# ====================================================================

# Universal company-based alpha partitioning
# Used by: job discovery assets, LLM enrichment assets, analytics assets
company_alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

# Backward compatibility alias for existing LLM assets
llm_company_partitions = company_alpha_partitions


# ====================================================================
# PARTITION FILTER BUILDERS
# Universal SQL filter generation for different partitioning strategies
# ====================================================================

def build_company_alpha_filter(partition_key: str, company_column: str = "COMPANY_NAME_CLEAN") -> str:
    """
    Build SQL filter for company-based alpha partitioning.

    Universal function that works with any company name column and supports
    the standard A-Z, 0-9, other partitioning strategy.

    Args:
        partition_key: Partition key (A-Z, 0-9, other)
        company_column: Column name containing company names (default: COMPANY_NAME_CLEAN)

    Returns:
        SQL WHERE clause filter for company name partitioning

    Examples:
        >>> build_company_alpha_filter("A", "COMPANY_NAME")
        "(COMPANY_NAME LIKE 'A%' OR COMPANY_NAME LIKE 'a%')"

        >>> build_company_alpha_filter("0-9", "COMPANY_NAME_CLEAN")
        "SUBSTRING(COMPANY_NAME_CLEAN, 1, 1) BETWEEN '0' AND '9'"
    """
    if partition_key == "0-9":
        return f"SUBSTRING({company_column}, 1, 1) BETWEEN '0' AND '9'"
    elif partition_key == "other":
        return f"""NOT (
            SUBSTRING({company_column}, 1, 1) BETWEEN 'A' AND 'Z' OR
            SUBSTRING({company_column}, 1, 1) BETWEEN 'a' AND 'z' OR
            SUBSTRING({company_column}, 1, 1) BETWEEN '0' AND '9'
        )"""
    else:
        return f"""(
            {company_column} LIKE '{partition_key}%' OR
            {company_column} LIKE '{partition_key.lower()}%'
        )"""


# Backward compatibility function for existing LLM assets
def build_company_partition_filter(partition_key: str) -> str:
    """
    Build SQL filter for company-based partitioning.

    DEPRECATED: Use build_company_alpha_filter() instead for better flexibility.
    Maintained for backward compatibility with existing LLM enrichment assets.
    """
    return build_company_alpha_filter(partition_key, "COMPANY_NAME_CLEAN")


# ====================================================================
# ADDITIONAL PARTITION DEFINITIONS
# Common partitioning strategies for different asset types
# ====================================================================

# Platform-based partitioning for multi-platform assets
platform_partitions = StaticPartitionsDefinition([
    "bamboohr", "greenhouse", "workday", "smartrecruiters", "icims", "lever", "jobvite"
])

# ====================================================================
# UNIVERSAL VALIDATION AND UTILITY FUNCTIONS
# ====================================================================

def validate_partition_key(partition_key: str, partitions_def: StaticPartitionsDefinition = company_alpha_partitions) -> bool:
    """
    Validate that a partition key is valid for the given partition definition.

    Args:
        partition_key: Partition key to validate
        partitions_def: Partition definition to validate against (default: company_alpha_partitions)

    Returns:
        True if valid, False otherwise
    """
    valid_keys = set(partitions_def.get_partition_keys())
    return partition_key in valid_keys


def get_company_alpha_partition_info(partition_key: str, company_column: str = "COMPANY_NAME_CLEAN") -> Dict:
    """
    Get information about a specific company alpha partition.

    Args:
        partition_key: Partition key
        company_column: Company column name to use in SQL filter

    Returns:
        Dict with partition metadata
    """
    if not validate_partition_key(partition_key, company_alpha_partitions):
        raise ValueError(f"Invalid company alpha partition key: {partition_key}")

    partition_info = {
        "partition_key": partition_key,
        "partition_type": "company_alpha",
        "filter_sql": build_company_alpha_filter(partition_key, company_column),
        "total_partitions": len(company_alpha_partitions.get_partition_keys()),
        "company_column": company_column
    }

    if partition_key == "0-9":
        partition_info["description"] = "Companies starting with numbers (0-9)"
    elif partition_key == "other":
        partition_info["description"] = "Companies starting with special characters"
    else:
        partition_info["description"] = f"Companies starting with letter '{partition_key}'"

    return partition_info


def get_platform_partition_info(partition_key: str) -> Dict:
    """
    Get information about a specific platform partition.

    Args:
        partition_key: Platform partition key

    Returns:
        Dict with partition metadata
    """
    if not validate_partition_key(partition_key, platform_partitions):
        raise ValueError(f"Invalid platform partition key: {partition_key}")

    return {
        "partition_key": partition_key,
        "partition_type": "platform",
        "filter_sql": f"PLATFORM = '{partition_key}'",
        "total_partitions": len(platform_partitions.get_partition_keys()),
        "description": f"Jobs from {partition_key} platform"
    }


# ====================================================================
# CONVENIENCE FUNCTIONS FOR COMMON USE CASES
# ====================================================================

def get_all_company_alpha_keys() -> List[str]:
    """Get all company alpha partition keys as a list."""
    return company_alpha_partitions.get_partition_keys()


def get_all_platform_keys() -> List[str]:
    """Get all platform partition keys as a list."""
    return platform_partitions.get_partition_keys()


def build_discovery_company_filter(partition_key: str, company_column: str = "company_name") -> str:
    """
    Build company filter for job discovery assets.

    Optimized for discovery assets which typically use lowercase column names.

    Args:
        partition_key: Company alpha partition key
        company_column: Company column name (default: company_name)

    Returns:
        SQL WHERE clause filter
    """
    return build_company_alpha_filter(partition_key, company_column)


def build_stage_company_filter(partition_key: str, company_column: str = "COMPANY_NAME_CLEAN") -> str:
    """
    Build company filter for stage/analytics assets.

    Optimized for stage assets which typically use uppercase column names.

    Args:
        partition_key: Company alpha partition key
        company_column: Company column name (default: COMPANY_NAME_CLEAN)

    Returns:
        SQL WHERE clause filter
    """
    return build_company_alpha_filter(partition_key, company_column)


# ====================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# Maintained for existing assets during migration period
# ====================================================================

# Backward compatibility function for existing assets
def get_partition_info(partition_key: str) -> dict:
    """
    Get information about a specific partition.

    DEPRECATED: Use get_company_alpha_partition_info() instead for better flexibility.
    Maintained for backward compatibility with existing LLM enrichment assets.
    """
    return get_company_alpha_partition_info(partition_key, "COMPANY_NAME_CLEAN")