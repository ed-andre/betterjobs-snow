"""
Dynamic Lookback Calculation for Job Discovery

Implements intelligent incremental processing for jobs discovery by calculating
optimal lookback periods based on actual data freshness patterns instead of
fixed days_to_lookback.

This module supports ENHANCEMENT-004: Incremental Processing Strategy - Smart Watermark System
STEP 1: Jobs Discovery Layer (RAW Data Ingestion)
"""

import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass
import snowflake.connector
from dagster import get_dagster_logger

logger = get_dagster_logger()


@dataclass
class DynamicLookbackConfig:
    """Configuration for dynamic lookback calculation."""
    cushion_days: int = 2          # Safety cushion to add to calculated days
    min_lookback_days: int = 2    # Minimum lookback period
    max_lookback_days: int = 90    # Maximum lookback period
    default_lookback_days: int = 15 # Default for new companies
    enable_dynamic: bool = True    # Enable dynamic calculation


def get_dynamic_lookback_period(
    platform: str,
    company_id: str,
    config: DynamicLookbackConfig,
    conn: snowflake.connector.SnowflakeConnection,
    context=None,
    date_field: str = "date_posted"
) -> int:
    """
    Calculate optimal lookback period based on actual data freshness.

    Args:
        platform: ATS platform name (bamboohr, greenhouse, workday, smartrecruiters)
        company_id: Company identifier
        config: Configuration with cushion and min/max values
        conn: Snowflake connection
        context: Dagster execution context for logging
        date_field: Name of the date field to check (date_posted or published_at)

    Returns:
        Optimal lookback period in days
    """
    if not config.enable_dynamic:
        return config.default_lookback_days

    try:
        database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
        schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")

        # Simple query to get days since last job posting
        query = f"""
        SELECT DATEDIFF('day', MAX({date_field}), CURRENT_DATE) as days_since_latest
        FROM {database_name}.{schema_name}.{platform}_jobs
        WHERE company_id = %s
        """

        cursor = conn.cursor()
        cursor.execute(query, (company_id,))
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            days_since_latest = int(result[0])
            # Calculate: max(days_since_latest + cushion, min_lookback), capped at max_lookback
            lookback_days = min(
                max(days_since_latest + config.cushion_days, config.min_lookback_days),
                config.max_lookback_days
            )

            if context:
                context.log.info(
                    f"[{platform}] Company {company_id}: Last job {days_since_latest} days ago. "
                    f"Using {lookback_days} day lookback"
                )
            return lookback_days
        else:
            # No historical data - new company
            if context:
                context.log.info(f"[{platform}] Company {company_id}: New company, using default {config.default_lookback_days} days")
            return config.default_lookback_days

    except Exception as e:
        if context:
            context.log.warning(
                f"[{platform}] Error calculating lookback for company {company_id}: {str(e)}. "
                f"Using default {config.default_lookback_days} days"
            )
        return config.default_lookback_days


def get_batch_lookback_periods(
    platform: str,
    company_ids: list,
    config: DynamicLookbackConfig,
    conn: snowflake.connector.SnowflakeConnection,
    context=None,
    date_field: str = "date_posted"
) -> Dict[str, int]:
    """
    Calculate dynamic lookback periods for a batch of companies efficiently.

    Args:
        platform: ATS platform name
        company_ids: List of company IDs to calculate lookback for
        config: Configuration with cushion and min/max values
        conn: Snowflake connection
        context: Dagster execution context for logging
        date_field: Name of the date field to check (date_posted or published_at)

    Returns:
        Dictionary mapping company_id to optimal lookback days
    """
    if not config.enable_dynamic:
        return {str(cid): config.default_lookback_days for cid in company_ids}

    if not company_ids:
        return {}

    try:
        database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
        schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")

        # Use parameterized query to avoid SQL injection
        placeholders = ",".join(["%s"] * len(company_ids))

        query = f"""
        SELECT
            company_id,
            DATEDIFF('day', MAX({date_field}), CURRENT_DATE) as days_since_latest
        FROM {database_name}.{schema_name}.{platform}_jobs
        WHERE company_id IN ({placeholders})
        GROUP BY company_id
        """

        cursor = conn.cursor()
        cursor.execute(query, company_ids)
        results = cursor.fetchall()
        cursor.close()

        # Build result dictionary
        lookback_map = {}

        # Process companies with historical data
        for result in results:
            company_id = str(result[0])
            days_since_latest = int(result[1]) if result[1] is not None else config.default_lookback_days

            lookback_days = min(
                max(days_since_latest + config.cushion_days, config.min_lookback_days),
                config.max_lookback_days
            )

            lookback_map[company_id] = lookback_days

        # Add default lookback for companies not found in historical data
        for cid in company_ids:
            if str(cid) not in lookback_map:
                lookback_map[str(cid)] = config.default_lookback_days

        if context:
            avg_lookback = sum(lookback_map.values()) / len(lookback_map) if lookback_map else 0
            context.log.info(
                f"[{platform}] Calculated lookback for {len(company_ids)} companies. "
                f"Average: {avg_lookback:.1f} days"
            )

        return lookback_map

    except Exception as e:
        if context:
            context.log.warning(
                f"[{platform}] Error calculating batch lookback: {str(e)}. "
                f"Using default {config.default_lookback_days} days for all companies"
            )
        return {str(cid): config.default_lookback_days for cid in company_ids}