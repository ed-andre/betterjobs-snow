import time
import json
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dagster import (
    asset, AssetExecutionContext, Config, get_dagster_logger,
    MetadataValue, AssetMaterialization, StaticPartitionsDefinition,
    Definitions, define_asset_job
)

from dagster_betterjobs.scrapers.workday_scraper import WorkdayScraper

logger = get_dagster_logger()

# Partition companies alphabetically A-Z + numeric + other
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

class WorkdayJobsDiscoveryConfig(Config):
    """Configuration parameters for Workday job discovery."""
    max_companies: Optional[int] = None  # Optional limit on processed companies
    rate_limit: float = 2.0  # Seconds between requests
    max_retries: int = 3
    retry_delay: int = 2
    min_company_id: Optional[int] = None  # For batch processing
    max_company_id: Optional[int] = None  # For batch processing
    days_to_look_back: int = 7  # Job freshness threshold in days
    batch_size: int = 10  # Companies per batch before committing
    skip_detailed_fetch: bool = False  # Set to True to skip detailed job info
    process_all_companies: bool = True  # By default, process all companies regardless of checkpoint status

@asset(
    group_name="raw_ingestion_extraction",
    kinds={"API", "snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=["snowflake_master_company_urls"],
    partitions_def=alpha_partitions
)
def workday_company_jobs_discovery(context: AssetExecutionContext, config: WorkdayJobsDiscoveryConfig) -> Dict:
    """
    Discovers and stores job listings from Workday career sites.
    Processes companies partitioned by first letter of company name.
    """
    # Initialize Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
    schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    partition_key = context.partition_key

    # Set job freshness cutoff date
    cutoff_date = datetime.now() - timedelta(days=config.days_to_look_back)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    context.log.info(f"Processing partition {partition_key} - jobs posted after {cutoff_str}")

    # Resolve checkpoint directory path
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Configure partition-specific checkpoint files
    checkpoint_file = checkpoint_dir / f"workday_jobs_discovery_{partition_key}_checkpoint.csv"
    failed_companies_file = checkpoint_dir / f"workday_jobs_discovery_{partition_key}_failed.csv"

    # Build query for companies in current partition
    if partition_key == "0-9":
        letter_filter = "AND SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'"
    elif partition_key == "other":
        letter_filter = "AND NOT (SUBSTRING(company_name, 1, 1) BETWEEN 'A' AND 'Z' OR SUBSTRING(company_name, 1, 1) BETWEEN 'a' AND 'z' OR SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9')"
    else:
        letter_filter = f"AND (company_name LIKE '{partition_key}%' OR company_name LIKE '{partition_key.lower()}%')"

    query = f"""
    SELECT company_id, company_name, company_industry, career_url, ats_url
    FROM {database_name}.{schema_name}.master_company_urls
    WHERE platform = 'workday'
    AND (ats_url IS NOT NULL OR career_url IS NOT NULL)
    AND url_verified = TRUE
    {letter_filter}
    """

    # Apply additional filters if specified
    if config.min_company_id is not None:
        query += f" AND company_id >= '{config.min_company_id}'"
    if config.max_company_id is not None:
        query += f" AND company_id <= '{config.max_company_id}'"

    query += " ORDER BY company_id"

    if config.max_companies:
        query += f" LIMIT {config.max_companies}"

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        companies_df = pd.DataFrame(results, columns=columns)
        companies_df.columns = companies_df.columns.str.lower()
        cursor.close()
    except Exception as e:
        context.log.error(f"Error querying master_company_urls: {str(e)}")
        return {"error": str(e), "status": "failed"}

    total_companies = len(companies_df)
    context.log.info(f"Found {total_companies} companies in partition {partition_key}")

    # Create workday_jobs table if it doesn't exist
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {database_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS workday_jobs (
            job_id STRING,
            company_id STRING,
            job_title STRING,
            job_description STRING,
            job_url STRING,
            location STRING,
            time_type STRING,
            employment_type STRING,
            published_at DATE,
            valid_through DATE,
            date_retrieved TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN,
            raw_data STRING,
            partition_key STRING,
            work_type STRING,
            compensation STRING
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        context.log.error(f"Error creating jobs table: {str(e)}")
        return {"error": str(e), "status": "failed"}
    finally:
        cursor.close()

    # Track processing statistics
    stats = {
        "total_companies": total_companies,
        "companies_processed": 0,
        "companies_with_jobs": 0,
        "total_jobs_found": 0,
        "new_jobs_added": 0,
        "updated_jobs": 0,
        "recent_jobs": 0,
        "old_jobs_skipped": 0,
        "errors": 0,
        "partition_key": partition_key
    }

    # Resume from checkpoint if exists
    processed_company_ids = set()
    checkpoint_results = []
    failed_companies = []

    if checkpoint_file.exists() and not config.process_all_companies:
        try:
            checkpoint_df = pd.read_csv(checkpoint_file)
            processed_company_ids = set(checkpoint_df["company_id"].astype(str).tolist())
            checkpoint_results = checkpoint_df.to_dict("records")
            context.log.info(f"Loaded {len(processed_company_ids)} previously processed companies")
        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Filter companies based on checkpoint status
    if not config.process_all_companies and checkpoint_file.exists():
        companies_to_process = companies_df[~companies_df["company_id"].astype(str).isin(processed_company_ids)]
        context.log.info(f"{len(companies_to_process)} companies remaining after checkpoint filter")
    else:
        companies_to_process = companies_df

    # Load previously failed companies
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Process companies in batches
    batch_size = config.batch_size
    new_failed_companies = []

    for i in range(0, len(companies_to_process), batch_size):
        batch = companies_to_process.iloc[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(companies_to_process) + batch_size - 1) // batch_size

        batch_jobs = []

        # Process each company in batch
        for _, company in batch.iterrows():
            company_id = str(company["company_id"])
            company_name = company["company_name"]
            career_url = company["career_url"]
            ats_url = company["ats_url"]

            context.log.info(f"Processing {company_name} (ID: {company_id})")

            # Use ATS URL if available, otherwise fall back to career URL
            url_to_use = ats_url if ats_url else career_url

            # Track company results for checkpoint
            company_result = {
                "company_id": company_id,
                "company_name": company_name,
                "processed_at": datetime.now().isoformat(),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "status": "success",
                "partition_key": partition_key
            }

            try:
                # Create Workday scraper
                scraper = WorkdayScraper(
                    career_url=url_to_use,
                    rate_limit=config.rate_limit,
                    max_retries=config.max_retries,
                    retry_delay=config.retry_delay,
                    dagster_log=context.log
                )

                # Get all job listings
                job_listings = scraper.search_jobs()

                if not job_listings:
                    checkpoint_results.append(company_result)
                    continue

                context.log.info(f"Found {len(job_listings)} jobs for {company_name}")
                company_result["jobs_found"] = len(job_listings)
                stats["companies_with_jobs"] += 1
                stats["total_jobs_found"] += len(job_listings)

                company_jobs_added = 0
                company_jobs_updated = 0

                # Check which jobs already exist in Snowflake
                existing_jobs = {}
                if job_listings:
                    job_ids = [job.get("job_id") for job in job_listings if job.get("job_id")]
                    if job_ids:
                        cursor = conn.cursor()
                        try:
                            job_ids_placeholders = ",".join(["%s"] * len(job_ids))
                            existing_query = f"""
                            SELECT job_id, is_active, date_retrieved
                            FROM {database_name}.{schema_name}.workday_jobs
                            WHERE company_id = %s
                            AND job_id IN ({job_ids_placeholders})
                            """
                            cursor.execute(existing_query, [company_id] + job_ids)
                            for row in cursor.fetchall():
                                existing_jobs[row[0]] = {
                                    "is_active": row[1],
                                    "date_retrieved": row[2]
                                }
                        except Exception as e:
                            context.log.error(f"Error checking existing jobs: {str(e)}")
                        finally:
                            cursor.close()

                # Process each job listing
                for job in job_listings:
                    job_id = job.get("job_id")
                    job_title = job.get("job_title", "")
                    job_url = job.get("job_url", "")
                    location = job.get("location", "")
                    time_type = job.get("time_type", "")
                    posted_on = job.get("posted_on", "")

                    if not job_id:
                        continue

                    # Get detailed job info unless skipped in config
                    job_description = ""
                    employment_type = ""
                    date_posted = ""
                    valid_through = ""
                    work_type = job.get("work_type", "")
                    compensation = ""
                    raw_data = json.dumps(job.get("raw_data", {}))

                    if not config.skip_detailed_fetch:
                        try:
                            job_details = scraper.get_job_details(job_url)
                            job_description = job_details.get("job_description", "")
                            date_posted = job_details.get("date_posted", "")
                            valid_through = job_details.get("valid_through", "")
                            employment_type = job_details.get("employment_type", "")

                            if job_details.get("work_type"):
                                work_type = job_details.get("work_type")

                            if job_details.get("raw_data"):
                                raw_data = json.dumps(job_details.get("raw_data"))

                            time.sleep(config.rate_limit / 2)
                        except Exception as e:
                            context.log.warning(f"Error getting details for job {job_id}: {str(e)}")

                    # Parse date from posted_on text
                    published_at = None
                    if posted_on:
                        days_ago_match = re.search(r'Posted (\d+) Days? Ago', posted_on)
                        if days_ago_match:
                            days_ago = int(days_ago_match.group(1))
                            published_at = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                        elif "Posted Today" in posted_on:
                            published_at = datetime.now().strftime("%Y-%m-%d")
                        elif "Posted Yesterday" in posted_on:
                            published_at = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                        elif "Posted 30+ Days Ago" in posted_on:
                            published_at = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

                    if date_posted:
                        published_at = date_posted

                    # Check posting date if available
                    is_recent = True
                    if published_at:
                        try:
                            posted_date = datetime.strptime(published_at, "%Y-%m-%d")
                            is_recent = posted_date >= cutoff_date
                        except (ValueError, TypeError):
                            is_recent = True

                    # Skip old jobs
                    if not is_recent:
                        stats["old_jobs_skipped"] += 1
                        continue

                    stats["recent_jobs"] += 1

                    # Prepare job record
                    job_record = {
                        "job_id": job_id,
                        "company_id": company_id,
                        "job_title": job_title,
                        "job_description": job_description,
                        "job_url": job_url,
                        "location": location,
                        "time_type": time_type,
                        "employment_type": employment_type,
                        "published_at": published_at,
                        "valid_through": valid_through,
                        "is_active": True,
                        "raw_data": raw_data,
                        "partition_key": partition_key,
                        "work_type": work_type,
                        "compensation": compensation
                    }

                    if job_id in existing_jobs:
                        company_jobs_updated += 1
                        stats["updated_jobs"] += 1
                    else:
                        batch_jobs.append(job_record)
                        company_jobs_added += 1
                        stats["new_jobs_added"] += 1

                # Update company result for checkpoint
                company_result["jobs_added"] = company_jobs_added
                company_result["jobs_updated"] = company_jobs_updated

                time.sleep(config.rate_limit)

            except Exception as e:
                context.log.error(f"Error processing company {company_name}: {str(e)}")
                stats["errors"] += 1

                company_result["status"] = "error"
                company_result["error"] = str(e)

                # Add to failed companies
                failed_entry = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "partition_key": partition_key
                }
                new_failed_companies.append(failed_entry)

            checkpoint_results.append(company_result)
            stats["companies_processed"] += 1

        # Insert jobs from this batch to Snowflake
        if batch_jobs:
            try:
                jobs_df = pd.DataFrame(batch_jobs)

                # Handle data types for Snowflake
                for col in jobs_df.select_dtypes(include=['object']).columns:
                    if col not in ['published_at', 'valid_through']:
                        jobs_df[col] = jobs_df[col].fillna('').astype(str)

                # Convert date columns properly for Snowflake
                if 'published_at' in jobs_df.columns:
                    jobs_df['published_at'] = pd.to_datetime(jobs_df['published_at'], errors='coerce')
                    jobs_df['published_at'] = jobs_df['published_at'].dt.date
                    jobs_df['published_at'] = jobs_df['published_at'].where(pd.notnull(jobs_df['published_at']), None)

                if 'valid_through' in jobs_df.columns:
                    jobs_df['valid_through'] = pd.to_datetime(jobs_df['valid_through'], errors='coerce')
                    jobs_df['valid_through'] = jobs_df['valid_through'].dt.date
                    jobs_df['valid_through'] = jobs_df['valid_through'].where(pd.notnull(jobs_df['valid_through']), None)

                if 'is_active' in jobs_df.columns:
                    jobs_df['is_active'] = jobs_df['is_active'].astype(bool)

                # Use write_pandas for bulk insert
                success, num_chunks, num_rows, output = write_pandas(
                    conn,
                    jobs_df,
                    'workday_jobs',
                    database=database_name,
                    schema=schema_name,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=False
                )

                if success:
                    context.log.info(f"Loaded batch of {len(batch_jobs)} jobs into Snowflake")
                else:
                    context.log.error(f"Failed to load jobs to Snowflake: {output}")
                    stats["errors"] += 1

                # Handle updates for existing jobs
                if any(result["jobs_updated"] > 0 for result in checkpoint_results):
                    cursor = conn.cursor()
                    try:
                        for job_record in batch_jobs:
                            if job_record["job_id"] in existing_jobs:
                                update_sql = f"""
                                UPDATE {database_name}.{schema_name}.workday_jobs
                                SET
                                    job_title = %s,
                                    job_description = %s,
                                    job_url = %s,
                                    location = %s,
                                    time_type = %s,
                                    employment_type = %s,
                                    published_at = %s,
                                    valid_through = %s,
                                    date_retrieved = CURRENT_TIMESTAMP,
                                    is_active = %s,
                                    raw_data = %s,
                                    partition_key = %s,
                                    work_type = %s,
                                    compensation = %s
                                WHERE job_id = %s AND company_id = %s
                                """
                                cursor.execute(update_sql, (
                                    job_record["job_title"],
                                    job_record["job_description"],
                                    job_record["job_url"],
                                    job_record["location"],
                                    job_record["time_type"],
                                    job_record["employment_type"],
                                    job_record["published_at"],
                                    job_record["valid_through"],
                                    job_record["is_active"],
                                    job_record["raw_data"],
                                    job_record["partition_key"],
                                    job_record["work_type"],
                                    job_record["compensation"],
                                    job_record["job_id"],
                                    job_record["company_id"]
                                ))
                        conn.commit()
                    except Exception as e:
                        context.log.error(f"Error updating existing jobs: {str(e)}")
                    finally:
                        cursor.close()

            except Exception as e:
                context.log.error(f"Error loading jobs to Snowflake: {str(e)}")
                stats["errors"] += 1

        # Save checkpoint after each batch
        try:
            checkpoint_df = pd.DataFrame(checkpoint_results)
            checkpoint_df.to_csv(checkpoint_file, index=False)
        except Exception as e:
            context.log.error(f"Error saving checkpoint: {str(e)}")

        # Save failed companies if any new failures
        if new_failed_companies:
            try:
                all_failed = failed_companies + new_failed_companies
                pd.DataFrame(all_failed).to_csv(failed_companies_file, index=False)
            except Exception as e:
                context.log.error(f"Error saving failed companies: {str(e)}")

        # Report progress to Dagster
        context.log_event(
            AssetMaterialization(
                asset_key=context.asset_key,
                description=f"Processed batch {batch_num}/{total_batches}",
                metadata={
                    "companies_processed": MetadataValue.int(int(stats["companies_processed"])),
                    "total_companies": MetadataValue.int(int(stats["total_companies"])),
                    "batch": MetadataValue.int(int(batch_num)),
                    "total_batches": MetadataValue.int(int(total_batches)),
                    "jobs_found": MetadataValue.int(int(stats["total_jobs_found"])),
                    "jobs_added": MetadataValue.int(int(stats["new_jobs_added"])),
                    "jobs_updated": MetadataValue.int(int(stats["updated_jobs"])),
                    "partition_key": MetadataValue.text(partition_key)
                }
            )
        )

    # Mark inactive jobs for processed companies
    if stats["companies_with_jobs"] > 0:
        try:
            processed_company_ids = []
            for result in checkpoint_results:
                if result.get("status") == "success" and result.get("jobs_found", 0) > 0:
                    processed_company_ids.append(result.get("company_id"))

            if processed_company_ids:
                cursor = conn.cursor()
                try:
                    company_ids_placeholders = ",".join(["%s"] * len(processed_company_ids))
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    inactivate_sql = f"""
                    UPDATE {database_name}.{schema_name}.workday_jobs
                    SET is_active = FALSE
                    WHERE company_id IN ({company_ids_placeholders})
                    AND DATE(date_retrieved) < %s
                    AND is_active = TRUE
                    """
                    cursor.execute(inactivate_sql, processed_company_ids + [current_date])
                    conn.commit()
                    inactive_count = cursor.rowcount
                    context.log.info(f"Marked {inactive_count} jobs as inactive")
                    stats["jobs_marked_inactive"] = inactive_count
                except Exception as e:
                    context.log.error(f"Error marking inactive jobs: {str(e)}")
                finally:
                    cursor.close()
        except Exception as e:
            context.log.error(f"Error processing inactive jobs: {str(e)}")

    # Log final summary
    context.log.info(f"Completed partition {partition_key}: {stats['companies_processed']} companies, {stats['new_jobs_added']} new jobs")

    # Convert NumPy integers to Python integers for Dagster metadata
    for key, value in stats.items():
        if hasattr(value, 'dtype') and 'int' in str(value.dtype):
            stats[key] = int(value)

    # Update job count in Snowflake
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.workday_jobs WHERE partition_key = %s", (partition_key,))
        count_result = cursor.fetchall()
        partition_jobs = count_result[0][0]

        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.workday_jobs")
        count_result = cursor.fetchall()
        total_jobs = count_result[0][0]

        stats["jobs_in_partition"] = int(partition_jobs)
        stats["total_jobs_in_table"] = int(total_jobs)
        cursor.close()
    except Exception as e:
        context.log.error(f"Error getting job count: {str(e)}")

    conn.close()

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies": MetadataValue.int(int(stats["total_companies"])),
        "companies_processed": MetadataValue.int(int(stats["companies_processed"])),
        "companies_with_jobs": MetadataValue.int(int(stats["companies_with_jobs"])),
        "total_jobs_found": MetadataValue.int(int(stats["total_jobs_found"])),
        "new_jobs_added": MetadataValue.int(int(stats["new_jobs_added"])),
        "jobs_updated": MetadataValue.int(int(stats["updated_jobs"])),
        "partition_key": MetadataValue.text(partition_key),
        "snowflake_table": MetadataValue.text(f"{database_name}.{schema_name}.workday_jobs")
    })

    return stats

# Create a job definition for this asset
workday_jobs_discovery_job = define_asset_job(
    name="workday_jobs_discovery_job",
    selection=[workday_company_jobs_discovery],
    description="Discovers and loads job listings from Workday career sites"
)

# Create the Dagster definitions
defs = Definitions(
    assets=[workday_company_jobs_discovery],
    jobs=[workday_jobs_discovery_job]
)