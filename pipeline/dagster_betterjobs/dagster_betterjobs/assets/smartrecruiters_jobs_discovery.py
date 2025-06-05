import time
import json
import os
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

from dagster_betterjobs.scrapers.smartrecruiters_scraper import SmartRecruitersJobScraper

logger = get_dagster_logger()

# Partition companies alphabetically A-Z + numeric + other
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

class SmartRecruitersJobsDiscoveryConfig(Config):
    """Configuration parameters for SmartRecruiters job discovery."""
    max_companies: Optional[int] = None  # Optional limit on processed companies
    rate_limit: float = 2.0  # Seconds between requests
    max_retries: int = 3
    retry_delay: int = 2
    min_company_id: Optional[int] = None  # For batch processing
    max_company_id: Optional[int] = None  # For batch processing
    days_to_look_back: int = 7  # Job freshness threshold in days
    batch_size: int = 10  # Companies per batch before committing
    process_all_companies: bool = True  # By default, process all companies regardless of checkpoint status

@asset(
    group_name="job_discovery",
    kinds={"API", "snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=["snowflake_master_company_urls"],
    partitions_def=alpha_partitions
)
def smartrecruiters_company_jobs_discovery(context: AssetExecutionContext, config: SmartRecruitersJobsDiscoveryConfig) -> Dict:
    """
    Discovers and stores job listings from SmartRecruiters career sites.

    Processes companies partitioned by first letter of company name,
    retrieves all current job listings using HTML parsing, and stores them in Snowflake.
    """
    # Initialize Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
    schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    partition_key = context.partition_key

    # Set job freshness cutoff date
    cutoff_date = datetime.now() - timedelta(days=config.days_to_look_back)
    # Make cutoff_date timezone-aware with UTC timezone
    cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    context.log.info(f"Processing company name partition {partition_key}")
    context.log.info(f"Only processing jobs posted after {cutoff_str}")

    # Resolve checkpoint directory path
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Configure partition-specific checkpoint files
    checkpoint_file = checkpoint_dir / f"smartrecruiters_url_discovery_{partition_key}_checkpoint.csv"
    failed_companies_file = checkpoint_dir / f"smartrecruiters_url_discovery_{partition_key}_failed.csv"

    context.log.info(f"Using checkpoint file: {checkpoint_file}")

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
    WHERE platform = 'smartrecruiters'
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

    # Apply limit if specified in config
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
    context.log.info(f"Found {total_companies} SmartRecruiters companies in partition {partition_key} to process")

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
            context.log.info(f"Loaded {len(processed_company_ids)} previously processed companies from checkpoint")

            # Update stats from checkpoint
            stats["companies_processed"] = len(processed_company_ids)
            if "jobs_found" in checkpoint_df.columns:
                stats["total_jobs_found"] = checkpoint_df["jobs_found"].sum()
            if "jobs_added" in checkpoint_df.columns:
                stats["new_jobs_added"] = checkpoint_df["jobs_added"].sum()
            if "jobs_updated" in checkpoint_df.columns:
                stats["updated_jobs"] = checkpoint_df["jobs_updated"].sum()

        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Skip already processed companies if configured to do so
    if not config.process_all_companies and checkpoint_file.exists():
        companies_to_process = companies_df[~companies_df["company_id"].astype(str).isin(processed_company_ids)]
        context.log.info(f"{len(companies_to_process)} SmartRecruiters companies remaining to process after skipping processed ones")
    else:
        # Process all companies, even those previously processed
        companies_to_process = companies_df
        context.log.info(f"Processing all {len(companies_to_process)} SmartRecruiters companies in this partition")

    # Load previously failed companies
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
            context.log.info(f"Loaded {len(failed_companies)} previously failed companies")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Create jobs table if it doesn't exist
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {database_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS smartrecruiters_jobs (
            job_id STRING,
            company_id STRING,
            job_title STRING,
            job_description STRING,
            job_url STRING,
            location STRING,
            department STRING,
            published_at DATE,
            requisition_id STRING,
            date_retrieved TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN,
            raw_data STRING,
            partition_key STRING
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        context.log.info("Created or verified smartrecruiters_jobs table in Snowflake")
    except Exception as e:
        context.log.error(f"Error creating jobs table: {str(e)}")
        return {"error": str(e), "status": "failed"}
    finally:
        cursor.close()

    # Process companies in batches
    batch_size = config.batch_size
    new_failed_companies = []

    # Process companies in batches
    for i in range(0, len(companies_to_process), batch_size):
        batch = companies_to_process.iloc[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(companies_to_process) + batch_size - 1) // batch_size
        context.log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")

        # List to track jobs from this batch
        batch_jobs = []

        # Process each company in batch
        for _, company in batch.iterrows():
            company_id = company["company_id"]
            company_name = company["company_name"]
            career_url = company["career_url"]
            ats_url = company["ats_url"]

            context.log.info(f"Processing jobs for {company_name} (ID: {company_id})")

            # Use ATS URL if available, otherwise fall back to career URL
            url_to_use = ats_url if ats_url else career_url
            context.log.info(f"Using URL: {url_to_use}")

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
                # Create SmartRecruiters scraper
                scraper = SmartRecruitersJobScraper(
                    career_url=url_to_use,
                    rate_limit=config.rate_limit,
                    max_retries=config.max_retries,
                    retry_delay=config.retry_delay,
                    dagster_log=context.log,  # Pass Dagster logger to the scraper
                    cutoff_date=cutoff_date  # Pass the cutoff date to the scraper
                )

                # Get all job listings
                job_listings = scraper.search_jobs()

                if not job_listings:
                    context.log.info(f"No jobs found for {company_name}")
                    checkpoint_results.append(company_result)
                    continue

                context.log.info(f"Found {len(job_listings)} jobs for {company_name}")
                company_result["jobs_found"] = len(job_listings)
                stats["companies_with_jobs"] += 1
                stats["total_jobs_found"] += len(job_listings)

                # Track jobs found for this company
                company_jobs_added = 0
                company_jobs_updated = 0

                # Process each job listing
                for job in job_listings:
                    job_id = job.get("job_id")
                    job_url = job.get("job_url")

                    if not job_id or not job_url:
                        continue

                    # Ensure job_id is always a string
                    job_id = str(job_id)
                    job["job_id"] = job_id

                    # Check if job is recent enough
                    is_recent = job.get("is_recent", True)

                    # Skip old jobs
                    if not is_recent:
                        stats["old_jobs_skipped"] += 1
                        continue

                    stats["recent_jobs"] += 1

                    # Check if job already exists in Snowflake
                    cursor = conn.cursor()
                    try:
                        check_sql = f"""
                        SELECT job_id
                        FROM {database_name}.{schema_name}.smartrecruiters_jobs
                        WHERE job_url = %s
                        """
                        cursor.execute(check_sql, (job_url,))
                        existing_job = cursor.fetchall()
                    except Exception as e:
                        context.log.error(f"Error checking if job exists: {str(e)}")
                        existing_job = []
                    finally:
                        cursor.close()

                    # Prepare job data
                    job_title = job.get("job_title", "")
                    job_description = job.get("job_description", "")

                    # Extract content (job description) if available
                    if "content" in job and not job_description:
                        job_description = job.get("content", "")

                    # Get department from job details
                    department = job.get("department")

                    # Get location string
                    location_str = job.get("location")

                    # Get dates
                    published_at = job.get("published_at")
                    requisition_id = job.get("requisition_id")

                    # Convert any structured data to JSON strings
                    raw_data = None
                    if "raw_data" in job:
                        try:
                            raw_data = json.dumps(job["raw_data"])
                        except Exception as e:
                            context.log.warning(f"Error converting raw data to JSON: {str(e)}")
                            raw_data = "{}"

                    # Prepare job record
                    job_record = {
                        "job_id": job_id,  # Now guaranteed to be a string
                        "company_id": str(company_id),  # Ensure company_id is string too
                        "job_title": job_title,
                        "job_description": job_description,
                        "job_url": job_url,
                        "location": location_str,
                        "department": department,
                        "published_at": published_at,
                        "requisition_id": str(requisition_id) if requisition_id else None,
                        "is_active": True,
                        "raw_data": raw_data,
                        "partition_key": partition_key
                    }

                    if existing_job:
                        # For existing jobs, we'll handle updates in a separate step
                        company_jobs_updated += 1
                        stats["updated_jobs"] += 1
                    else:
                        # Add to batch for insertion
                        batch_jobs.append(job_record)
                        company_jobs_added += 1
                        stats["new_jobs_added"] += 1

                # Update company result for checkpoint
                company_result["jobs_added"] = company_jobs_added
                company_result["jobs_updated"] = company_jobs_updated
                context.log.info(f"Processed {company_name}: Added {company_jobs_added}, Updated {company_jobs_updated}")

                # Add a delay between companies to avoid rate limits
                time.sleep(config.rate_limit)

            except Exception as e:
                context.log.error(f"Error processing company {company_name}: {str(e)}")
                stats["errors"] += 1

                # Update company result for checkpoint with error
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

            # Add company result to checkpoint results
            checkpoint_results.append(company_result)

            # Increment counter
            stats["companies_processed"] += 1

        # Insert jobs from this batch to Snowflake
        if batch_jobs:
            try:
                # Convert list of dicts to dataframe
                jobs_df = pd.DataFrame(batch_jobs)

                # Handle data types for Snowflake - ensure all IDs are strings
                for col in jobs_df.columns:
                    if col.endswith('_id') or col == 'job_id' or col == 'company_id':
                        jobs_df[col] = jobs_df[col].astype(str)

                # Handle other object columns
                for col in jobs_df.select_dtypes(include=['object']).columns:
                    if col not in ['published_at']:
                        jobs_df[col] = jobs_df[col].fillna('').astype(str)

                # Convert date columns properly for Snowflake
                if 'published_at' in jobs_df.columns:
                    jobs_df['published_at'] = pd.to_datetime(jobs_df['published_at'], errors='coerce')
                    jobs_df['published_at'] = jobs_df['published_at'].dt.date
                    jobs_df['published_at'] = jobs_df['published_at'].where(pd.notnull(jobs_df['published_at']), None)

                if 'is_active' in jobs_df.columns:
                    jobs_df['is_active'] = jobs_df['is_active'].astype(bool)

                # Use write_pandas for bulk insert
                success, num_chunks, num_rows, output = write_pandas(
                    conn,
                    jobs_df,
                    'smartrecruiters_jobs',
                    database=database_name,
                    schema=schema_name,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=False
                )

                if success:
                    context.log.info(f"Successfully loaded batch of {len(batch_jobs)} jobs into Snowflake")
                else:
                    context.log.error(f"Failed to load jobs to Snowflake: {output}")
                    stats["errors"] += 1

                # Handle updates for existing jobs
                cursor = conn.cursor()
                try:
                    for job_record in batch_jobs:
                        if any(result["jobs_updated"] > 0 for result in checkpoint_results[-len(batch):]):
                            update_sql = f"""
                            UPDATE {database_name}.{schema_name}.smartrecruiters_jobs
                            SET
                                job_title = %s,
                                job_description = %s,
                                location = %s,
                                department = %s,
                                published_at = %s,
                                requisition_id = %s,
                                date_retrieved = CURRENT_TIMESTAMP,
                                is_active = %s,
                                raw_data = %s,
                                partition_key = %s
                            WHERE job_url = %s
                            """
                            cursor.execute(update_sql, (
                                job_record["job_title"],
                                job_record["job_description"],
                                job_record["location"],
                                job_record["department"],
                                job_record["published_at"],
                                job_record["requisition_id"],
                                job_record["is_active"],
                                job_record["raw_data"],
                                job_record["partition_key"],
                                job_record["job_url"]
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
            context.log.info(f"Updated checkpoint with {len(checkpoint_results)} companies")
        except Exception as e:
            context.log.error(f"Error saving checkpoint: {str(e)}")

        # Save failed companies if any new failures
        if new_failed_companies:
            try:
                all_failed = failed_companies + new_failed_companies
                pd.DataFrame(all_failed).to_csv(failed_companies_file, index=False)
                context.log.info(f"Updated failed companies list with {len(new_failed_companies)} new entries")
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

    # Log summary stats
    context.log.info(f"SmartRecruiters job discovery complete for partition {partition_key}. Stats:")
    context.log.info(f"Total companies: {stats['total_companies']}")
    context.log.info(f"Companies processed: {stats['companies_processed']}")
    context.log.info(f"Companies with jobs: {stats['companies_with_jobs']}")
    context.log.info(f"Total jobs found: {stats['total_jobs_found']}")
    context.log.info(f"New jobs added: {stats['new_jobs_added']}")
    context.log.info(f"Jobs updated: {stats['updated_jobs']}")
    context.log.info(f"Recent jobs: {stats['recent_jobs']}")
    context.log.info(f"Old jobs skipped: {stats['old_jobs_skipped']}")
    context.log.info(f"Errors: {stats['errors']}")

    # Convert any NumPy integers to Python integers
    # This is needed because Dagster's MetadataValue.int() doesn't accept NumPy types
    for key, value in stats.items():
        if hasattr(value, 'dtype') and 'int' in str(value.dtype):
            stats[key] = int(value)
        elif isinstance(value, (list, dict)):
            # If we have nested structures, convert those too
            context.log.info(f"Converting complex stat: {key}, type: {type(value)}")

    # Update job count in Snowflake
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.smartrecruiters_jobs WHERE partition_key = %s", (partition_key,))
        count_result = cursor.fetchall()
        partition_jobs = count_result[0][0]
        context.log.info(f"Jobs in Snowflake table for partition {partition_key}: {partition_jobs}")

        # Get total job count too
        cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.smartrecruiters_jobs")
        count_result = cursor.fetchall()
        total_jobs = count_result[0][0]
        context.log.info(f"Total jobs in Snowflake table: {total_jobs}")

        # Add to stats - ensure they're Python ints
        stats["jobs_in_partition"] = int(partition_jobs)
        stats["total_jobs_in_table"] = int(total_jobs)
        cursor.close()
    except Exception as e:
        context.log.error(f"Error getting job count: {str(e)}")

    # Close Snowflake connection
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
        "snowflake_table": MetadataValue.text(f"{database_name}.{schema_name}.smartrecruiters_jobs")
    })

    return stats

# Create a job to run for specific partitions (normally by schedule)
smartrecruiters_jobs_discovery_job = define_asset_job(
    name="smartrecruiters_jobs_discovery_job",
    selection=[smartrecruiters_company_jobs_discovery]
)

# Create a job to process all partitions at once (for backfills)
smartrecruiters_jobs_all_partitions_job = define_asset_job(
    name="smartrecruiters_jobs_all_partitions_job",
    selection=[smartrecruiters_company_jobs_discovery]
)

# Create Dagster Definitions object for deployment
defs = Definitions(
    assets=[smartrecruiters_company_jobs_discovery],
    jobs=[smartrecruiters_jobs_discovery_job, smartrecruiters_jobs_all_partitions_job]
)