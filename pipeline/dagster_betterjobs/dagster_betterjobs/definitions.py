from dagster import (
    Definitions,
    load_assets_from_modules,
    EnvVar,
    resource
)
from dagster_duckdb import DuckDBResource
from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster_gcp import BigQueryResource
from dagster_gemini import GeminiResource

from dagster_openai import OpenAIResource
from pathlib import Path
import os
import base64
import json
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from dagster_betterjobs import assets  # noqa: TID252
from dagster_betterjobs.sensors import adhoc_company_urls_sensor

from dagster_betterjobs.assets.bamboohr_jobs_discovery import (
    bamboohr_company_jobs_discovery
)
from dagster_betterjobs.assets.greenhouse_jobs_discovery import (
    greenhouse_company_jobs_discovery
)
from dagster_betterjobs.assets.smartrecruiters_jobs_discovery import (
    smartrecruiters_company_jobs_discovery
)
from dagster_betterjobs.assets.workday_jobs_discovery import (
    workday_company_jobs_discovery
)
from dagster_betterjobs.assets.raw_company_profiles import (
    raw_company_profiles
)
from dagster_betterjobs.io import BetterJobsIOManager
from dagster_betterjobs.jobs import (
    data_engineering_job,
    full_jobs_discovery_job,
    bamboohr_jobs_discovery_job,
    greenhouse_jobs_discovery_job,
    smartrecruiters_jobs_discovery_job,
    workday_jobs_discovery_job,
    full_jobs_discovery_and_search_job,
    snowflake_master_company_urls_job,
)
from dagster_betterjobs.schedules import (
    full_jobs_discovery_and_search_schedule,
)

# Import the custom PostgresResource
from dagster_betterjobs.resources import PostgresResource, SnowflakeResource


@resource
def bigquery_client_resource(context):
    """Creates a BigQuery client using environment credentials."""
    project_id = os.environ.get('GCP_PROJECT_ID')
    gcp_credentials_b64 = os.environ.get('GCP_CREDENTIALS')

    if not project_id or not gcp_credentials_b64:
        raise ValueError("Missing required environment variables: GCP_PROJECT_ID, GCP_CREDENTIALS")

    try:
        credentials_json = base64.b64decode(gcp_credentials_b64).decode("utf-8")
        credentials_info = json.loads(credentials_json)
        credentials = Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client
    except Exception as e:
        context.log.error(f"Failed to create BigQuery client: {str(e)}")
        raise


# Load assets from modules
all_assets = load_assets_from_modules([assets])

# Resolve database path based on execution directory
current_dir = Path(os.getcwd())
if current_dir.name == "dagster_betterjobs" and "pipeline" in str(current_dir):
    db_path = Path("dagster_betterjobs/db/betterjobs.db")
else:
    db_path = Path("pipeline/dagster_betterjobs/dagster_betterjobs/db/betterjobs.db")

# Ensure database directory exists
db_dir = db_path.parent
os.makedirs(db_dir, exist_ok=True)

# Configure resources
resources = {
    "duckdb_resource": DuckDBResource(
        database=str(db_path)
    ),
    "gemini": GeminiResource(
        api_key=EnvVar("GEMINI_API_KEY"),
        generative_model_name="gemini-2.5-flash-preview-04-17",
    ),
    "openai": OpenAIResource(
        api_key=EnvVar("OPENAI_API_KEY"),
        model=""
    ),
    "duckdb": BetterJobsIOManager(database=str(db_path)),
    "bigquery": bigquery_client_resource,
    "bigquery_io": BigQueryPandasIOManager(
        project=EnvVar("GCP_PROJECT_ID"),
        dataset=EnvVar("GCP_DATASET_ID"),
        location=EnvVar("GCP_LOCATION"),
        timeout=15.0,
        gcp_credentials=EnvVar("GCP_CREDENTIALS")
    ),
    "supabase_postgres": PostgresResource(
        host=EnvVar("SUPABASE_HOST"),
        port=6543,
        user=EnvVar("SUPABASE_USER"),
        password=EnvVar("SUPABASE_PASSWORD"),
        dbname=EnvVar("SUPABASE_DB"),
        sslmode="require"
    ),
    "snowflake": SnowflakeResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=EnvVar("SNOWFLAKE_RAW_SCHEMA"),
        role=EnvVar("SNOWFLAKE_ROLE")
    ),
}

# Verify presence of required environment variables
print("GCP_CREDENTIALS present:", bool(os.getenv("GCP_CREDENTIALS")))
print("GCP_PROJECT_ID present:", bool(os.getenv("GCP_PROJECT_ID")))
print("GCP_DATASET_ID present:", bool(os.getenv("GCP_DATASET_ID")))
print("GCP_LOCATION present:", bool(os.getenv("GCP_LOCATION")))
print("SUPABASE_HOST present:", bool(os.getenv("SUPABASE_HOST")))
print("SUPABASE_USER present:", bool(os.getenv("SUPABASE_USER")))
print("SUPABASE_PASSWORD present:", bool(os.getenv("SUPABASE_PASSWORD")))
print("SNOWFLAKE_ACCOUNT present:", bool(os.getenv("SNOWFLAKE_ACCOUNT")))
print("SNOWFLAKE_USER present:", bool(os.getenv("SNOWFLAKE_USER")))
print("SNOWFLAKE_PASSWORD present:", bool(os.getenv("SNOWFLAKE_PASSWORD")))
print("S3_URI present:", bool(os.getenv("S3_URI")))
print("MAIN_INPUT_FOLDER present:", bool(os.getenv("MAIN_INPUT_FOLDER")))

# Define Dagster application
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[
        data_engineering_job,
        full_jobs_discovery_job,
        bamboohr_jobs_discovery_job,
        greenhouse_jobs_discovery_job,
        smartrecruiters_jobs_discovery_job,
        workday_jobs_discovery_job,
        full_jobs_discovery_and_search_job,
        snowflake_master_company_urls_job,
    ],
    schedules=[
        full_jobs_discovery_and_search_schedule,
    ],
    sensors=[
        adhoc_company_urls_sensor,
    ],
)
