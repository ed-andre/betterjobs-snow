# BetterJobs-Snow

BetterJobs-Snow is a comprehensive job market analytics project that retrieves job postings directly from company career portals across various Applicant Tracking Systems (ATS) such as Workday, Greenhouse, BambooHR, and more. The goal of this project is to analyze the job market for trends, hiring patterns, and industry insights.

## ⚠️ Disclaimer

**This is a personal work in progress project.**

- This project is not officially supported
- Use at your own risk
- No warranty or support is provided
- The codebase may change significantly without notice
- Use responsibly and for personal research only

## Project Architecture

The project consists of a Dagster data pipeline implementing a medallion architecture (Bronze, Silver, Gold) for retrieving, processing, and storing job data in Snowflake for analytics and reporting.

### Data Flow Architecture

```
CSV Data Sources (Company URLs with verified ATS links)
         ↓
   CSV Ingestion (Bronze Layer)
         ↓
    Job Discovery Jobs
         ↓
   Data Transformation (Silver/Gold Layers)
         ↓
   Snowflake Storage
         ↓
Analytics & Reporting
         ↓
  Business Intelligence
```

### Pipeline Components
- **CSV Ingestion**: Processes CSV files containing company information with verified ATS URLs from S3 and local sources
- **Adhoc Processing**: Sensor-based ingestion for additional CSV files as needed
- **Job Discovery**: Extracts job listings from company sites, varies by ATS platform
- **Data Transformation**: Medallion architecture layers for data quality and enrichment (Bronze → Silver → Gold)
- **Data Storage**: Stores job and company data in Snowflake for analytics

**Note**: The initial CSV files with verified ATS URLs were generated from a previous version of this project that included automated URL discovery and extraction processes.

### Job Search & Reporting

The pipeline includes an advanced job search capability that generates modern, interactive HTML reports for analyzing job market data. The search functionality leverages the cleaned and enriched STAGE data to provide high-quality results with advanced filtering capabilities.

#### Features:
- **Interactive Filtering**: Client-side filtering by keywords, job titles, locations, and platforms
- **Quality Scoring**: Filter jobs by data quality scores and language detection confidence
- **Modern UI**: Responsive design with professional styling suitable for stakeholder presentations
- **Smart Deduplication**: Cross-platform duplicate removal ensures unique job listings
- **Shareable URLs**: Bookmark and share filtered search results with URL parameters
- **Real-time Updates**: Dynamic job counts and filter states update instantly

![Job Search HTML Report](media/searchjobhtmlreport.png)

The job search reports show:
- **Header Stats**: Total results, platforms searched, date range, and data freshness
- **Filter Panel**: Interactive tags for keywords, job titles, locations, and platforms
- **Job Cards**: Clean, professional job listing cards with relevance and quality scores
- **Detailed Information**: Company details, posting dates, locations, and full job descriptions

### Dagster Pipeline Architecture (IN DEVELOPMENT)

The following diagrams show the current structure of our evolving Dagster pipeline, implementing a medallion architecture with parallel processing and AI-powered enrichment. **This is an active work in progress** with additional assets and a complete Gold layer coming soon.

#### RAW (Bronze) Layer Pipeline ✅ **COMPLETED**
![RAW Layer Pipeline](media/1-raw_dagsterpipeline.png)

The RAW layer handles data ingestion and initial discovery:
- **Company URL Management**: Processes master company data from S3 and local sources
- **Parallel Job Discovery**: Individual platform assets for BambooHR, Greenhouse, Workday, and SmartRecruiters discovery
- **Company Profile Extraction**: Automated company information gathering
- **Sensor-Based Processing**: Adhoc company processing with automatic detection

#### STAGE (Silver) Layer Pipeline 🚧 **IN PROGRESS**
![STAGE Layer Pipeline](media/2-stage_dagsterpipeline.png)

The STAGE layer provides comprehensive data transformation and AI enrichment:
- **Platform-Specific Processing**: Individual stage assets for each ATS platform enable parallel processing
- **Unified Data Integration**: Cross-platform validation and deduplication in `stage_jobs_unified`
- **AI-Powered Enrichment**: Parallel LLM processing for salary extraction, skills analysis, and job classification
- **Quality Validation**: Comprehensive data quality scoring and language detection
- **Enhanced Job Search**: Modern HTML report generation with interactive filtering capabilities

##### LLM Data Standardization Pipeline 🔄 **ACTIVE DEVELOPMENT**
![STAGE Standardization Pipeline](media/3-stage_standardization_dagsterpipeline.png)

The LLM Standardization group transforms AI-extracted VARIANT/JSON data into normalized relational structures:
- **Skills Normalization** ✅ **COMPLETED**: 8,302 skills standardized with 149,477 job-skill relationships and family classification (as of June 11, 2025)
- **Keywords Standardization** ✅ **COMPLETED**: 2,847 keywords normalized with industry and role type classifications (as of June 11, 2025)
- **Location Standardization** 🚧 **COMPLETED**: Geographic hierarchy and tech hub classification with work arrangement context
- **Data Quality Validation** 📋 **PLANNED**: Comprehensive quality monitoring, anomaly detection, and automated alerting
- **Analytics Enablement** 📋 **PLANNED**: Pre-aggregated views and performance optimization for downstream analytics

**Current Progress:**
- **Phase 1 & 2 Complete**: Skills and keywords fully normalized with 97.6% job coverage
- **Confidence Scoring**: Advanced confidence tracking with manual review flagging for low-confidence items
- **Bridge Tables**: Many-to-many relationships with source tracking and context classification
- **Phase 3**: Location standardization with geographic enrichment and remote work indicators

**Upcoming Phases:**
- **Phase 4**: Automated data quality monitoring with trend analysis and anomaly detection
- **Phase 5**: Analytics-optimized views and materialized tables for Gold layer integration

#### GOLD (Presentation) Layer Pipeline 🔮 **COMING SOON**
*Diagram will be added as assets are developed*

The planned GOLD layer will provide business-ready analytics and reporting:
- **Aggregated Job Market Metrics**: Daily, weekly, and monthly job posting trends
- **Skills & Salary Analytics**: Market rates, in-demand skills, and compensation benchmarks
- **Geographic Intelligence**: Location-based job market insights and remote work trends
- **Industry Analysis**: Sector-specific hiring patterns and emerging job categories

#### Current Architecture Benefits:
- **Parallel Processing**: 4-5x performance improvement through platform-specific assets
- **Failure Isolation**: Individual platform failures don't affect others
- **Resilient Processing**: Individual record error handling prevents batch failures
- **AI Integration**: Gemini LLM extraction for structured job information
- **Production Ready**: Comprehensive monitoring, error tracking, and quality validation

#### Upcoming Enhancements:
- **Additional ATS Platforms**: Lever, Jobvite, iCIMS integration
- **Advanced Analytics**: Predictive hiring models and market forecasting
- **API Endpoints**: External access to processed job market data

### Analytics & Reporting
- **Job Market Trends**: Track hiring patterns across industries and companies
- **Company Analysis**: Monitor job posting frequency and patterns by company
- **Geographic Distribution**: Analyze job opportunities by location
- **Skills & Requirements**: Extract insights from job descriptions and requirements

## Getting Started

### Prerequisites

- Python (v3.10+)
- Docker (optional, for containerized deployment)
- Dagster
- Access to the following external services:
  - Snowflake
  - AWS S3
  - Google AI Gemini API


### External Service Setup

#### 1. Snowflake
- Create a Snowflake account and warehouse
- Create a database (e.g., `BETTERJOBS_DB`)
- Create schemas for raw data (`RAW`) and processed data (`PROCESSED`)
- Create a user with appropriate permissions
- Note your account identifier, warehouse, and database details

**For detailed Snowflake setup instructions, see: [SNOWFLAKE_SETUP.md](pipeline/docs/setup/SNOWFLAKE_SETUP.md)**

**For S3-Snowflake integration setup, see: [S3_SNOWFLAKE_SETUP.md](pipeline/docs/setup/S3_SNOWFLAKE_SETUP.md)**

#### 2. Gemini API
- Set up Google AI Studio account
- Generate an API key for Gemini

### Environment Configuration

Create a `.env` file in the project root with the following variables:

```
# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=BETTERJOBS_DB
SNOWFLAKE_RAW_SCHEMA=RAW
SNOWFLAKE_PROCESSED_SCHEMA=PROCESSED
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role

# AI APIs
GEMINI_API_KEY=your_gemini_api_key
OPENAI_API_KEY=your_openai_api_key
```

### Installation

1. Clone the repository
```bash
git clone [repository URL]
cd betterjobs
```

2. Set up the Dagster pipeline
```bash
cd pipeline/dagster_betterjobs
pip install -e .
```

The `setup.py` file includes all necessary dependencies including:
- Dagster and related packages
- Snowflake connector
- Web discovery tools (BeautifulSoup, lxml)
- Gemini AI integration

### Running the Pipeline

Start the Dagster pipeline:
```bash
cd pipeline/dagster_betterjobs
dagster dev
```

The Dagster UI will be available at `http://localhost:3000` where you can run jobs, materialize assets, and monitor the pipeline.

## Pipeline Jobs

The following Dagster jobs are available to run:




- **Master Company URLs Job**:
  - `snowflake_master_company_urls_job`: Process and maintain master company URLs in Snowflake from S3 and local CSV sources

- **Job Position Discovery Jobs**:
  - `bamboohr_jobs_discovery_job`: Find BambooHR jobs
  - `greenhouse_jobs_discovery_job`: Find Greenhouse jobs
  - `workday_jobs_discovery_job`: Find Workday jobs
  - `smartrecruiters_jobs_discovery_job`: Find SmartRecruiters jobs

- **Data Engineering Jobs**:
  - `data_engineering_job`: Run job search for data engineering positions

- **End-to-End Jobs**:
  - `full_jobs_discovery_and_search_job`: Run all job discovery, and data_engineering job


See the Dagster UI for the complete list of available jobs and their descriptions.

## Usage Workflow

### Adding Company Data Sources

1. For each supported ATS platform, create or update a CSV file named `[ats-name]_companies.csv` under the `pipeline/dagster_betterjobs/dagster_betterjobs/data_load/datasource` directory with the following headers:

```
company_name,company_industry,employee_count_range,city,platform
```

Example for Workday:
```
company_name,company_industry,employee_count_range,city,platform
Acme Corp,Technology,1001-5000,San Francisco,workday
Widget Inc,Manufacturing,5001-10000,Chicago,workday
```

Supported platforms include:
- workday
- greenhouse
- bamboohr
- smartrecruiters
- lever (job discovery not implemented yet)
- jobvite (job discovery not implemented yet)
- icims (job discovery not properly implemented yet. The response from icims sites is a bit on the complex site. Will revisit in the future)



### Asset Materialization Workflows

There are two main approaches to running the pipeline:

#### Option 1: Full Asset Materialization

To materialize all assets in the pipeline:

```bash
cd pipeline/dagster_betterjobs
python -m dagster asset materialize
```

This approach is useful for initial setup or when adding many new companies.

#### Option 2: Adhoc Company Processing

For quickly adding or updating specific companies:

1. Materialize the snowflake_master_company_urls asset:
```bash
cd pipeline/dagster_betterjobs
python -m dagster asset materialize -a snowflake_master_company_urls
```

2. Add company entries to `pipeline/dagster_betterjobs/input/adhoc_companies.csv` with the following format:
```
company_name,company_industry,platform,ats_url,career_url,url_verified
Acme Corp,Technology,workday,https://acme.wd1.myworkdayjobs.com/acme_careers/,https://www.acme.com/careers,True
```

3. The adhoc_company_urls_sensor will automatically detect changes to this file and trigger the relevant job discovery pipelines.

### Scheduling Jobs

For automated use, you should configure appropriate schedules:

1. The snowflake_master_company_urls_job only needs to run when you update the datasource CSV files with new companies.

2. The job discovery assets need to run frequently to find new job postings.

To modify the built-in schedules or create new ones:

1. Edit `pipeline/dagster_betterjobs/dagster_betterjobs/schedules.py` to adjust:
   - Run frequency (cron schedule)
   - Assets to materialize
   - Execution parameters

Example configuration:
```python
@schedule(
    cron_schedule="0 */4 * * *",  # Every 4 hours
    job=data_engineering_job,
    execution_timezone="America/New_York",
)
def jobs_every_four_hours_schedule():
    return RunRequest(
        run_key=None,
        run_config={},
        tags={"schedule": "jobs_every_four_hours"},
    )
```

2. Add your new schedule to the `definitions.py` file in the schedules list.

3. When running Dagster, your schedule will appear in the UI where you can turn it on.

### Best Practices for Resource Usage

- Snowflake master company urls job should be run when you update the CSV files with new companies on S3 or locally
- Job discovery jobs should be run more frequently (daily or multiple times daily)
- Some ATS platforms have rate limits - avoid running jobs too frequently

## Data Analytics & Reporting

### Snowflake Tables

The pipeline creates and maintains several key tables in Snowflake:

- **master_company_urls**: Company information and career site URLs
- **workday_jobs**: Job listings from Workday platforms
- **greenhouse_jobs**: Job listings from Greenhouse platforms
- **bamboohr_jobs**: Job listings from BambooHR platforms
- **smartrecruiters_jobs**: Job listings from SmartRecruiters platforms

### Sample Analytics Queries

**Job Posting Trends by Platform:**
```sql
SELECT
    platform,
    DATE_TRUNC('week', date_posted) as week,
    COUNT(*) as jobs_posted
FROM (
    SELECT 'workday' as platform, date_posted FROM workday_jobs WHERE is_active = TRUE
    UNION ALL
    SELECT 'greenhouse' as platform, date_posted FROM greenhouse_jobs WHERE is_active = TRUE
    UNION ALL
    SELECT 'bamboohr' as platform, date_posted FROM bamboohr_jobs WHERE is_active = TRUE
)
GROUP BY platform, week
ORDER BY week DESC;
```

**Top Companies by Job Volume:**
```sql
SELECT
    c.company_name,
    c.company_industry,
    COUNT(*) as total_jobs
FROM master_company_urls c
JOIN workday_jobs j ON c.company_id = j.company_id
WHERE j.is_active = TRUE
GROUP BY c.company_name, c.company_industry
ORDER BY total_jobs DESC
LIMIT 20;
```

### Business Intelligence Integration

The Snowflake data warehouse can be connected to various BI tools:

- **Tableau**: Connect directly to Snowflake for interactive dashboards
- **Power BI**: Use Snowflake connector for real-time reporting
- **Looker**: Create data models and exploration interfaces
- **Metabase**: Create data models and exploration interfaces

## Features

- **Efficient Job Discovery**: Find jobs faster than traditional job search engines
- **Multi-platform Coverage**: Support for major ATS platforms (Workday, Greenhouse, BambooHR, etc.)
- **Comprehensive Analytics**: Track job market trends, company hiring patterns, and industry insights
- **Scalable Architecture**: Handle large volumes of job data with Snowflake's cloud data platform and Gemini AI
- **Automated Processing**: Schedule regular job discovery and data updates
- **Data Quality**: Built-in validation and deduplication of job listings

## Migration Notes

This project has been migrated from BigQuery to Snowflake to provide:
- Better performance for analytics workloads
- More cost-effective data storage and compute
- Enhanced support for semi-structured data
- Improved integration with modern BI tools
- Better separation of compute and storage

## Contributing

This is a personal project and not actively seeking contributions. However, feel free to fork the repository if you find it useful.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

Built as a personal project for exploring job market data and modern data engineering technologies.
