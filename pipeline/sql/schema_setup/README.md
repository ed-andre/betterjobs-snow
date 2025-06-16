# BetterJobs Database Setup - Automated Infrastructure

This directory contains the automated setup for the BetterJobs Snowflake database following the medallion architecture pattern.

## 🎯 **Automated Setup Process**

✅ **Complete automation** - Dagster assets execute all setup automatically
✅ **Environment consistency** - Identical setup across all environments
✅ **Infrastructure as Code** - All database objects version controlled
✅ **15-minute setup** - Complete environment ready in under 15 minutes

## 🏗️ **Database Architecture**

### **Medallion Architecture (Bronze → Silver → Gold)**

| Schema | Layer | Purpose | Contents |
|--------|-------|---------|----------|
| `RAW` | Bronze | Data ingestion | Raw tables from ATS platforms, S3 stages |
| `STAGE` | Silver | Cleaned data | Unified jobs, LLM enrichment, normalized skills/keywords |
| `ANALYTICS` | Gold | Business analytics | Dimensional models, aggregates, reporting tables |

## 🔧 **Prerequisites**

### **Snowflake Requirements:**
- Snowflake account with `ACCOUNTADMIN` permissions
- Warehouse named `BETTERJOBS_WH`
- Dagster environment configured with SnowflakeResource

### **S3 Integration:**
- AWS IAM role configured for Snowflake access
- S3 bucket: `s3://betterjobs-dagster/`

## 🚀 **Setup Instructions**

### **Complete Environment Setup**
```bash
# Run complete infrastructure setup (recommended)
dagster asset materialize --select "infrastructure_setup*"
```

### **Step-by-Step Setup**
```bash
# Option: Run individual components
dagster asset materialize --select "database_schema_setup"
dagster asset materialize --select "raw_schema_setup"
dagster asset materialize --select "stage_schema_setup"
dagster asset materialize --select "analytics_schema_setup"
```

### **Setup Validation**
```bash
# Verify setup completed successfully
dagster asset materialize --select "validate_setup"
```

## 📊 **Verification**

After setup completion, verify your environment:

```sql
-- Check database and schemas exist
SHOW DATABASES LIKE 'BETTERJOBS_DB';
SHOW SCHEMAS IN DATABASE BETTERJOBS_DB;

-- Check tables were created
USE DATABASE BETTERJOBS_DB;
USE SCHEMA RAW;
SHOW TABLES;

USE SCHEMA STAGE;
SHOW TABLES;
SHOW VIEWS;

-- Verify permissions
SHOW GRANTS TO ROLE BETTERJOBS_ROLE;
```

## 🛡️ **Security & Permissions**

The setup creates:
- **`BETTERJOBS_ROLE`**: Application role with required permissions
- **Secure access**: Minimal permissions following security best practices
- **Future objects**: Automatic permissions for new tables/views

## 🚨 **Troubleshooting**

### **Setup Issues:**

**Check Setup Status:**
```bash
# View infrastructure setup status
dagster asset list --group infrastructure_setup
```

**Re-run Failed Components:**
```bash
# Re-run specific failed component
dagster asset materialize --select "failed_component_name"
```

**Common Issues:**

**Permission Errors:**
- Verify Snowflake credentials in Dagster configuration
- Ensure `ACCOUNTADMIN` role access for initial setup
- Check AWS IAM roles for S3 integration

**Connection Issues:**
- Verify Snowflake account details in environment variables
- Check network connectivity to Snowflake
- Validate warehouse `BETTERJOBS_WH` exists

**S3 Integration Issues:**
- Verify AWS IAM role configuration
- Check S3 bucket `betterjobs-dagster` access
- Review storage integration setup in asset logs

## ⚡ **Environment Updates**

### **Updating Your Environment:**
```bash
# Update environment with latest changes
dagster asset materialize --select "infrastructure_setup*" --force-materialize
```

### **Schema Changes:**
The infrastructure automatically updates when you materialize the setup assets with the latest SQL definitions.

## ✅ **Success Indicators**

Your setup is complete when:
- ✅ All infrastructure assets show "Materialized" status in Dagster
- ✅ Database `BETTERJOBS_DB` with schemas `RAW`, `STAGE`, `ANALYTICS` exist
- ✅ Tables and views are created in each schema
- ✅ Role `BETTERJOBS_ROLE` has appropriate permissions
- ✅ S3 stages are configured for data ingestion

## 📞 **Next Steps**

After successful setup:

1. **Data Pipeline**: Your BetterJobs data pipeline is ready to run
2. **Monitoring**: Check Dagster UI for pipeline execution status
3. **Data Ingestion**: S3 stages are ready for data loading
4. **Analytics**: ANALYTICS schema ready for business reporting

## 🆘 **Support**

If you encounter issues:
1. Check the Dagster UI for detailed error logs
2. Verify all prerequisites are met
3. Review the troubleshooting section above
4. Contact the data team for assistance