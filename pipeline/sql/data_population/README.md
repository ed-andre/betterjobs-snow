# Static Data Population Scripts

This directory contains SQL scripts for populating reference and mapping tables used in the BetterJobs data pipeline. These scripts ensure consistent data normalization and standardization across the platform.

## Purpose

**Static data population is managed separately from Dagster assets to:**
- ✅ Enable easy rule updates without code changes
- ✅ Maintain clear separation of data from logic
- ✅ Support database-driven configuration
- ✅ Allow version control of business rules
- ✅ Facilitate team collaboration on data rules

## File Structure

```
pipeline/sql/data_population/
├── README.md                                    # This file
├── data_population_order.yaml                  # Execution order configuration
│
├─── Foundation Data ─────
├── insert_countries_mapping.sql                # World countries and territories
├── insert_us_states_mapping.sql               # US states and territories
│
├─── Location Mappings ───
├── insert_location_metro_area_mapping.sql     # Metropolitan area classifications
├── insert_location_region_mapping.sql         # Regional groupings (US regions, etc.)
├── insert_location_tech_hub_mapping.sql       # Technology hub classifications
│
├─── Standardization Rules ───
├── insert_experience_standardization_rules.sql # Experience level normalization
├── insert_location_standardization_rules.sql  # Location name standardization
├── insert_keyword_standardization_rules.sql   # Job keyword standardization
│
└─── Complex Mappings ───
    └── insert_keyword_type_mappings.sql       # Keyword type classifications
```

## Execution Order

Scripts are executed in dependency order as defined in `data_population_order.yaml`:

1. **Foundation Data** - Core reference tables (countries, states)
2. **Location Mappings** - Geographic classifications and groupings
3. **Standardization Rules** - Data normalization rules
4. **Complex Mappings** - Advanced type classifications

## Adding New Data Population Scripts

### 1. Create the SQL File

Create a new SQL file in this directory following the naming convention:

```sql
-- Example: insert_new_mapping_table.sql
INSERT INTO STAGE.NEW_MAPPING_TABLE
    (ID, NAME, CATEGORY, CREATED_DATE)
VALUES
    ('001', 'Example Entry', 'sample_category', CURRENT_TIMESTAMP),
    ('002', 'Another Entry', 'sample_category', CURRENT_TIMESTAMP);
```

### 2. Update Configuration

Add your new file to `data_population_order.yaml` in the appropriate dependency group:

```yaml
# Add to the correct section based on dependencies
standardization_rules:
  - insert_experience_standardization_rules.sql
  - insert_location_standardization_rules.sql
  - insert_keyword_standardization_rules.sql
  - insert_new_mapping_table.sql  # Your new file
```

### 3. Test the Script

Verify your script works by running it individually in Snowflake before deploying.

## Best Practices

### 1. File Naming Convention
- Use descriptive names starting with `insert_`
- Group related data by prefix (e.g., `insert_location_*`)
- Include table purpose in filename

### 2. Data Structure Guidelines
- Include `CREATED_DATE` or `UPDATED_DATE` timestamps
- Use consistent ID patterns and data types
- Add meaningful comments in SQL files
- Test with small datasets first

### 3. Dependency Management
- Place files in correct YAML section based on dependencies
- Foundation data should have no dependencies
- Complex mappings should reference simpler tables
- Avoid circular dependencies

### 4. Version Control
- Commit all changes to git with descriptive messages
- Test scripts before committing
- Review changes with team for business rule updates

## Current Data Categories

### Foundation Data
- **Countries Mapping**: International countries with official names and ISO codes
- **US States Mapping**: All 50 US states plus territories for location parsing

### Location Intelligence
- **Metro Area Mapping**: Major metropolitan area classifications
- **Region Mapping**: Geographic regional groupings (US regions, international zones)
- **Tech Hub Mapping**: Technology center classifications for market analysis
- **Location Standardization**: Rules for normalizing location names and formats

### Job Data Standardization
- **Experience Rules**: Standardizes experience levels (entry, mid, senior, etc.)
- **Keyword Rules**: Normalizes job-related keywords and terminology
- **Keyword Type Mapping**: Classifies keywords by type and category

## Key Benefits

This data population approach provides:
- 🎯 **Clean separation** between static data and application logic
- 🔄 **Easy maintenance** of rules without code changes
- 📊 **Database-driven** configuration that can be queried and analyzed
- 🚀 **Scalable** approach that supports growing rule sets
- 🔒 **Version controlled** rule changes with audit trails
- 🌍 **Comprehensive location parsing** for worldwide coverage