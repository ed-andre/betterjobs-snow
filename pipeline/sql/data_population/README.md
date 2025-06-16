# LLM Standardization Static Data Management

This directory contains SQL files for managing static data used in the LLM standardization process. This keeps static data separate from Dagster asset logic for better maintainability and version control.

## Coverage

- **Phase 1: Skills Normalization** - Complete configuration files for skills standardization
- **Phase 2: Keywords Normalization** - Complete configuration files for keywords and classification

## Philosophy

**Static data should be managed separately from application code to:**
- ✅ Avoid conflicts when multiple developers need to modify rules
- ✅ Enable easy rule updates without code changes
- ✅ Maintain clear separation of concerns
- ✅ Support database-driven configuration
- ✅ Enable rule versioning and audit trails

## File Structure

```
pipeline/sql/llm_standardization/
├── README.md                                    # This file
│
├─── Phase 1: Skills Normalization ─────
├── insert_skill_standardization_rules.sql      # Comprehensive skill aliases and variants
├── insert_skill_category_patterns.sql          # Skill category detection patterns
├── insert_skill_family_mappings.sql            # Skill family classification mappings
│
├─── Phase 2: Keywords Normalization ───
├── insert_keyword_standardization_rules.sql    # Keyword standardization and aliases
├── insert_keyword_type_mappings.sql            # Keyword type and category classifications
│
├─── Phase 3: Location Standardization ───────────
├── insert_location_standardization_rules.sql   # Location standardization rules
├── insert_us_states_mapping.sql               # US states for automatic country inference
├── insert_countries_mapping.sql               # World countries for international location parsing
│
└── migrations/                                 # Version-controlled rule updates
    ├── 001_initial_skill_rules.sql
    ├── 002_add_microsoft_tools.sql
    ├── 003_update_confidence_scores.sql
    └── 004_add_keyword_rules.sql
```

## Standard Process

### 1. Initial Setup

Run the comprehensive rule files once during initial setup:

```sql
-- Phase 1: Skills Normalization
@insert_skill_standardization_rules.sql
@insert_skill_category_patterns.sql
@insert_skill_family_mappings.sql

-- Phase 2: Keywords Normalization
@insert_keyword_standardization_rules.sql
@insert_keyword_type_mappings.sql

-- Phase 3: Location Standardization
@insert_location_standardization_rules.sql
@insert_us_states_mapping.sql
@insert_countries_mapping.sql
```

### 2. Adding New Rules

**Option A: Direct INSERT (for small changes)**
```sql
INSERT INTO SKILL_STANDARDIZATION_RULES VALUES
('rule_new_001', 'rust', 'Rust', 'languages', 'system_language', 1.0, 'exact_match', CURRENT_TIMESTAMP);
```

**Option B: Migration Script (recommended for larger changes)**
```sql
-- Create: pipeline/sql/llm_standardization/migrations/004_add_rust_language.sql
INSERT INTO SKILL_STANDARDIZATION_RULES VALUES
('rule_new_001', 'rust', 'Rust', 'languages', 'system_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_new_002', 'rust-lang', 'Rust', 'languages', 'system_language', 0.9, 'exact_match', CURRENT_TIMESTAMP);
```

### 3. Updating Existing Rules

```sql
UPDATE SKILL_STANDARDIZATION_RULES
SET CONFIDENCE_SCORE = 0.95,
    UPDATED_TIMESTAMP = CURRENT_TIMESTAMP
WHERE RULE_ID = 'rule_006';
```

### 4. Asset Integration

The Dagster assets check for rule existence but don't manage the rules themselves:

```python
# In skills_normalization.py
def stage_skills_standardization_rules():
    """
    Ensure standardization rules table exists and validate rules are present.
    Rules are managed via SQL files, not hardcoded in assets.
    """
    # Check if table exists and has rules
    # Log warning if no rules found
    # Don't insert rules (managed separately)
```

## Rule Management Best Practices

### 1. Rule Naming Convention
- **Pattern**: `rule_XXX` where XXX is sequential number
- **Categories**: Use consistent category names
- **Confidence**: Score from 0.0 to 1.0 based on match accuracy

### 2. Version Control
- Commit all rule changes to git
- Use migration scripts for production changes
- Document rule changes in commit messages

### 3. Testing
Always test new rules against sample data:

```sql
-- Test query: How many skills would match the new rule?
SELECT COUNT(*)
FROM SKILLS_RAW_EXTRACTION
WHERE LOWER(SKILL_NAME_RAW) = 'rust';
```

### 4. Conflict Resolution
- Use database constraints to prevent duplicate patterns
- Review confidence scores regularly
- Monitor standardization quality metrics

## Integration with Dagster Assets

The assets follow this pattern:

```python
@asset(deps=["stage_skills_standardization_rules"])
def stage_skills_normalized():
    """
    Uses rules from SKILL_STANDARDIZATION_RULES table.
    Rules are managed separately via SQL files.
    """
    # Query existing rules from database
    # Apply standardization logic
    # No hardcoded rules in asset code
```

## Maintenance Commands

### Check Rule Coverage
```sql
SELECT
    SKILL_CATEGORY,
    COUNT(*) as RULE_COUNT,
    AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE
FROM SKILL_STANDARDIZATION_RULES
GROUP BY SKILL_CATEGORY;
```

### Find Missing Rules
```sql
-- Skills that don't match any standardization rule
SELECT DISTINCT SKILL_NAME_RAW, COUNT(*) as FREQUENCY
FROM SKILLS_RAW_EXTRACTION sre
LEFT JOIN SKILL_STANDARDIZATION_RULES sr
    ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
WHERE sr.PATTERN IS NULL
GROUP BY SKILL_NAME_RAW
ORDER BY FREQUENCY DESC
LIMIT 20;
```

### Validate Rule Quality
```sql
-- Check for duplicate patterns
SELECT PATTERN, COUNT(*) as DUPLICATES
FROM SKILL_STANDARDIZATION_RULES
GROUP BY PATTERN
HAVING COUNT(*) > 1;
```

## US States Country Inference

The `insert_us_states_mapping.sql` file creates a comprehensive lookup table for automatically inferring country as "United States" when state information is present:

### Problem Solved
Many locations like "Sunnyvale, CA" or "Bozeman, MT" don't have explicit rules in the location standardization table, resulting in null country values despite having clear US state indicators.

### Solution
- **Comprehensive Mapping**: All 50 US states + territories (DC, PR, VI, GU, AS, MP)
- **Dual Format Support**: Both full names ("California") and abbreviations ("CA")
- **Automatic Inference**: During location normalization, if state matches any US state, country is set to "United States"
- **Efficient Lookup**: Uses `US_STATES_LOOKUP` view that combines both formats for fast matching

### Usage in Location Normalization
```sql
-- Enhanced country inference logic
COALESCE(
    lsr.COUNTRY,                    -- First try explicit rules
    CASE
        WHEN usl.COUNTRY IS NOT NULL THEN usl.COUNTRY  -- Then US states lookup
        ELSE NULL
    END
) as country
```

## International Location Parsing

The `insert_countries_mapping.sql` file enables intelligent parsing of international locations like "gurugram, india" and "singapore, singapore":

### Problem Solved
Previously, international locations like:
- `"gurugram, india"` → CITY: "Gurugram", STATE_PROVINCE: "India", COUNTRY: null
- `"singapore, singapore"` → CITY: "Singapore", STATE_PROVINCE: "Singapore", COUNTRY: null

Were incorrectly parsed with countries ending up in the STATE_PROVINCE field.

### Enhanced Solution
- **Comprehensive Countries Database**: 100+ countries with official names, common variations, and ISO codes
- **Tech Hub Classification**: Major technology centers marked for business intelligence
- **Smart Parsing Logic**: Priority-based parsing (US states > Countries > Fallback)
- **Combined Lookup View**: `LOCATION_PARSING_LOOKUP` includes both US states and countries

### New Parsing Logic
```sql
-- Priority-based location parsing
CASE
    -- 1. Check if second part is a US state
    WHEN lpl_state.LOCATION_TYPE = 'US_STATE' THEN
        CITY: first_part, STATE: second_part, COUNTRY: "United States"

    -- 2. Check if second part is a country
    WHEN lpl_country.LOCATION_TYPE = 'COUNTRY' THEN
        CITY: first_part, STATE: null, COUNTRY: second_part

    -- 3. Fallback to existing logic
    ELSE original_parsing_logic
END
```

### Expected Results After Implementation
- `"gurugram, india"` → CITY: "Gurugram", STATE_PROVINCE: null, COUNTRY: "India"
- `"singapore, singapore"` → CITY: "Singapore", STATE_PROVINCE: null, COUNTRY: "Singapore"
- `"toronto, canada"` → CITY: "Toronto", STATE_PROVINCE: null, COUNTRY: "Canada"
- `"sunnyvale, ca"` → CITY: "Sunnyvale", STATE_PROVINCE: "CA", COUNTRY: "United States" (unchanged)

This approach provides:
- 🎯 **Clean separation** between static data and application logic
- 🔄 **Easy maintenance** of rules without code changes
- 📊 **Database-driven** configuration that can be queried and analyzed
- 🚀 **Scalable** approach that supports growing rule sets
- 🔒 **Version controlled** rule changes with audit trails
- 🇺🇸 **Automatic country inference** for US locations based on state data
- 🌍 **International location parsing** for worldwide coverage with smart city/country detection