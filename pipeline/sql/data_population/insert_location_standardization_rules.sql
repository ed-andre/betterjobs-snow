-- =========================================================================
-- Location Standardization Rules Setup
-- =========================================================================
-- This file contains standardization rules for normalizing location data
-- extracted from job postings. These rules handle common variations,
-- abbreviations, and formats for cities, states, and work arrangements.
--
-- Usage:
--   Run this file once during initial setup or when rules need updates
--
-- Rule Types:
--   - exact_match: Exact string matching (case-insensitive)
--   - regex_pattern: Regular expression patterns
--   - fuzzy_match: Fuzzy string matching (reserved for future use)
--
-- Location Types:
--   - office: Physical office location
--   - remote: Remote work arrangement
--   - hybrid: Hybrid work arrangement
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- Clear existing rules (optional - comment out to preserve existing data)
-- DELETE FROM LOCATION_STANDARDIZATION_RULES;

-- Comprehensive Location Standardization Rules
INSERT INTO LOCATION_STANDARDIZATION_RULES
(RULE_ID, PATTERN, STANDARDIZED_NAME, CITY, STATE_PROVINCE, COUNTRY, LOCATION_TYPE, CONFIDENCE_SCORE, RULE_TYPE, CREATED_TIMESTAMP)
VALUES

-- =============================================================================
-- MAJOR US TECH HUBS
-- =============================================================================

-- San Francisco Bay Area
('loc_001', 'san francisco', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_002', 'sf', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_003', 'san francisco, ca', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_004', 'san francisco bay area', 'San Francisco Bay Area, CA', 'San Francisco', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_005', 'bay area', 'San Francisco Bay Area, CA', 'San Francisco', 'California', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_006', 'palo alto', 'Palo Alto, CA', 'Palo Alto', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_007', 'mountain view', 'Mountain View, CA', 'Mountain View', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_008', 'cupertino', 'Cupertino, CA', 'Cupertino', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_009', 'sunnyvale', 'Sunnyvale, CA', 'Sunnyvale', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_010', 'santa clara', 'Santa Clara, CA', 'Santa Clara', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- New York Area
('loc_011', 'new york', 'New York, NY', 'New York', 'New York', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_012', 'nyc', 'New York, NY', 'New York', 'New York', 'United States', 'office', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('loc_013', 'new york city', 'New York, NY', 'New York', 'New York', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_014', 'new york, ny', 'New York, NY', 'New York', 'New York', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_015', 'manhattan', 'New York, NY', 'New York', 'New York', 'United States', 'office', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('loc_016', 'brooklyn', 'Brooklyn, NY', 'Brooklyn', 'New York', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Seattle Area
('loc_020', 'seattle', 'Seattle, WA', 'Seattle', 'Washington', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_021', 'seattle, wa', 'Seattle, WA', 'Seattle', 'Washington', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_022', 'bellevue', 'Bellevue, WA', 'Bellevue', 'Washington', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_023', 'redmond', 'Redmond, WA', 'Redmond', 'Washington', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Austin Area
('loc_030', 'austin', 'Austin, TX', 'Austin', 'Texas', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_031', 'austin, tx', 'Austin, TX', 'Austin', 'Texas', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_032', 'austin, texas', 'Austin, TX', 'Austin', 'Texas', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Boston Area
('loc_040', 'boston', 'Boston, MA', 'Boston', 'Massachusetts', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_041', 'boston, ma', 'Boston, MA', 'Boston', 'Massachusetts', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_042', 'cambridge', 'Cambridge, MA', 'Cambridge', 'Massachusetts', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Los Angeles Area
('loc_050', 'los angeles', 'Los Angeles, CA', 'Los Angeles', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_051', 'la', 'Los Angeles, CA', 'Los Angeles', 'California', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_052', 'los angeles, ca', 'Los Angeles, CA', 'Los Angeles', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_053', 'santa monica', 'Santa Monica, CA', 'Santa Monica', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Chicago Area
('loc_060', 'chicago', 'Chicago, IL', 'Chicago', 'Illinois', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_061', 'chicago, il', 'Chicago, IL', 'Chicago', 'Illinois', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Denver Area
('loc_070', 'denver', 'Denver, CO', 'Denver', 'Colorado', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_071', 'denver, co', 'Denver, CO', 'Denver', 'Colorado', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_072', 'boulder', 'Boulder, CO', 'Boulder', 'Colorado', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Atlanta Area
('loc_080', 'atlanta', 'Atlanta, GA', 'Atlanta', 'Georgia', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_081', 'atlanta, ga', 'Atlanta, GA', 'Atlanta', 'Georgia', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Washington DC Area
('loc_090', 'washington dc', 'Washington, DC', 'Washington', 'District of Columbia', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_091', 'washington, dc', 'Washington, DC', 'Washington', 'District of Columbia', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_092', 'dc', 'Washington, DC', 'Washington', 'District of Columbia', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_093', 'arlington', 'Arlington, VA', 'Arlington', 'Virginia', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- REMOTE WORK PATTERNS
-- =============================================================================

('loc_100', 'remote', 'Remote', NULL, NULL, 'Global', 'remote', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_101', 'work from home', 'Remote', NULL, NULL, 'Global', 'remote', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_102', 'wfh', 'Remote', NULL, NULL, 'Global', 'remote', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_103', 'telecommute', 'Remote', NULL, NULL, 'Global', 'remote', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_104', 'fully remote', 'Remote', NULL, NULL, 'Global', 'remote', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_105', '100% remote', 'Remote', NULL, NULL, 'Global', 'remote', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_106', 'anywhere', 'Remote', NULL, NULL, 'Global', 'remote', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_107', 'virtual', 'Remote', NULL, NULL, 'Global', 'remote', 0.8, 'exact_match', CURRENT_TIMESTAMP),

-- Hybrid patterns
('loc_110', 'hybrid', 'Hybrid', NULL, NULL, 'Various', 'hybrid', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_111', 'hybrid remote', 'Hybrid', NULL, NULL, 'Various', 'hybrid', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_112', 'flexible', 'Hybrid', NULL, NULL, 'Various', 'hybrid', 0.7, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- OTHER MAJOR US CITIES
-- =============================================================================

-- Miami Area
('loc_120', 'miami', 'Miami, FL', 'Miami', 'Florida', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_121', 'miami, fl', 'Miami, FL', 'Miami', 'Florida', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Phoenix Area
('loc_130', 'phoenix', 'Phoenix, AZ', 'Phoenix', 'Arizona', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_131', 'phoenix, az', 'Phoenix, AZ', 'Phoenix', 'Arizona', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Portland Area
('loc_140', 'portland', 'Portland, OR', 'Portland', 'Oregon', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_141', 'portland, or', 'Portland, OR', 'Portland', 'Oregon', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Salt Lake City Area
('loc_150', 'salt lake city', 'Salt Lake City, UT', 'Salt Lake City', 'Utah', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_151', 'slc', 'Salt Lake City, UT', 'Salt Lake City', 'Utah', 'United States', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- San Diego Area
('loc_160', 'san diego', 'San Diego, CA', 'San Diego', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_161', 'san diego, ca', 'San Diego, CA', 'San Diego', 'California', 'United States', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- INTERNATIONAL LOCATIONS
-- =============================================================================

-- Canada
('loc_200', 'toronto', 'Toronto, ON', 'Toronto', 'Ontario', 'Canada', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_201', 'vancouver', 'Vancouver, BC', 'Vancouver', 'British Columbia', 'Canada', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_202', 'montreal', 'Montreal, QC', 'Montreal', 'Quebec', 'Canada', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- United Kingdom
('loc_210', 'london', 'London, UK', 'London', 'England', 'United Kingdom', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_211', 'london, uk', 'London, UK', 'London', 'England', 'United Kingdom', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('loc_212', 'edinburgh', 'Edinburgh, UK', 'Edinburgh', 'Scotland', 'United Kingdom', 'office', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Germany
('loc_220', 'berlin', 'Berlin, Germany', 'Berlin', 'Berlin', 'Germany', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_221', 'munich', 'Munich, Germany', 'Munich', 'Bavaria', 'Germany', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Netherlands
('loc_230', 'amsterdam', 'Amsterdam, Netherlands', 'Amsterdam', 'North Holland', 'Netherlands', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- France
('loc_240', 'paris', 'Paris, France', 'Paris', 'Île-de-France', 'France', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),

-- Australia
('loc_250', 'sydney', 'Sydney, Australia', 'Sydney', 'New South Wales', 'Australia', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('loc_251', 'melbourne', 'Melbourne, Australia', 'Melbourne', 'Victoria', 'Australia', 'office', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- STATE ABBREVIATIONS AND COMMON VARIANTS
-- =============================================================================

-- California variations
('loc_300', 'california', 'California, US', NULL, 'California', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_301', 'ca', 'California, US', NULL, 'California', 'United States', 'office', 0.6, 'exact_match', CURRENT_TIMESTAMP),

-- New York variations
('loc_310', 'new york state', 'New York, US', NULL, 'New York', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_311', 'ny', 'New York, US', NULL, 'New York', 'United States', 'office', 0.6, 'exact_match', CURRENT_TIMESTAMP),

-- Texas variations
('loc_320', 'texas', 'Texas, US', NULL, 'Texas', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_321', 'tx', 'Texas, US', NULL, 'Texas', 'United States', 'office', 0.6, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- COMMON VARIATIONS AND MISSPELLINGS
-- =============================================================================

-- Common misspellings and variations
('loc_400', 'san fransisco', 'San Francisco, CA', 'San Francisco', 'California', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_401', 'new yourk', 'New York, NY', 'New York', 'New York', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('loc_402', 'seatle', 'Seattle, WA', 'Seattle', 'Washington', 'United States', 'office', 0.8, 'exact_match', CURRENT_TIMESTAMP),

-- Generic US patterns
('loc_500', 'usa', 'United States', NULL, NULL, 'United States', 'office', 0.5, 'exact_match', CURRENT_TIMESTAMP),
('loc_501', 'united states', 'United States', NULL, NULL, 'United States', 'office', 0.5, 'exact_match', CURRENT_TIMESTAMP),
('loc_502', 'us', 'United States', NULL, NULL, 'United States', 'office', 0.4, 'exact_match', CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these to verify the rules were inserted correctly

-- Check total rule count by location type
SELECT
    LOCATION_TYPE,
    COUNT(*) as RULE_COUNT,
    COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as HIGH_CONFIDENCE_RULES,
    AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE
FROM LOCATION_STANDARDIZATION_RULES
GROUP BY LOCATION_TYPE
ORDER BY RULE_COUNT DESC;

-- Check for duplicate patterns
SELECT
    PATTERN,
    COUNT(*) as DUPLICATE_COUNT
FROM LOCATION_STANDARDIZATION_RULES
GROUP BY PATTERN
HAVING COUNT(*) > 1;

-- Sample rules by type
SELECT
    LOCATION_TYPE,
    PATTERN,
    STANDARDIZED_NAME,
    CONFIDENCE_SCORE,
    RULE_TYPE
FROM LOCATION_STANDARDIZATION_RULES
WHERE LOCATION_TYPE IN ('office', 'remote', 'hybrid')
ORDER BY LOCATION_TYPE, CONFIDENCE_SCORE DESC
LIMIT 30;

-- Check geographic distribution
SELECT
    COUNTRY,
    STATE_PROVINCE,
    COUNT(*) as LOCATION_COUNT
FROM LOCATION_STANDARDIZATION_RULES
WHERE LOCATION_TYPE = 'office'
AND COUNTRY IS NOT NULL
GROUP BY COUNTRY, STATE_PROVINCE
ORDER BY COUNTRY, LOCATION_COUNT DESC;