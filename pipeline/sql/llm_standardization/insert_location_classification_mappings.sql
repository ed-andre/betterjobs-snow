-- =========================================================================
-- Location Classification Mappings Setup
-- =========================================================================
-- This file contains classification mappings for location normalization:
-- - Metro area mappings (cities to metropolitan areas)
-- - Region mappings (states/provinces to geographic regions)
-- - Tech hub classifications (major technology centers)
--
-- Usage:
--   Run this file once during initial setup or when mappings need updates
--
-- Dependencies:
--   - LOCATION_METRO_AREA_MAPPING table
--   - LOCATION_REGION_MAPPING table
--   - LOCATION_TECH_HUB_MAPPING table
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- =============================================================================
-- METRO AREA MAPPINGS
-- =============================================================================

INSERT INTO LOCATION_METRO_AREA_MAPPING
(MAPPING_ID, CITY, STATE_PROVINCE, COUNTRY, METRO_AREA, IS_PRIMARY_CITY, CONFIDENCE_SCORE, IS_ACTIVE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP)
VALUES

-- San Francisco Bay Area
('metro_001', 'San Francisco', 'California', 'United States', 'San Francisco Bay Area', TRUE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_002', 'San Jose', 'California', 'United States', 'San Francisco Bay Area', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_003', 'Oakland', 'California', 'United States', 'San Francisco Bay Area', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_004', 'Palo Alto', 'California', 'United States', 'San Francisco Bay Area', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- New York Metro Area
('metro_011', 'New York', 'New York', 'United States', 'New York Metro', TRUE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_012', 'Brooklyn', 'New York', 'United States', 'New York Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_013', 'Queens', 'New York', 'United States', 'New York Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_014', 'Manhattan', 'New York', 'United States', 'New York Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Los Angeles Metro Area
('metro_020', 'Los Angeles', 'California', 'United States', 'Los Angeles Metro', TRUE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_021', 'Santa Monica', 'California', 'United States', 'Los Angeles Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_022', 'Beverly Hills', 'California', 'United States', 'Los Angeles Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Seattle Metro Area
('metro_030', 'Seattle', 'Washington', 'United States', 'Seattle Metro', TRUE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_031', 'Bellevue', 'Washington', 'United States', 'Seattle Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_032', 'Redmond', 'Washington', 'United States', 'Seattle Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Boston Metro Area
('metro_040', 'Boston', 'Massachusetts', 'United States', 'Boston Metro', TRUE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_041', 'Cambridge', 'Massachusetts', 'United States', 'Boston Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('metro_042', 'Somerville', 'Massachusetts', 'United States', 'Boston Metro', FALSE, 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- =============================================================================
-- REGION MAPPINGS
-- =============================================================================

INSERT INTO LOCATION_REGION_MAPPING
(MAPPING_ID, STATE_PROVINCE, COUNTRY, REGION, REGION_DESCRIPTION, IS_ACTIVE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP)
VALUES

-- United States Regions
('region_001', 'California', 'United States', 'West Coast', 'Pacific coast states with major tech centers', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_002', 'Oregon', 'United States', 'Pacific Northwest', 'Northwestern states with growing tech scenes', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_003', 'Washington', 'United States', 'Pacific Northwest', 'Northwestern states with growing tech scenes', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('region_010', 'New York', 'United States', 'Northeast', 'Northeastern financial and tech corridor', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_011', 'New Jersey', 'United States', 'Northeast', 'Northeastern financial and tech corridor', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_012', 'Connecticut', 'United States', 'Northeast', 'Northeastern financial and tech corridor', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_013', 'Massachusetts', 'United States', 'Northeast', 'Northeastern financial and tech corridor', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('region_020', 'Texas', 'United States', 'South', 'Southern states with emerging tech hubs', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_021', 'Florida', 'United States', 'South', 'Southern states with emerging tech hubs', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_022', 'Georgia', 'United States', 'South', 'Southern states with emerging tech hubs', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('region_040', 'Illinois', 'United States', 'Midwest', 'Central states with established business centers', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_041', 'Michigan', 'United States', 'Midwest', 'Central states with established business centers', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('region_042', 'Ohio', 'United States', 'Midwest', 'Central states with established business centers', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- =============================================================================
-- TECH HUB MAPPINGS
-- =============================================================================

INSERT INTO LOCATION_TECH_HUB_MAPPING
(MAPPING_ID, CITY, STATE_PROVINCE, COUNTRY, IS_MAJOR_TECH_HUB, TECH_HUB_TYPE, HUB_DESCRIPTION, CONFIDENCE_SCORE, IS_ACTIVE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP)
VALUES

-- Tier 1 Tech Hubs
('tech_001', 'San Francisco', 'California', 'United States', TRUE, 'Tier 1', 'Silicon Valley venture capital and startups epicenter', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_002', 'San Jose', 'California', 'United States', TRUE, 'Tier 1', 'Silicon Valley hardware and semiconductor center', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_003', 'Palo Alto', 'California', 'United States', TRUE, 'Tier 1', 'Stanford area research and AI development', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_004', 'Mountain View', 'California', 'United States', TRUE, 'Tier 1', 'Google headquarters and Googleplex area', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_005', 'Sunnyvale', 'California', 'United States', TRUE, 'Tier 1', 'Major tech companies and semiconductor industry', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('tech_010', 'Seattle', 'Washington', 'United States', TRUE, 'Tier 1', 'Amazon and Microsoft headquarters region', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_011', 'Redmond', 'Washington', 'United States', TRUE, 'Tier 1', 'Microsoft headquarters and enterprise software', 1.0, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_012', 'Bellevue', 'Washington', 'United States', TRUE, 'Tier 1', 'Growing tech center across from Seattle', 0.9, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('tech_020', 'New York', 'New York', 'United States', TRUE, 'Tier 1', 'Fintech, adtech, and enterprise software hub', 0.95, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_021', 'Brooklyn', 'New York', 'United States', TRUE, 'Tier 2', 'Emerging startup ecosystem and creative tech', 0.8, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Tier 2 Tech Hubs
('tech_030', 'Boston', 'Massachusetts', 'United States', TRUE, 'Tier 2', 'Biotech, robotics, and enterprise software', 0.9, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_031', 'Cambridge', 'Massachusetts', 'United States', TRUE, 'Tier 2', 'MIT/Harvard area research and innovation', 0.9, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

('tech_040', 'Austin', 'Texas', 'United States', TRUE, 'Tier 2', 'Emerging tech hub with favorable business climate', 0.9, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('tech_041', 'Dallas', 'Texas', 'United States', TRUE, 'Tier 2', 'Large corporate tech centers and telecommunications', 0.8, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- Check metro area mapping counts
SELECT
    METRO_AREA,
    COUNT(*) as CITY_COUNT,
    COUNT(CASE WHEN IS_PRIMARY_CITY THEN 1 END) as PRIMARY_CITIES
FROM LOCATION_METRO_AREA_MAPPING
WHERE IS_ACTIVE = TRUE
GROUP BY METRO_AREA
ORDER BY CITY_COUNT DESC;

-- Check region mapping distribution
SELECT
    REGION,
    COUNT(*) as STATE_COUNT,
    REGION_DESCRIPTION
FROM LOCATION_REGION_MAPPING
WHERE IS_ACTIVE = TRUE
GROUP BY REGION, REGION_DESCRIPTION
ORDER BY STATE_COUNT DESC;

-- Check tech hub distribution
SELECT
    TECH_HUB_TYPE,
    COUNT(*) as HUB_COUNT,
    AVG(CONFIDENCE_SCORE) as AVG_CONFIDENCE
FROM LOCATION_TECH_HUB_MAPPING
WHERE IS_ACTIVE = TRUE
GROUP BY TECH_HUB_TYPE
ORDER BY HUB_COUNT DESC;
