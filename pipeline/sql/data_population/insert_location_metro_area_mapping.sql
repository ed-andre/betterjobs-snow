-- =========================================================================
-- Location Metro Area Mapping Data Population
-- =========================================================================
-- This file contains INSERT statements for metro area location mappings
-- to support regional job market analysis and location normalization.
--
-- Usage: Executed automatically by static_data_population asset
-- Target Table: STAGE.LOCATION_METRO_AREA_MAPPING
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

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

-- Commit the transaction
COMMIT;

