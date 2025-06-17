-- =========================================================================
-- Location Region Mapping Data Population
-- =========================================================================
-- This file contains INSERT statements for regional location mappings
-- to support geographic analysis and location categorization.
--
-- Usage: Executed automatically by static_data_population asset
-- Target Table: STAGE.LOCATION_REGION_MAPPING
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

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

-- Commit the transaction
COMMIT;


