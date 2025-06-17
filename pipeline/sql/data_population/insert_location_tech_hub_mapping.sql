-- =========================================================================
-- Location Tech Hub Mapping Data Population
-- =========================================================================
-- This file contains INSERT statements for tech hub location mappings
-- to identify major technology centers for location normalization.
--
-- Usage: Executed automatically by static_data_population asset
-- Target Table: STAGE.LOCATION_TECH_HUB_MAPPING
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

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

-- Commit the transaction
COMMIT;
