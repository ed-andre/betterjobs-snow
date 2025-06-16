-- =========================================================================
-- Countries Mapping for International Location Standardization
-- =========================================================================
-- This file contains a comprehensive mapping of world countries (full names,
-- common variations, and abbreviations) to enable proper parsing of
-- international locations like "singapore, singapore" or "gurugram, india"
--
-- Usage:
--   Run this file once during initial setup or when updates are needed
--   Used by location normalization to distinguish countries from US states
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;



-- Clear existing data (optional - comment out to preserve existing data)
DELETE FROM COUNTRIES_MAPPING;

-- Insert comprehensive countries mapping
INSERT INTO COUNTRIES_MAPPING
(COUNTRY_ID, COUNTRY_NAME_OFFICIAL, COUNTRY_NAME_COMMON, COUNTRY_CODE_ISO2, COUNTRY_CODE_ISO3, REGION, IS_MAJOR_TECH_HUB, CREATED_TIMESTAMP)
VALUES

-- =============================================================================
-- MAJOR TECH HUB COUNTRIES
-- =============================================================================

-- North America
('country_001', 'United States of America', 'United States', 'US', 'USA', 'North America', TRUE, CURRENT_TIMESTAMP),
('country_002', 'Canada', 'Canada', 'CA', 'CAN', 'North America', TRUE, CURRENT_TIMESTAMP),
('country_003', 'Mexico', 'Mexico', 'MX', 'MEX', 'North America', FALSE, CURRENT_TIMESTAMP),

-- Europe - Major Tech Centers
('country_010', 'United Kingdom of Great Britain and Northern Ireland', 'United Kingdom', 'GB', 'GBR', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_011', 'Germany', 'Germany', 'DE', 'DEU', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_012', 'France', 'France', 'FR', 'FRA', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_013', 'Netherlands', 'Netherlands', 'NL', 'NLD', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_014', 'Switzerland', 'Switzerland', 'CH', 'CHE', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_015', 'Sweden', 'Sweden', 'SE', 'SWE', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_016', 'Ireland', 'Ireland', 'IE', 'IRL', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_017', 'Denmark', 'Denmark', 'DK', 'DNK', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_018', 'Finland', 'Finland', 'FI', 'FIN', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_019', 'Norway', 'Norway', 'NO', 'NOR', 'Europe', TRUE, CURRENT_TIMESTAMP),

-- Asia-Pacific - Major Tech Centers
('country_020', 'India', 'India', 'IN', 'IND', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_021', 'China', 'China', 'CN', 'CHN', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_022', 'Japan', 'Japan', 'JP', 'JPN', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_023', 'Singapore', 'Singapore', 'SG', 'SGP', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_024', 'South Korea', 'South Korea', 'KR', 'KOR', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_025', 'Australia', 'Australia', 'AU', 'AUS', 'Oceania', TRUE, CURRENT_TIMESTAMP),
('country_026', 'New Zealand', 'New Zealand', 'NZ', 'NZL', 'Oceania', TRUE, CURRENT_TIMESTAMP),
('country_027', 'Hong Kong', 'Hong Kong', 'HK', 'HKG', 'Asia', TRUE, CURRENT_TIMESTAMP),
('country_028', 'Taiwan', 'Taiwan', 'TW', 'TWN', 'Asia', TRUE, CURRENT_TIMESTAMP),

-- Middle East & Africa
('country_030', 'Israel', 'Israel', 'IL', 'ISR', 'Middle East', TRUE, CURRENT_TIMESTAMP),
('country_031', 'United Arab Emirates', 'UAE', 'AE', 'ARE', 'Middle East', TRUE, CURRENT_TIMESTAMP),
('country_032', 'South Africa', 'South Africa', 'ZA', 'ZAF', 'Africa', TRUE, CURRENT_TIMESTAMP),

-- Other European Countries
('country_040', 'Italy', 'Italy', 'IT', 'ITA', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_041', 'Spain', 'Spain', 'ES', 'ESP', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_042', 'Portugal', 'Portugal', 'PT', 'PRT', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_043', 'Belgium', 'Belgium', 'BE', 'BEL', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_044', 'Austria', 'Austria', 'AT', 'AUT', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_045', 'Poland', 'Poland', 'PL', 'POL', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_046', 'Czech Republic', 'Czech Republic', 'CZ', 'CZE', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_047', 'Hungary', 'Hungary', 'HU', 'HUN', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_048', 'Romania', 'Romania', 'RO', 'ROU', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_049', 'Bulgaria', 'Bulgaria', 'BG', 'BGR', 'Europe', FALSE, CURRENT_TIMESTAMP),
('country_050', 'Croatia', 'Croatia', 'HR', 'HRV', 'Europe', FALSE, CURRENT_TIMESTAMP),

-- Other Asian Countries
('country_060', 'Thailand', 'Thailand', 'TH', 'THA', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_061', 'Malaysia', 'Malaysia', 'MY', 'MYS', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_062', 'Indonesia', 'Indonesia', 'ID', 'IDN', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_063', 'Philippines', 'Philippines', 'PH', 'PHL', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_064', 'Vietnam', 'Vietnam', 'VN', 'VNM', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_065', 'Bangladesh', 'Bangladesh', 'BD', 'BGD', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_066', 'Pakistan', 'Pakistan', 'PK', 'PAK', 'Asia', FALSE, CURRENT_TIMESTAMP),
('country_067', 'Sri Lanka', 'Sri Lanka', 'LK', 'LKA', 'Asia', FALSE, CURRENT_TIMESTAMP),

-- Latin America
('country_070', 'Brazil', 'Brazil', 'BR', 'BRA', 'South America', FALSE, CURRENT_TIMESTAMP),
('country_071', 'Argentina', 'Argentina', 'AR', 'ARG', 'South America', FALSE, CURRENT_TIMESTAMP),
('country_072', 'Chile', 'Chile', 'CL', 'CHL', 'South America', FALSE, CURRENT_TIMESTAMP),
('country_073', 'Colombia', 'Colombia', 'CO', 'COL', 'South America', FALSE, CURRENT_TIMESTAMP),
('country_074', 'Peru', 'Peru', 'PE', 'PER', 'South America', FALSE, CURRENT_TIMESTAMP),
('country_075', 'Uruguay', 'Uruguay', 'UY', 'URY', 'South America', FALSE, CURRENT_TIMESTAMP),

-- Additional Middle East
('country_080', 'Turkey', 'Turkey', 'TR', 'TUR', 'Middle East', FALSE, CURRENT_TIMESTAMP),
('country_081', 'Saudi Arabia', 'Saudi Arabia', 'SA', 'SAU', 'Middle East', FALSE, CURRENT_TIMESTAMP),
('country_082', 'Qatar', 'Qatar', 'QA', 'QAT', 'Middle East', FALSE, CURRENT_TIMESTAMP),
('country_083', 'Kuwait', 'Kuwait', 'KW', 'KWT', 'Middle East', FALSE, CURRENT_TIMESTAMP),

-- =============================================================================
-- COMMON COUNTRY NAME VARIATIONS AND ABBREVIATIONS
-- =============================================================================

-- Additional variations for major countries
('country_100', 'United Kingdom', 'UK', 'GB', 'GBR', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_101', 'United Kingdom', 'Britain', 'GB', 'GBR', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_102', 'United Kingdom', 'England', 'GB', 'GBR', 'Europe', TRUE, CURRENT_TIMESTAMP),
('country_103', 'United States', 'USA', 'US', 'USA', 'North America', TRUE, CURRENT_TIMESTAMP),
('country_104', 'United States', 'US', 'US', 'USA', 'North America', TRUE, CURRENT_TIMESTAMP),
('country_105', 'United Arab Emirates', 'Emirates', 'AE', 'ARE', 'Middle East', TRUE, CURRENT_TIMESTAMP),
('country_106', 'Republic of Korea', 'Korea', 'KR', 'KOR', 'Asia', TRUE, CURRENT_TIMESTAMP),

-- Common tech hub city-countries (for parsing)
('country_200', 'Singapore', 'Singapore', 'SG', 'SGP', 'Asia', TRUE, CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

