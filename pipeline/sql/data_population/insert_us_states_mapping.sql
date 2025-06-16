-- =========================================================================
-- US States Mapping for Country Inference
-- =========================================================================
-- This file contains a comprehensive mapping of US states (full names and
-- abbreviations) to automatically set country to "United States" during
-- location normalization when state information is present.
--
-- Usage:
--   Run this file once during initial setup or when updates are needed
--   Used by the location normalization process to infer country from state
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;
-- Clear existing data (optional - comment out to preserve existing data)
DELETE FROM US_STATES_MAPPING;

-- Insert comprehensive US states mapping
INSERT INTO US_STATES_MAPPING
(STATE_ID, STATE_NAME_FULL, STATE_ABBREVIATION, COUNTRY, CREATED_TIMESTAMP)
VALUES
-- All 50 US States
('state_001', 'Alabama', 'AL', 'United States', CURRENT_TIMESTAMP),
('state_002', 'Alaska', 'AK', 'United States', CURRENT_TIMESTAMP),
('state_003', 'Arizona', 'AZ', 'United States', CURRENT_TIMESTAMP),
('state_004', 'Arkansas', 'AR', 'United States', CURRENT_TIMESTAMP),
('state_005', 'California', 'CA', 'United States', CURRENT_TIMESTAMP),
('state_006', 'Colorado', 'CO', 'United States', CURRENT_TIMESTAMP),
('state_007', 'Connecticut', 'CT', 'United States', CURRENT_TIMESTAMP),
('state_008', 'Delaware', 'DE', 'United States', CURRENT_TIMESTAMP),
('state_009', 'Florida', 'FL', 'United States', CURRENT_TIMESTAMP),
('state_010', 'Georgia', 'GA', 'United States', CURRENT_TIMESTAMP),
('state_011', 'Hawaii', 'HI', 'United States', CURRENT_TIMESTAMP),
('state_012', 'Idaho', 'ID', 'United States', CURRENT_TIMESTAMP),
('state_013', 'Illinois', 'IL', 'United States', CURRENT_TIMESTAMP),
('state_014', 'Indiana', 'IN', 'United States', CURRENT_TIMESTAMP),
('state_015', 'Iowa', 'IA', 'United States', CURRENT_TIMESTAMP),
('state_016', 'Kansas', 'KS', 'United States', CURRENT_TIMESTAMP),
('state_017', 'Kentucky', 'KY', 'United States', CURRENT_TIMESTAMP),
('state_018', 'Louisiana', 'LA', 'United States', CURRENT_TIMESTAMP),
('state_019', 'Maine', 'ME', 'United States', CURRENT_TIMESTAMP),
('state_020', 'Maryland', 'MD', 'United States', CURRENT_TIMESTAMP),
('state_021', 'Massachusetts', 'MA', 'United States', CURRENT_TIMESTAMP),
('state_022', 'Michigan', 'MI', 'United States', CURRENT_TIMESTAMP),
('state_023', 'Minnesota', 'MN', 'United States', CURRENT_TIMESTAMP),
('state_024', 'Mississippi', 'MS', 'United States', CURRENT_TIMESTAMP),
('state_025', 'Missouri', 'MO', 'United States', CURRENT_TIMESTAMP),
('state_026', 'Montana', 'MT', 'United States', CURRENT_TIMESTAMP),
('state_027', 'Nebraska', 'NE', 'United States', CURRENT_TIMESTAMP),
('state_028', 'Nevada', 'NV', 'United States', CURRENT_TIMESTAMP),
('state_029', 'New Hampshire', 'NH', 'United States', CURRENT_TIMESTAMP),
('state_030', 'New Jersey', 'NJ', 'United States', CURRENT_TIMESTAMP),
('state_031', 'New Mexico', 'NM', 'United States', CURRENT_TIMESTAMP),
('state_032', 'New York', 'NY', 'United States', CURRENT_TIMESTAMP),
('state_033', 'North Carolina', 'NC', 'United States', CURRENT_TIMESTAMP),
('state_034', 'North Dakota', 'ND', 'United States', CURRENT_TIMESTAMP),
('state_035', 'Ohio', 'OH', 'United States', CURRENT_TIMESTAMP),
('state_036', 'Oklahoma', 'OK', 'United States', CURRENT_TIMESTAMP),
('state_037', 'Oregon', 'OR', 'United States', CURRENT_TIMESTAMP),
('state_038', 'Pennsylvania', 'PA', 'United States', CURRENT_TIMESTAMP),
('state_039', 'Rhode Island', 'RI', 'United States', CURRENT_TIMESTAMP),
('state_040', 'South Carolina', 'SC', 'United States', CURRENT_TIMESTAMP),
('state_041', 'South Dakota', 'SD', 'United States', CURRENT_TIMESTAMP),
('state_042', 'Tennessee', 'TN', 'United States', CURRENT_TIMESTAMP),
('state_043', 'Texas', 'TX', 'United States', CURRENT_TIMESTAMP),
('state_044', 'Utah', 'UT', 'United States', CURRENT_TIMESTAMP),
('state_045', 'Vermont', 'VT', 'United States', CURRENT_TIMESTAMP),
('state_046', 'Virginia', 'VA', 'United States', CURRENT_TIMESTAMP),
('state_047', 'Washington', 'WA', 'United States', CURRENT_TIMESTAMP),
('state_048', 'West Virginia', 'WV', 'United States', CURRENT_TIMESTAMP),
('state_049', 'Wisconsin', 'WI', 'United States', CURRENT_TIMESTAMP),
('state_050', 'Wyoming', 'WY', 'United States', CURRENT_TIMESTAMP),

-- US Territories and Federal District
('state_051', 'District of Columbia', 'DC', 'United States', CURRENT_TIMESTAMP),
('state_052', 'Puerto Rico', 'PR', 'United States', CURRENT_TIMESTAMP),
('state_053', 'US Virgin Islands', 'VI', 'United States', CURRENT_TIMESTAMP),
('state_054', 'Guam', 'GU', 'United States', CURRENT_TIMESTAMP),
('state_055', 'American Samoa', 'AS', 'United States', CURRENT_TIMESTAMP),
('state_056', 'Northern Mariana Islands', 'MP', 'United States', CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

