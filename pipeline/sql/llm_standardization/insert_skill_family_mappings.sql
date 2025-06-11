-- =========================================================================
-- Skill Family Mappings Setup
-- =========================================================================
-- This file contains mappings from skill categories to skill families.
-- Skill families group related categories together for higher-level analytics
-- and reporting purposes.
--
-- Usage:
--   Run this file once during initial setup or when mappings need updates
--
-- Families:
--   - development: Core software development skills
--   - data: Data management and storage technologies
--   - devops: Infrastructure and deployment technologies
--   - interpersonal: Soft skills and people management
--   - business: Business and domain knowledge
--   - professional: Certifications and credentials
--   - process: Methodologies and frameworks
--   - creative: Design and creative skills
--   - security: Security and compliance skills
--   - analytics: Analytics and reporting tools
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- Clear existing mappings (optional - comment out to preserve existing data)
-- DELETE FROM SKILL_FAMILY_MAPPING;

-- Comprehensive Skill Family Mappings
INSERT INTO SKILL_FAMILY_MAPPING
(MAPPING_ID, SKILL_CATEGORY, SKILL_FAMILY, FAMILY_DESCRIPTION, PRIORITY, IS_ACTIVE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP)
VALUES

-- =============================================================================
-- DEVELOPMENT FAMILY - Core software development skills
-- =============================================================================

('map_001', 'languages', 'development', 'Programming languages and scripting', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_002', 'frameworks', 'development', 'Application frameworks and libraries', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_003', 'tools', 'development', 'Development tools and utilities', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- DATA FAMILY - Data management and storage
-- =============================================================================

('map_004', 'databases', 'data', 'Database technologies and data storage', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_005', 'data_tools', 'data', 'Data processing and analytics tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_006', 'etl', 'data', 'Extract, Transform, Load technologies', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- DEVOPS FAMILY - Infrastructure and deployment
-- =============================================================================

('map_007', 'cloud', 'devops', 'Cloud platforms and infrastructure', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_008', 'containers', 'devops', 'Containerization and orchestration', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_009', 'infrastructure', 'devops', 'Infrastructure as Code and automation', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_010', 'monitoring', 'devops', 'Monitoring and observability tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- INTERPERSONAL FAMILY - Soft skills and people management
-- =============================================================================

('map_011', 'soft', 'interpersonal', 'Soft skills and interpersonal abilities', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_012', 'leadership', 'interpersonal', 'Leadership and management skills', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_013', 'communication', 'interpersonal', 'Communication and collaboration skills', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- BUSINESS FAMILY - Business and domain knowledge
-- =============================================================================

('map_014', 'keyword', 'business', 'Business and domain-specific keywords', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_015', 'industry', 'business', 'Industry-specific knowledge and terms', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_016', 'business_tools', 'business', 'Business productivity and analysis tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- PROFESSIONAL FAMILY - Certifications and credentials
-- =============================================================================

('map_017', 'certification', 'professional', 'Professional certifications and credentials', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_018', 'qualification', 'professional', 'Educational qualifications and degrees', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- PROCESS FAMILY - Methodologies and frameworks
-- =============================================================================

('map_019', 'methodology', 'process', 'Development methodologies and processes', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_020', 'agile', 'process', 'Agile and project management frameworks', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_021', 'testing', 'process', 'Testing methodologies and practices', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- CREATIVE FAMILY - Design and creative skills
-- =============================================================================

('map_022', 'design', 'creative', 'Design and creative skills', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_023', 'ui_ux', 'creative', 'User interface and experience design', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_024', 'graphics', 'creative', 'Graphics and visual design tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- SECURITY FAMILY - Security and compliance
-- =============================================================================

('map_025', 'security', 'security', 'Security tools and practices', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_026', 'compliance', 'security', 'Compliance and governance frameworks', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_027', 'privacy', 'security', 'Privacy and data protection', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- ANALYTICS FAMILY - Analytics and reporting
-- =============================================================================

('map_028', 'analytics', 'analytics', 'Analytics and business intelligence tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_029', 'reporting', 'analytics', 'Reporting and visualization tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_030', 'ml', 'analytics', 'Machine learning and AI tools', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- ADDITIONAL MAPPINGS - Specialized categories
-- =============================================================================

('map_031', 'mobile', 'development', 'Mobile development platforms and tools', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_032', 'web', 'development', 'Web development technologies', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_033', 'api', 'development', 'API development and integration', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_034', 'game', 'development', 'Game development technologies', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('map_035', 'blockchain', 'development', 'Blockchain and cryptocurrency technologies', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Quality assurance and fallback
('map_999', 'uncategorized', 'general', 'General or uncategorized skills', 9, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these to verify the mappings were inserted correctly

-- Check total mapping count by family
SELECT
    SKILL_FAMILY,
    COUNT(*) as CATEGORY_COUNT,
    COUNT(CASE WHEN IS_ACTIVE = TRUE THEN 1 END) as ACTIVE_MAPPINGS,
    COUNT(CASE WHEN PRIORITY = 1 THEN 1 END) as PRIMARY_MAPPINGS
FROM SKILL_FAMILY_MAPPING
GROUP BY SKILL_FAMILY
ORDER BY CATEGORY_COUNT DESC;

-- Check for duplicate category mappings (should be carefully managed)
SELECT
    SKILL_CATEGORY,
    COUNT(*) as MAPPING_COUNT,
    LISTAGG(SKILL_FAMILY, ', ') WITHIN GROUP (ORDER BY PRIORITY) as FAMILIES
FROM SKILL_FAMILY_MAPPING
WHERE IS_ACTIVE = TRUE
GROUP BY SKILL_CATEGORY
HAVING COUNT(*) > 1;

-- Show complete mapping structure
SELECT
    SKILL_FAMILY,
    SKILL_CATEGORY,
    FAMILY_DESCRIPTION,
    PRIORITY,
    IS_ACTIVE
FROM SKILL_FAMILY_MAPPING
ORDER BY SKILL_FAMILY, PRIORITY, SKILL_CATEGORY;

-- Show categories without mappings (should be empty after running this script)
SELECT DISTINCT sre.SKILL_CATEGORY
FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION sre
LEFT JOIN SKILL_FAMILY_MAPPING sfm ON sre.SKILL_CATEGORY = sfm.SKILL_CATEGORY AND sfm.IS_ACTIVE = TRUE
WHERE sfm.SKILL_CATEGORY IS NULL;