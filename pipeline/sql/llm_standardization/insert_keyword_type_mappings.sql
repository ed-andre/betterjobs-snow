/*
 * Keyword Type Mappings Configuration
 *
 * This file contains INSERT statements for populating the KEYWORD_TYPE_MAPPING table
 * with comprehensive keyword type and category classifications.
 *
 * Classification Categories:
 * - Industry Types: technology, healthcare, finance, retail, manufacturing
 * - Company Stage: startup, growth, enterprise, public, non_profit
 * - Role Hierarchy: individual_contributor, manager, director, executive
 * - Function Types: engineering, sales, marketing, operations, support
 * - Work Style: remote_friendly, hybrid, on_site, distributed
 *
 * Usage:
 * Execute this file after creating the KEYWORD_TYPE_MAPPING table
 * to populate it with type classification mappings.
 */

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- Clear existing mappings (optional - use for clean setup)
-- DELETE FROM BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING;

-- Industry Type Mappings
-- ======================

-- Technology Industry Keywords
INSERT INTO BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING VALUES
('ktm_001', 'technology', 'industry', 'technology', 'Technology and software companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_002', 'software', 'industry', 'technology', 'Software development and services', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_003', 'fintech', 'industry', 'financial_services', 'Financial technology companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_004', 'financial technology', 'industry', 'financial_services', 'Financial technology companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_005', 'saas', 'industry', 'technology', 'Software as a Service companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_006', 'b2b saas', 'industry', 'technology', 'Business-to-business SaaS companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_007', 'healthtech', 'industry', 'healthcare', 'Healthcare technology companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_008', 'edtech', 'industry', 'education', 'Education technology companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_009', 'cybersecurity', 'industry', 'technology', 'Cybersecurity and security companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_010', 'artificial intelligence', 'industry', 'technology', 'AI and machine learning companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Healthcare Industry Keywords
('ktm_011', 'healthcare', 'industry', 'healthcare', 'Healthcare and medical services', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_012', 'biotech', 'industry', 'healthcare', 'Biotechnology companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_013', 'pharmaceutical', 'industry', 'healthcare', 'Pharmaceutical companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_014', 'medical device', 'industry', 'healthcare', 'Medical device manufacturers', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_015', 'digital health', 'industry', 'healthcare', 'Digital healthcare companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Financial Services Keywords
('ktm_016', 'banking', 'industry', 'financial_services', 'Banking and financial institutions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_017', 'insurance', 'industry', 'financial_services', 'Insurance companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_018', 'investment', 'industry', 'financial_services', 'Investment and asset management', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_019', 'payments', 'industry', 'financial_services', 'Payment processing companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_020', 'cryptocurrency', 'industry', 'financial_services', 'Cryptocurrency and blockchain companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- E-commerce and Retail Keywords
('ktm_021', 'ecommerce', 'industry', 'retail', 'E-commerce companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_022', 'retail', 'industry', 'retail', 'Retail companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_023', 'marketplace', 'industry', 'retail', 'Online marketplace platforms', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_024', 'consumer goods', 'industry', 'retail', 'Consumer products companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_025', 'fashion', 'industry', 'retail', 'Fashion and apparel companies', 1.0, 'medium', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Media and Entertainment Keywords
('ktm_026', 'media', 'industry', 'media', 'Media and publishing companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_027', 'entertainment', 'industry', 'media', 'Entertainment companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_028', 'gaming', 'industry', 'media', 'Gaming and interactive entertainment', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_029', 'streaming', 'industry', 'media', 'Streaming and digital content', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_030', 'social media', 'industry', 'media', 'Social media platforms', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Company Stage Mappings
-- ======================

-- Early Stage Company Keywords
('ktm_031', 'startup', 'company_stage', 'early_stage', 'Early stage startup companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_032', 'early stage', 'company_stage', 'early_stage', 'Early stage companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_033', 'pre-seed', 'company_stage', 'early_stage', 'Pre-seed funding stage', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_034', 'seed', 'company_stage', 'early_stage', 'Seed funding stage', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_035', 'series a', 'company_stage', 'growth_stage', 'Series A funding stage', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Growth Stage Keywords
('ktm_036', 'growth stage', 'company_stage', 'growth_stage', 'Growth stage companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_037', 'series b', 'company_stage', 'growth_stage', 'Series B funding stage', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_038', 'series c', 'company_stage', 'growth_stage', 'Series C funding stage', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_039', 'scale up', 'company_stage', 'growth_stage', 'Scale-up stage companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_040', 'unicorn', 'company_stage', 'growth_stage', 'Unicorn companies (1B+ valuation)', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Late Stage and Public Keywords
('ktm_041', 'late stage', 'company_stage', 'late_stage', 'Late stage private companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_042', 'pre-ipo', 'company_stage', 'late_stage', 'Pre-IPO stage companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_043', 'public company', 'company_stage', 'public', 'Publicly traded companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_044', 'fortune 500', 'company_stage', 'public', 'Fortune 500 companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_045', 'enterprise', 'company_stage', 'established', 'Enterprise companies', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Role Type Mappings
-- ==================

-- Individual Contributor Levels
('ktm_046', 'individual contributor', 'role_type', 'individual_contributor', 'Individual contributor roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_047', 'ic', 'role_type', 'individual_contributor', 'Individual contributor roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_048', 'entry level', 'role_type', 'entry_level', 'Entry level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_049', 'junior', 'role_type', 'entry_level', 'Junior level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_050', 'associate', 'role_type', 'entry_level', 'Associate level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Mid-Level Roles
('ktm_051', 'mid level', 'role_type', 'mid_level', 'Mid-level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_052', 'intermediate', 'role_type', 'mid_level', 'Intermediate level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_053', 'experienced', 'role_type', 'mid_level', 'Experienced professional positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Senior Individual Contributor Roles
('ktm_054', 'senior', 'role_type', 'senior_level', 'Senior level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_055', 'sr', 'role_type', 'senior_level', 'Senior level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_056', 'staff', 'role_type', 'staff_level', 'Staff level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_057', 'principal', 'role_type', 'principal_level', 'Principal level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_058', 'lead', 'role_type', 'lead_level', 'Lead level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Management Hierarchy
('ktm_059', 'manager', 'role_type', 'manager', 'Management positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_060', 'team lead', 'role_type', 'manager', 'Team leadership positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_061', 'senior manager', 'role_type', 'senior_manager', 'Senior management positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_062', 'director', 'role_type', 'director', 'Director level positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_063', 'senior director', 'role_type', 'senior_director', 'Senior director positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_064', 'vice president', 'role_type', 'vp', 'Vice president positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_065', 'vp', 'role_type', 'vp', 'Vice president positions', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Work Style Mappings
-- ===================

-- Remote Work Culture
('ktm_066', 'remote first', 'work_style', 'remote_culture', 'Remote-first work culture', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_067', 'fully remote', 'work_style', 'remote_arrangement', 'Fully remote work arrangement', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_068', 'distributed team', 'work_style', 'remote_arrangement', 'Distributed workforce', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_069', 'remote friendly', 'work_style', 'remote_culture', 'Remote-friendly work culture', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Hybrid and Flexible Work
('ktm_070', 'hybrid work', 'work_style', 'hybrid_arrangement', 'Hybrid work model', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_071', 'flexible schedule', 'work_style', 'flexible_arrangement', 'Flexible work schedule', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_072', 'work life balance', 'work_style', 'culture_values', 'Work-life balance focus', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_073', 'flexible hours', 'work_style', 'flexible_arrangement', 'Flexible working hours', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Company Culture Attributes
('ktm_074', 'fast paced', 'work_style', 'culture_pace', 'Fast-paced work environment', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_075', 'high growth', 'work_style', 'culture_growth', 'High growth environment', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_076', 'collaborative', 'work_style', 'culture_values', 'Collaborative work culture', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_077', 'innovative', 'work_style', 'culture_values', 'Innovation-focused culture', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_078', 'data driven', 'work_style', 'culture_methodology', 'Data-driven decision making', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_079', 'agile environment', 'work_style', 'culture_methodology', 'Agile work methodology', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_080', 'lean startup', 'work_style', 'culture_methodology', 'Lean startup methodology', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Function Type Mappings
-- ======================

-- Engineering Functions
('ktm_081', 'engineering', 'function_type', 'engineering', 'Engineering roles and teams', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_082', 'software engineering', 'function_type', 'engineering', 'Software engineering focus', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_083', 'backend focus', 'function_type', 'engineering', 'Backend engineering specialization', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_084', 'frontend focus', 'function_type', 'engineering', 'Frontend engineering specialization', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_085', 'full stack', 'function_type', 'engineering', 'Full-stack engineering', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Data and Analytics
('ktm_086', 'data science', 'function_type', 'data', 'Data science roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_087', 'analytics', 'function_type', 'data', 'Analytics and business intelligence', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_088', 'machine learning', 'function_type', 'data', 'Machine learning and AI roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Product Functions
('ktm_089', 'product management', 'function_type', 'product', 'Product management roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_090', 'product design', 'function_type', 'product', 'Product design roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_091', 'ux design', 'function_type', 'product', 'User experience design', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_092', 'ui design', 'function_type', 'product', 'User interface design', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Sales and Marketing
('ktm_093', 'sales', 'function_type', 'sales', 'Sales roles and teams', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_094', 'marketing', 'function_type', 'marketing', 'Marketing roles and teams', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_095', 'business development', 'function_type', 'sales', 'Business development roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_096', 'customer success', 'function_type', 'customer', 'Customer success roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Operations and Support
('ktm_097', 'operations', 'function_type', 'operations', 'Operations roles and teams', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_098', 'customer support', 'function_type', 'customer', 'Customer support roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_099', 'devops', 'function_type', 'engineering', 'DevOps and infrastructure', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ktm_100', 'security', 'function_type', 'security', 'Security and compliance roles', 1.0, 'high', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Summary Information
SELECT
    'Keyword Type Mappings Loaded' as status,
    COUNT(*) as total_mappings,
    COUNT(DISTINCT KEYWORD_TYPE) as types_covered,
    COUNT(DISTINCT KEYWORD_CATEGORY) as categories_covered,
    COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_mappings,
    COUNT(CASE WHEN BUSINESS_RELEVANCE = 'high' THEN 1 END) as high_relevance_mappings
FROM BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING;