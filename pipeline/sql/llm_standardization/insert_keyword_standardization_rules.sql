/*
 * Keyword Standardization Rules Configuration
 *
 * This file contains INSERT statements for populating the KEYWORD_STANDARDIZATION_RULES table
 * with comprehensive keyword standardization and normalization rules.
 *
 * Categories covered:
 * - Industry keyword standardization (FinTech -> Financial Technology)
 * - Role type normalization (IC -> Individual Contributor)
 * - Company stage standardization (Series A -> Growth Stage)
 * - Function type standardization
 * - Work style normalization
 *
 * Usage:
 * Execute this file after creating the KEYWORD_STANDARDIZATION_RULES table
 * to populate it with standardization rules.
 */

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;


-- Clear existing rules (optional - use for clean setup)
-- DELETE FROM BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES;

-- Industry Keyword Standardization Rules
-- ====================================

-- FinTech and Financial Services
INSERT INTO BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES VALUES
('ksr_001', 'fintech', 'Financial Technology', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_002', 'fin tech', 'Financial Technology', 'industry', 'financial_services', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_003', 'financial technology', 'Financial Technology', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_004', 'banking', 'Banking & Financial Services', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_005', 'payments', 'Payments & Financial Services', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_006', 'crypto', 'Cryptocurrency & Blockchain', 'industry', 'financial_services', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_007', 'cryptocurrency', 'Cryptocurrency & Blockchain', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_008', 'blockchain', 'Cryptocurrency & Blockchain', 'industry', 'financial_services', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_009', 'defi', 'Decentralized Finance', 'industry', 'financial_services', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_010', 'insurtech', 'Insurance Technology', 'industry', 'financial_services', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- SaaS and Software
('ksr_011', 'saas', 'Software as a Service', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_012', 'b2b saas', 'B2B Software as a Service', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_013', 'b2c saas', 'B2C Software as a Service', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_014', 'enterprise software', 'Enterprise Software', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_015', 'software development', 'Software Development', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_016', 'cybersecurity', 'Cybersecurity & Security', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_017', 'cyber security', 'Cybersecurity & Security', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_018', 'information security', 'Cybersecurity & Security', 'industry', 'software', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_019', 'cloud computing', 'Cloud Services', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_020', 'paas', 'Platform as a Service', 'industry', 'software', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Healthcare and Biotech
('ksr_021', 'healthtech', 'Healthcare Technology', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_022', 'health tech', 'Healthcare Technology', 'industry', 'healthcare', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_023', 'digital health', 'Digital Healthcare', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_024', 'biotech', 'Biotechnology', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_025', 'biotechnology', 'Biotechnology', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_026', 'pharmaceutical', 'Pharmaceutical', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_027', 'pharma', 'Pharmaceutical', 'industry', 'healthcare', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_028', 'medtech', 'Medical Technology', 'industry', 'healthcare', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_029', 'medical device', 'Medical Technology', 'industry', 'healthcare', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_030', 'telemedicine', 'Digital Healthcare', 'industry', 'healthcare', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- E-commerce and Retail
('ksr_031', 'ecommerce', 'E-commerce', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_032', 'e-commerce', 'E-commerce', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_033', 'online retail', 'E-commerce', 'industry', 'retail', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_034', 'marketplace', 'Online Marketplace', 'industry', 'retail', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_035', 'retail technology', 'Retail Technology', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_036', 'consumer goods', 'Consumer Products', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_037', 'fashion tech', 'Fashion Technology', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_038', 'luxury goods', 'Luxury & Premium Products', 'industry', 'retail', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_039', 'supply chain', 'Supply Chain & Logistics', 'industry', 'retail', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_040', 'logistics', 'Supply Chain & Logistics', 'industry', 'retail', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- EdTech and Education
('ksr_041', 'edtech', 'Education Technology', 'industry', 'education', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_042', 'ed tech', 'Education Technology', 'industry', 'education', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_043', 'education technology', 'Education Technology', 'industry', 'education', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_044', 'e-learning', 'Online Learning', 'industry', 'education', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_045', 'online education', 'Online Learning', 'industry', 'education', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_046', 'learning management', 'Learning Management Systems', 'industry', 'education', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_047', 'training', 'Professional Training', 'industry', 'education', 0.7, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_048', 'corporate training', 'Professional Training', 'industry', 'education', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_049', 'skill development', 'Professional Training', 'industry', 'education', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_050', 'mooc', 'Online Learning', 'industry', 'education', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Role Type Standardization Rules
-- ===============================

-- Individual Contributor Variations
('ksr_051', 'ic', 'Individual Contributor', 'role_type', 'hierarchy', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_052', 'individual contributor', 'Individual Contributor', 'role_type', 'hierarchy', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_053', 'non-manager', 'Individual Contributor', 'role_type', 'hierarchy', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_054', 'non manager', 'Individual Contributor', 'role_type', 'hierarchy', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_055', 'contributor', 'Individual Contributor', 'role_type', 'hierarchy', 0.7, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Seniority Level Variations
('ksr_056', 'entry level', 'Entry Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_057', 'junior', 'Entry Level', 'role_type', 'seniority', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_058', 'jr', 'Entry Level', 'role_type', 'seniority', 0.85, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_059', 'associate', 'Entry Level', 'role_type', 'seniority', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_060', 'mid level', 'Mid Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_061', 'mid-level', 'Mid Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_062', 'intermediate', 'Mid Level', 'role_type', 'seniority', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_063', 'experienced', 'Mid Level', 'role_type', 'seniority', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_064', 'senior', 'Senior Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_065', 'sr', 'Senior Level', 'role_type', 'seniority', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_066', 'senior level', 'Senior Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_067', 'staff', 'Staff Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_068', 'principal', 'Principal Level', 'role_type', 'seniority', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_069', 'lead', 'Lead Level', 'role_type', 'seniority', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_070', 'team lead', 'Lead Level', 'role_type', 'seniority', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Management Hierarchy
('ksr_071', 'manager', 'Manager', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_072', 'team manager', 'Manager', 'role_type', 'management', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_073', 'people manager', 'Manager', 'role_type', 'management', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_074', 'supervisor', 'Manager', 'role_type', 'management', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_075', 'senior manager', 'Senior Manager', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_076', 'director', 'Director', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_077', 'senior director', 'Senior Director', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_078', 'vp', 'Vice President', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_079', 'vice president', 'Vice President', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_080', 'svp', 'Senior Vice President', 'role_type', 'management', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Company Stage Standardization Rules
-- ===================================

('ksr_081', 'startup', 'Early Stage Startup', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_082', 'early stage', 'Early Stage Startup', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_083', 'pre-seed', 'Pre-Seed Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_084', 'seed', 'Seed Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_085', 'seed stage', 'Seed Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_086', 'series a', 'Series A Growth', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_087', 'series b', 'Series B Growth', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_088', 'series c', 'Series C Growth', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_089', 'growth stage', 'Growth Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_090', 'scale up', 'Scale-up Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_091', 'scaleup', 'Scale-up Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_092', 'unicorn', 'Unicorn Company', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_093', 'late stage', 'Late Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_094', 'pre-ipo', 'Pre-IPO Stage', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_095', 'public company', 'Public Company', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_096', 'enterprise', 'Enterprise Company', 'company_stage', 'growth_phase', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_097', 'fortune 500', 'Fortune 500', 'company_stage', 'growth_phase', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_098', 'multinational', 'Multinational Corporation', 'company_stage', 'growth_phase', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_099', 'established', 'Established Company', 'company_stage', 'growth_phase', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_100', 'mature', 'Mature Company', 'company_stage', 'growth_phase', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Work Style and Culture Keywords
-- ===============================

('ksr_101', 'remote first', 'Remote-First Culture', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_102', 'remote-first', 'Remote-First Culture', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_103', 'fully remote', 'Fully Remote', 'work_style', 'arrangement', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_104', 'distributed team', 'Distributed Workforce', 'work_style', 'arrangement', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_105', 'hybrid work', 'Hybrid Work Model', 'work_style', 'arrangement', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_106', 'flexible schedule', 'Flexible Work Schedule', 'work_style', 'arrangement', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_107', 'work life balance', 'Work-Life Balance', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_108', 'work-life balance', 'Work-Life Balance', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_109', 'fast paced', 'Fast-Paced Environment', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_110', 'fast-paced', 'Fast-Paced Environment', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_111', 'high growth', 'High Growth Environment', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_112', 'collaborative', 'Collaborative Culture', 'work_style', 'culture', 0.9, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_113', 'agile environment', 'Agile Work Environment', 'work_style', 'methodology', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_114', 'lean startup', 'Lean Startup Methodology', 'work_style', 'methodology', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_115', 'data driven', 'Data-Driven Culture', 'work_style', 'culture', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Technology Focus Areas
-- ======================

('ksr_116', 'ai/ml', 'Artificial Intelligence & Machine Learning', 'technology', 'ai_ml', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_117', 'artificial intelligence', 'Artificial Intelligence & Machine Learning', 'technology', 'ai_ml', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_118', 'machine learning', 'Artificial Intelligence & Machine Learning', 'technology', 'ai_ml', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_119', 'deep learning', 'Artificial Intelligence & Machine Learning', 'technology', 'ai_ml', 0.95, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_120', 'nlp', 'Natural Language Processing', 'technology', 'ai_ml', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_121', 'computer vision', 'Computer Vision & AI', 'technology', 'ai_ml', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_122', 'iot', 'Internet of Things', 'technology', 'hardware', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_123', 'internet of things', 'Internet of Things', 'technology', 'hardware', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_124', 'robotics', 'Robotics & Automation', 'technology', 'hardware', 1.0, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ksr_125', 'automation', 'Robotics & Automation', 'technology', 'hardware', 0.8, 'exact_match', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Summary Information
SELECT
    'Keyword Standardization Rules Loaded' as status,
    COUNT(*) as total_rules,
    COUNT(DISTINCT KEYWORD_TYPE) as types_covered,
    COUNT(DISTINCT KEYWORD_CATEGORY) as categories_covered,
    COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_rules
FROM BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES;