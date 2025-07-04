-- =========================================================================
-- Skill Standardization Rules Setup
-- =========================================================================
-- This file contains all skill standardization rules for the LLM data
-- normalization process. It should be executed once during initial setup
-- or when rules need to be updated.
--
-- Usage:
--   1. Initial setup: Run this entire file
--   2. Add new rules: Add INSERT statements at the end
--   3. Update rules: Use UPDATE statements with specific RULE_IDs
--
-- Note: This keeps static data separate from Dagster asset logic for
-- better maintainability and version control.
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- Clear existing rules (optional - comment out if you want to preserve existing data)
-- DELETE FROM SKILL_STANDARDIZATION_RULES;

-- Comprehensive Skill Standardization Rules
INSERT INTO SKILL_STANDARDIZATION_RULES
(RULE_ID, PATTERN, STANDARDIZED_NAME, SKILL_CATEGORY, SKILL_SUBCATEGORY, CONFIDENCE_SCORE, RULE_TYPE, CREATED_TIMESTAMP)
VALUES

-- =============================================================================
-- PROGRAMMING LANGUAGES
-- =============================================================================

-- Python variants
('rule_001', 'python', 'Python', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_002', 'python3', 'Python', 'languages', 'backend_language', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_003', 'py', 'Python', 'languages', 'backend_language', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_004', 'python programming', 'Python', 'languages', 'backend_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- JavaScript variants
('rule_005', 'javascript', 'JavaScript', 'languages', 'frontend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_006', 'js', 'JavaScript', 'languages', 'frontend_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_007', 'ecmascript', 'JavaScript', 'languages', 'frontend_language', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_008', 'node.js', 'Node.js', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_009', 'nodejs', 'Node.js', 'languages', 'backend_language', 0.95, 'exact_match', CURRENT_TIMESTAMP),

-- Java variants
('rule_010', 'java', 'Java', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_011', 'java programming', 'Java', 'languages', 'backend_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- TypeScript variants
('rule_012', 'typescript', 'TypeScript', 'languages', 'frontend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_013', 'ts', 'TypeScript', 'languages', 'frontend_language', 0.85, 'exact_match', CURRENT_TIMESTAMP),

-- C++ variants
('rule_014', 'c++', 'C++', 'languages', 'system_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_015', 'cpp', 'C++', 'languages', 'system_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- C# variants
('rule_016', 'c#', 'C#', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_017', 'csharp', 'C#', 'languages', 'backend_language', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Additional languages
('rule_018', 'go', 'Go', 'languages', 'backend_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_019', 'golang', 'Go', 'languages', 'backend_language', 0.95, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- FRAMEWORKS
-- =============================================================================

-- React variants
('rule_020', 'react', 'React', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_021', 'react.js', 'React', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_022', 'reactjs', 'React', 'frameworks', 'frontend_framework', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_023', 'react framework', 'React', 'frameworks', 'frontend_framework', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Angular variants
('rule_024', 'angular', 'Angular', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_025', 'angularjs', 'AngularJS', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Vue variants
('rule_026', 'vue', 'Vue.js', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_027', 'vue.js', 'Vue.js', 'frameworks', 'frontend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_028', 'vuejs', 'Vue.js', 'frameworks', 'frontend_framework', 0.95, 'exact_match', CURRENT_TIMESTAMP),

-- Python frameworks
('rule_029', 'django', 'Django', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_030', 'django framework', 'Django', 'frameworks', 'backend_framework', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_031', 'flask', 'Flask', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Java frameworks
('rule_032', 'spring', 'Spring Framework', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_033', 'spring boot', 'Spring Boot', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Node.js frameworks
('rule_034', 'express', 'Express.js', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_035', 'express.js', 'Express.js', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_036', 'nestjs', 'NestJS', 'frameworks', 'backend_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Robotics frameworks
('rule_037', 'ros', 'Robot Operating System', 'frameworks', 'robotics_framework', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- DATABASES
-- =============================================================================

-- PostgreSQL variants
('rule_040', 'postgresql', 'PostgreSQL', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_041', 'postgres', 'PostgreSQL', 'databases', 'relational_database', 0.95, 'exact_match', CURRENT_TIMESTAMP),
('rule_042', 'pgsql', 'PostgreSQL', 'databases', 'relational_database', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- MySQL variants
('rule_043', 'mysql', 'MySQL', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_044', 'my sql', 'MySQL', 'databases', 'relational_database', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- MongoDB variants
('rule_045', 'mongodb', 'MongoDB', 'databases', 'nosql_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_046', 'mongo db', 'MongoDB', 'databases', 'nosql_database', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_047', 'mongo', 'MongoDB', 'databases', 'nosql_database', 0.8, 'exact_match', CURRENT_TIMESTAMP),

-- Other databases
('rule_048', 'redis', 'Redis', 'databases', 'cache_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_049', 'sqlite', 'SQLite', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_050', 'oracle', 'Oracle Database', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_051', 'sql server', 'Microsoft SQL Server', 'databases', 'relational_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_052', 'cassandra', 'Apache Cassandra', 'databases', 'nosql_database', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- CLOUD PLATFORMS
-- =============================================================================

-- AWS variants
('rule_060', 'aws', 'Amazon Web Services', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_061', 'amazon web services', 'Amazon Web Services', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_062', 'amazon cloud', 'Amazon Web Services', 'cloud', 'public_cloud', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Azure variants
('rule_063', 'azure', 'Microsoft Azure', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_064', 'microsoft azure', 'Microsoft Azure', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_065', 'ms azure', 'Microsoft Azure', 'cloud', 'public_cloud', 0.95, 'exact_match', CURRENT_TIMESTAMP),

-- Google Cloud variants
('rule_066', 'gcp', 'Google Cloud Platform', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_067', 'google cloud', 'Google Cloud Platform', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_068', 'google cloud platform', 'Google Cloud Platform', 'cloud', 'public_cloud', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- DEVELOPMENT TOOLS
-- =============================================================================

-- Containerization
('rule_080', 'docker', 'Docker', 'tools', 'containerization', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_081', 'containerization', 'Docker', 'tools', 'containerization', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_082', 'docker containers', 'Docker', 'tools', 'containerization', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Orchestration
('rule_083', 'kubernetes', 'Kubernetes', 'tools', 'orchestration', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_084', 'k8s', 'Kubernetes', 'tools', 'orchestration', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Version control
('rule_085', 'git', 'Git', 'tools', 'version_control', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_086', 'version control', 'Git', 'tools', 'version_control', 0.7, 'exact_match', CURRENT_TIMESTAMP),
('rule_087', 'github', 'GitHub', 'tools', 'code_hosting', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_088', 'gitlab', 'GitLab', 'tools', 'code_hosting', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- CI/CD
('rule_089', 'jenkins', 'Jenkins', 'tools', 'ci_cd', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_090', 'github actions', 'GitHub Actions', 'tools', 'ci_cd', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- KEYWORDS AND CONCEPTS
-- =============================================================================

-- Machine Learning
('rule_100', 'ml', 'Machine Learning', 'keyword', 'technology', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_101', 'machine learning', 'Machine Learning', 'keyword', 'technology', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_102', 'ai', 'Artificial Intelligence', 'keyword', 'technology', 0.8, 'exact_match', CURRENT_TIMESTAMP),
('rule_103', 'artificial intelligence', 'Artificial Intelligence', 'keyword', 'technology', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- API Development
('rule_104', 'api', 'API Development', 'keyword', 'technology', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_105', 'rest api', 'REST API', 'keyword', 'technology', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_106', 'restful', 'REST API', 'keyword', 'technology', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_107', 'graphql', 'GraphQL', 'keyword', 'technology', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- Architecture
('rule_108', 'microservices', 'Microservices', 'keyword', 'architecture', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_109', 'micro services', 'Microservices', 'keyword', 'architecture', 0.9, 'exact_match', CURRENT_TIMESTAMP),

-- Methodologies
('rule_110', 'devops', 'DevOps', 'keyword', 'methodology', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_111', 'dev ops', 'DevOps', 'keyword', 'methodology', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_112', 'agile', 'Agile', 'keyword', 'methodology', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_113', 'scrum', 'Scrum', 'keyword', 'methodology', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- PRODUCTIVITY TOOLS (Microsoft Office Suite)
-- =============================================================================

('rule_120', 'excel', 'Microsoft Excel', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_121', 'microsoft excel', 'Microsoft Excel', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_122', 'word', 'Microsoft Word', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_123', 'microsoft word', 'Microsoft Word', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_124', 'outlook', 'Microsoft Outlook', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_125', 'microsoft outlook', 'Microsoft Outlook', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_126', 'powerpoint', 'Microsoft PowerPoint', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_127', 'microsoft powerpoint', 'Microsoft PowerPoint', 'tools', 'productivity', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_128', 'microsoft project', 'Microsoft Project', 'tools', 'project_management', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- ENTERPRISE SOFTWARE
-- =============================================================================

('rule_130', 'sap', 'SAP', 'tools', 'enterprise_software', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_131', 'sap (preferred)', 'SAP', 'tools', 'enterprise_software', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_132', 'cognos', 'IBM Cognos', 'tools', 'business_intelligence', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_133', 'maximo', 'IBM Maximo', 'tools', 'asset_management', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_134', 'maximo (preferred)', 'IBM Maximo', 'tools', 'asset_management', 0.9, 'exact_match', CURRENT_TIMESTAMP),
('rule_135', 'tableau', 'Tableau', 'tools', 'business_intelligence', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_136', 'power bi', 'Microsoft Power BI', 'tools', 'business_intelligence', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_137', 'powerbi', 'Microsoft Power BI', 'tools', 'business_intelligence', 0.95, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- SPECIALIZED SIMULATION TOOLS (from sample data)
-- =============================================================================

('rule_140', 'foretify', 'Foretify', 'tools', 'simulation', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_141', 'simian', 'Simian', 'tools', 'simulation', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- COMMUNICATION AND COLLABORATION TOOLS
-- =============================================================================

('rule_150', 'slack', 'Slack', 'tools', 'communication', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_151', 'teams', 'Microsoft Teams', 'tools', 'communication', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_152', 'microsoft teams', 'Microsoft Teams', 'tools', 'communication', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_153', 'zoom', 'Zoom', 'tools', 'communication', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- PROJECT MANAGEMENT TOOLS
-- =============================================================================

('rule_160', 'jira', 'JIRA', 'tools', 'project_management', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_161', 'confluence', 'Confluence', 'tools', 'documentation', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_162', 'trello', 'Trello', 'tools', 'project_management', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_163', 'asana', 'Asana', 'tools', 'project_management', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- DATA TOOLS
-- =============================================================================

('rule_170', 'hadoop', 'Apache Hadoop', 'tools', 'big_data', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_171', 'spark', 'Apache Spark', 'tools', 'big_data', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_172', 'kafka', 'Apache Kafka', 'tools', 'streaming', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_173', 'airflow', 'Apache Airflow', 'tools', 'orchestration', 1.0, 'exact_match', CURRENT_TIMESTAMP),

-- =============================================================================
-- WEB TECHNOLOGIES
-- =============================================================================

('rule_180', 'html', 'HTML', 'languages', 'markup_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_181', 'css', 'CSS', 'languages', 'stylesheet_language', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_182', 'sass', 'Sass', 'tools', 'css_preprocessor', 1.0, 'exact_match', CURRENT_TIMESTAMP),
('rule_183', 'scss', 'Sass', 'tools', 'css_preprocessor', 0.95, 'exact_match', CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these to verify the rules were inserted correctly

-- Check total rule count by category
SELECT
    SKILL_CATEGORY,
    COUNT(*) as RULE_COUNT,
    COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as HIGH_CONFIDENCE_COUNT
FROM SKILL_STANDARDIZATION_RULES
GROUP BY SKILL_CATEGORY
ORDER BY RULE_COUNT DESC;

-- Check for duplicate patterns
SELECT
    PATTERN,
    COUNT(*) as DUPLICATE_COUNT
FROM SKILL_STANDARDIZATION_RULES
GROUP BY PATTERN
HAVING COUNT(*) > 1;

-- Sample rules by category
SELECT
    SKILL_CATEGORY,
    PATTERN,
    STANDARDIZED_NAME,
    CONFIDENCE_SCORE
FROM SKILL_STANDARDIZATION_RULES
WHERE SKILL_CATEGORY IN ('languages', 'frameworks', 'tools', 'keyword')
ORDER BY SKILL_CATEGORY, CONFIDENCE_SCORE DESC
LIMIT 20;

-- -------------------------------------------------------------------------
-- NOTE ON AI SKILL CATEGORIZATION (ENHANCEMENT-036)
-- -------------------------------------------------------------------------
-- As of ENHANCEMENT-036 (2025-07-03) we bucket AI-related raw skill names
-- under the umbrella category `Artificial Intelligence` directly inside the
-- Python asset `stage_skills_normalized` (see
-- `pipeline/dagster_betterjobs/dagster_betterjobs/assets/llm_standardization/skills_normalization.py`)
-- via an inline Common Table Expression (CTE) named `ai_skill_category`.
-- This avoids adding dozens of near-duplicate rows here while we evaluate a
-- longer-term mapping/rule-based strategy.
--
-- Impact:
--   • The CTE overrides both `SKILL_CATEGORY` and `SKILL_SUBCATEGORY` before
--     aggregation. No additional rows are required in this rule file for AI
--     variants such as "ai-ops", "ai tools", etc.
--   • Once a dedicated mapping table or rule set is in place this comment
--     will be removed and the CTE deleted.
-- -------------------------------------------------------------------------