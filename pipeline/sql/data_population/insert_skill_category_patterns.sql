-- =========================================================================
-- Skill Category Detection Patterns Setup
-- =========================================================================
-- This file contains regex patterns for automatic skill category detection.
-- These patterns help categorize skills when they don't match exact
-- standardization rules.
--
-- Usage:
--   Run this file once during initial setup or when patterns need updates
--
-- Pattern Types:
--   - regex: Regular expression patterns for flexible matching
--   - exact_match: Exact string matching
--   - contains: Substring matching
--
-- Categories:
--   - languages: Programming languages and markup
--   - frameworks: Development frameworks and libraries
--   - databases: Database technologies
--   - cloud: Cloud platforms and services
--   - tools: Development and productivity tools
-- =========================================================================

USE DATABASE BETTERJOBS_DB;
USE SCHEMA STAGE;

-- Clear existing patterns (optional - comment out to preserve existing data)
-- DELETE FROM SKILL_CATEGORY_PATTERNS;

-- Comprehensive Skill Category Detection Patterns
INSERT INTO SKILL_CATEGORY_PATTERNS
(PATTERN_ID, SKILL_CATEGORY, PATTERN, PATTERN_TYPE, CONFIDENCE_SCORE, DESCRIPTION, IS_ACTIVE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP)
VALUES

-- =============================================================================
-- PROGRAMMING LANGUAGES PATTERNS
-- =============================================================================

('cat_001', 'languages', '\\bpython\\b', 'regex', 1.0, 'Python programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_002', 'languages', '\\bjavascript\\b', 'regex', 1.0, 'JavaScript programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_003', 'languages', '\\bjava\\b', 'regex', 1.0, 'Java programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_004', 'languages', '\\bc\\+\\+\\b', 'regex', 1.0, 'C++ programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_005', 'languages', '\\bc#\\b', 'regex', 1.0, 'C# programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_006', 'languages', '\\bruntime\\b', 'regex', 0.8, 'Runtime environments', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_007', 'languages', '\\bprogramming\\b', 'regex', 0.7, 'General programming references', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_008', 'languages', '\\blanguage\\b', 'regex', 0.6, 'Language keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Additional programming languages
('cat_009', 'languages', '\\btypescript\\b', 'regex', 1.0, 'TypeScript programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_010', 'languages', '\\bgo\\b', 'regex', 0.9, 'Go programming language (may have false positives)', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_011', 'languages', '\\bgolang\\b', 'regex', 1.0, 'Go programming language (golang)', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_012', 'languages', '\\brust\\b', 'regex', 0.9, 'Rust programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_013', 'languages', '\\bswift\\b', 'regex', 0.8, 'Swift programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_014', 'languages', '\\bkotlin\\b', 'regex', 1.0, 'Kotlin programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_015', 'languages', '\\bscala\\b', 'regex', 1.0, 'Scala programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_016', 'languages', '\\bruby\\b', 'regex', 0.9, 'Ruby programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_017', 'languages', '\\bphp\\b', 'regex', 1.0, 'PHP programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_018', 'languages', '\\bperl\\b', 'regex', 0.9, 'Perl programming language', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_019', 'languages', '\\br\\b', 'regex', 0.7, 'R programming language (short pattern)', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- FRAMEWORKS PATTERNS
-- =============================================================================

('cat_020', 'frameworks', '\\bframework\\b', 'regex', 1.0, 'Framework keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_021', 'frameworks', '\\breact\\b', 'regex', 1.0, 'React framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_022', 'frameworks', '\\bangular\\b', 'regex', 1.0, 'Angular framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_023', 'frameworks', '\\bvue\\b', 'regex', 1.0, 'Vue.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_024', 'frameworks', '\\bdjango\\b', 'regex', 1.0, 'Django framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_025', 'frameworks', '\\bspring\\b', 'regex', 1.0, 'Spring framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_026', 'frameworks', '\\bexpress\\b', 'regex', 1.0, 'Express.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_027', 'frameworks', '\\.js$', 'regex', 0.8, 'JavaScript framework extensions', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_028', 'frameworks', '\\.ts$', 'regex', 0.8, 'TypeScript framework extensions', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Additional frameworks
('cat_029', 'frameworks', '\\bflask\\b', 'regex', 1.0, 'Flask framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_030', 'frameworks', '\\blaravel\\b', 'regex', 1.0, 'Laravel framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_031', 'frameworks', '\\bruby on rails\\b', 'regex', 1.0, 'Ruby on Rails framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_032', 'frameworks', '\\brails\\b', 'regex', 0.9, 'Rails framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_033', 'frameworks', '\\bnextjs\\b', 'regex', 1.0, 'Next.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_034', 'frameworks', '\\bnuxt\\b', 'regex', 1.0, 'Nuxt.js framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_035', 'frameworks', '\\bgatsby\\b', 'regex', 1.0, 'Gatsby framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_036', 'frameworks', '\\bsvelte\\b', 'regex', 1.0, 'Svelte framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- DATABASE PATTERNS
-- =============================================================================

('cat_040', 'databases', '\\bdatabase\\b', 'regex', 1.0, 'Database keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_041', 'databases', '\\bsql\\b', 'regex', 1.0, 'SQL databases', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_042', 'databases', '\\bmysql\\b', 'regex', 1.0, 'MySQL database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_043', 'databases', '\\bpostgresql\\b', 'regex', 1.0, 'PostgreSQL database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_044', 'databases', '\\bmongodb\\b', 'regex', 1.0, 'MongoDB database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_045', 'databases', '\\bredis\\b', 'regex', 1.0, 'Redis database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_046', 'databases', '\\boracle\\b', 'regex', 1.0, 'Oracle database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Additional databases
('cat_047', 'databases', '\\bcassandra\\b', 'regex', 1.0, 'Cassandra database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_048', 'databases', '\\belasticsearch\\b', 'regex', 1.0, 'Elasticsearch database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_049', 'databases', '\\bsqlite\\b', 'regex', 1.0, 'SQLite database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_050', 'databases', '\\bmariadb\\b', 'regex', 1.0, 'MariaDB database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_051', 'databases', '\\bdynamodb\\b', 'regex', 1.0, 'DynamoDB database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_052', 'databases', '\\bneo4j\\b', 'regex', 1.0, 'Neo4j graph database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_053', 'databases', '\\binfluxdb\\b', 'regex', 1.0, 'InfluxDB time series database', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- CLOUD PLATFORMS PATTERNS
-- =============================================================================

('cat_060', 'cloud', '\\baws\\b', 'regex', 1.0, 'Amazon Web Services', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_061', 'cloud', '\\bazure\\b', 'regex', 1.0, 'Microsoft Azure', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_062', 'cloud', '\\bgcp\\b', 'regex', 1.0, 'Google Cloud Platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_063', 'cloud', '\\bcloud\\b', 'regex', 1.0, 'Cloud keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_064', 'cloud', '\\bkubernetes\\b', 'regex', 1.0, 'Kubernetes orchestration', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_065', 'cloud', '\\bcontainer\\b', 'regex', 0.8, 'Container technologies', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Additional cloud services
('cat_066', 'cloud', '\\bheroku\\b', 'regex', 1.0, 'Heroku cloud platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_067', 'cloud', '\\bvercel\\b', 'regex', 1.0, 'Vercel cloud platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_068', 'cloud', '\\bnetlify\\b', 'regex', 1.0, 'Netlify cloud platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_069', 'cloud', '\\bdigitalocean\\b', 'regex', 1.0, 'DigitalOcean cloud platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_070', 'cloud', '\\blinode\\b', 'regex', 1.0, 'Linode cloud platform', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- TOOLS PATTERNS
-- =============================================================================

('cat_080', 'tools', '\\btool\\b', 'regex', 0.7, 'Tool keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_081', 'tools', '\\bgit\\b', 'regex', 1.0, 'Git version control', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_082', 'tools', '\\bdocker\\b', 'regex', 1.0, 'Docker containerization', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_083', 'tools', '\\bjenkins\\b', 'regex', 1.0, 'Jenkins CI/CD', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_084', 'tools', '\\bslack\\b', 'regex', 0.9, 'Slack communication tool', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_085', 'tools', '\\bjira\\b', 'regex', 1.0, 'JIRA project management', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- Additional development tools
('cat_086', 'tools', '\\bwebpack\\b', 'regex', 1.0, 'Webpack build tool', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_087', 'tools', '\\bbabel\\b', 'regex', 0.9, 'Babel transpiler', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_088', 'tools', '\\bterraform\\b', 'regex', 1.0, 'Terraform infrastructure tool', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_089', 'tools', '\\bansible\\b', 'regex', 1.0, 'Ansible automation tool', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_090', 'tools', '\\bchef\\b', 'regex', 0.8, 'Chef configuration management', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_091', 'tools', '\\bpuppet\\b', 'regex', 0.9, 'Puppet configuration management', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_092', 'tools', '\\bvagrant\\b', 'regex', 1.0, 'Vagrant development environment', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- TESTING TOOLS PATTERNS
-- =============================================================================

('cat_100', 'tools', '\\btesting\\b', 'regex', 0.8, 'Testing keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_101', 'tools', '\\bselenium\\b', 'regex', 1.0, 'Selenium testing framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_102', 'tools', '\\bjest\\b', 'regex', 1.0, 'Jest testing framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_103', 'tools', '\\bmocha\\b', 'regex', 1.0, 'Mocha testing framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_104', 'tools', '\\bcypress\\b', 'regex', 1.0, 'Cypress testing framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_105', 'tools', '\\bplaywright\\b', 'regex', 1.0, 'Playwright testing framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- MONITORING AND OBSERVABILITY PATTERNS
-- =============================================================================

('cat_110', 'tools', '\\bmonitoring\\b', 'regex', 0.8, 'Monitoring keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_111', 'tools', '\\bprometheus\\b', 'regex', 1.0, 'Prometheus monitoring', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_112', 'tools', '\\bgrafana\\b', 'regex', 1.0, 'Grafana visualization', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_113', 'tools', '\\belk\\b', 'regex', 0.9, 'ELK stack (Elasticsearch, Logstash, Kibana)', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_114', 'tools', '\\bkibana\\b', 'regex', 1.0, 'Kibana visualization', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_115', 'tools', '\\blogstash\\b', 'regex', 1.0, 'Logstash log processing', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),

-- =============================================================================
-- DATA SCIENCE AND ML PATTERNS
-- =============================================================================

('cat_120', 'tools', '\\bmachine learning\\b', 'regex', 1.0, 'Machine learning keyword', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_121', 'tools', '\\btensorflow\\b', 'regex', 1.0, 'TensorFlow ML framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_122', 'tools', '\\bpytorch\\b', 'regex', 1.0, 'PyTorch ML framework', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_123', 'tools', '\\bscikit-learn\\b', 'regex', 1.0, 'Scikit-learn ML library', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_124', 'tools', '\\bpandas\\b', 'regex', 1.0, 'Pandas data analysis library', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_125', 'tools', '\\bnumpy\\b', 'regex', 1.0, 'NumPy numerical computing library', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('cat_126', 'tools', '\\bjupyter\\b', 'regex', 1.0, 'Jupyter notebook environment', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Commit the transaction
COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these to verify the patterns were inserted correctly

-- Check total pattern count by category
SELECT
    SKILL_CATEGORY,
    COUNT(*) as PATTERN_COUNT,
    COUNT(CASE WHEN IS_ACTIVE = TRUE THEN 1 END) as ACTIVE_PATTERNS,
    COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as HIGH_CONFIDENCE_PATTERNS
FROM SKILL_CATEGORY_PATTERNS
GROUP BY SKILL_CATEGORY
ORDER BY PATTERN_COUNT DESC;

-- Check for duplicate patterns
SELECT
    PATTERN,
    COUNT(*) as DUPLICATE_COUNT
FROM SKILL_CATEGORY_PATTERNS
GROUP BY PATTERN
HAVING COUNT(*) > 1;

-- Sample patterns by category
SELECT
    SKILL_CATEGORY,
    PATTERN,
    DESCRIPTION,
    CONFIDENCE_SCORE,
    IS_ACTIVE
FROM SKILL_CATEGORY_PATTERNS
WHERE SKILL_CATEGORY IN ('languages', 'frameworks', 'tools', 'databases')
ORDER BY SKILL_CATEGORY, CONFIDENCE_SCORE DESC
LIMIT 25;