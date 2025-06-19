-- Experience Standardization Rules
-- Defines standardized experience levels, seniority mappings, and year ranges
-- Used by stage_experience_normalized asset for consistent experience categorization

-- Clear existing data
DELETE FROM BETTERJOBS_DB.STAGE.EXPERIENCE_STANDARDIZATION_RULES;

-- Insert General Experience Levels (years-based)
INSERT INTO BETTERJOBS_DB.STAGE.EXPERIENCE_STANDARDIZATION_RULES
(RULE_ID, EXPERIENCE_CATEGORY, EXPERIENCE_NAME, MIN_YEARS_REQUIRED, MAX_YEARS_REQUIRED, SENIORITY_ORDER, EXPERIENCE_DESCRIPTION, SOURCE_PATTERNS, MAPPING_TYPE, IS_ACTIVE)

-- General Years-Based Experience Levels
SELECT 'exp_entry_level', 'general', 'Entry Level', 0, 2, 1, 'Entry-level positions requiring 0-2 years of experience', PARSE_JSON('["entry", "junior", "graduate", "new grad", "0-2 years", "fresher"]'), 'years_range', TRUE
UNION ALL SELECT 'exp_junior_level', 'general', 'Junior Level', 1, 3, 2, 'Junior positions requiring 1-3 years of experience', PARSE_JSON('["junior", "1-3 years", "associate"]'), 'years_range', TRUE
UNION ALL SELECT 'exp_mid_level', 'general', 'Mid Level', 3, 5, 3, 'Mid-level positions requiring 3-5 years of experience', PARSE_JSON('["mid-level", "intermediate", "3-5 years", "experienced"]'), 'years_range', TRUE
UNION ALL SELECT 'exp_senior_level', 'general', 'Senior Level', 5, 8, 4, 'Senior positions requiring 5-8 years of experience', PARSE_JSON('["senior", "5+ years", "5-8 years", "lead"]'), 'years_range', TRUE
UNION ALL SELECT 'exp_lead_level', 'general', 'Lead Level', 7, 12, 5, 'Lead positions requiring 7-12 years of experience', PARSE_JSON('["lead", "team lead", "7+ years", "technical lead"]'), 'years_range', TRUE
UNION ALL SELECT 'exp_expert_level', 'general', 'Expert Level', 10, 15, 6, 'Expert positions requiring 10-15 years of experience', PARSE_JSON('["expert", "specialist", "10+ years", "subject matter expert"]'), 'years_range', TRUE

-- Role Seniority Levels (career progression)
UNION ALL SELECT 'exp_staff_level', 'role_seniority', 'Staff Level', 8, 15, 7, 'Staff-level individual contributor roles', PARSE_JSON('["staff", "staff engineer", "staff developer", "ic4", "ic5"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_principal_level', 'role_seniority', 'Principal Level', 10, 20, 8, 'Principal-level individual contributor roles', PARSE_JSON('["principal", "principal engineer", "principal architect", "ic6", "ic7"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_director_level', 'role_seniority', 'Director Level', 12, 25, 9, 'Director-level management positions', PARSE_JSON('["director", "engineering director", "director of", "head of"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_vp_level', 'role_seniority', 'VP Level', 15, 30, 10, 'Vice President level executive positions', PARSE_JSON('["vp", "vice president", "svp", "senior vice president"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_executive_level', 'role_seniority', 'Executive Level', 20, 40, 11, 'Executive leadership positions', PARSE_JSON('["cto", "ceo", "cfo", "chief", "president", "executive"]'), 'seniority_level', TRUE

-- Experience Level Mappings (from LLM extraction)
UNION ALL SELECT 'exp_map_entry', 'experience_level_mapping', 'Entry Level', 0, 2, 1, 'Maps entry-level classifications from LLM', PARSE_JSON('["entry", "entry level", "entry-level", "junior", "associate"]'), 'experience_level', TRUE
UNION ALL SELECT 'exp_map_mid', 'experience_level_mapping', 'Mid Level', 3, 5, 3, 'Maps mid-level classifications from LLM', PARSE_JSON('["mid", "mid level", "mid-level", "intermediate", "experienced"]'), 'experience_level', TRUE
UNION ALL SELECT 'exp_map_senior', 'experience_level_mapping', 'Senior Level', 5, 8, 4, 'Maps senior-level classifications from LLM', PARSE_JSON('["senior", "senior level", "senior-level", "lead"]'), 'experience_level', TRUE
UNION ALL SELECT 'exp_map_executive', 'experience_level_mapping', 'Executive Level', 15, 40, 11, 'Maps executive-level classifications from LLM', PARSE_JSON('["executive", "executive level", "c-level", "leadership"]'), 'experience_level', TRUE

-- Seniority Level Mappings (from LLM extraction)
UNION ALL SELECT 'exp_map_staff', 'seniority_level_mapping', 'Staff Level', 8, 15, 7, 'Maps staff seniority from LLM', PARSE_JSON('["staff", "staff engineer", "staff developer", "staff analyst"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_map_principal', 'seniority_level_mapping', 'Principal Level', 10, 20, 8, 'Maps principal seniority from LLM', PARSE_JSON('["principal", "principal engineer", "principal architect", "distinguished"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_map_director', 'seniority_level_mapping', 'Director Level', 12, 25, 9, 'Maps director seniority from LLM', PARSE_JSON('["director", "engineering director", "technical director"]'), 'seniority_level', TRUE
UNION ALL SELECT 'exp_map_vp', 'seniority_level_mapping', 'VP Level', 15, 30, 10, 'Maps VP seniority from LLM', PARSE_JSON('["vp", "vice president", "svp", "senior vp"]'), 'seniority_level', TRUE

-- Technology-Specific Experience Patterns
UNION ALL SELECT 'exp_tech_beginner', 'technology_specific', 'Technology Beginner', 0, 1, 1, 'Beginner level for specific technologies', PARSE_JSON('["beginner", "learning", "basic"]'), 'technology_years', TRUE
UNION ALL SELECT 'exp_tech_competent', 'technology_specific', 'Technology Competent', 1, 3, 2, 'Competent level for specific technologies', PARSE_JSON('["competent", "working knowledge", "familiar"]'), 'technology_years', TRUE
UNION ALL SELECT 'exp_tech_proficient', 'technology_specific', 'Technology Proficient', 3, 5, 3, 'Proficient level for specific technologies', PARSE_JSON('["proficient", "experienced", "solid"]'), 'technology_years', TRUE
UNION ALL SELECT 'exp_tech_expert', 'technology_specific', 'Technology Expert', 5, 10, 4, 'Expert level for specific technologies', PARSE_JSON('["expert", "advanced", "deep"]'), 'technology_years', TRUE
UNION ALL SELECT 'exp_tech_master', 'technology_specific', 'Technology Master', 8, 20, 5, 'Master level for specific technologies', PARSE_JSON('["master", "guru", "architect"]'), 'technology_years', TRUE

-- Special Categories
UNION ALL SELECT 'exp_internship', 'special', 'Internship Level', 0, 0, 0, 'Internship and training positions', PARSE_JSON('["intern", "internship", "trainee", "co-op", "student"]'), 'special', TRUE
UNION ALL SELECT 'exp_contract', 'special', 'Contract Level', NULL, NULL, 50, 'Contract and freelance positions (experience varies)', PARSE_JSON('["contract", "contractor", "freelance", "consultant"]'), 'special', TRUE
UNION ALL SELECT 'exp_any_level', 'special', 'Any Experience Level', 0, 40, 99, 'Positions open to any experience level', PARSE_JSON('["any", "all levels", "open", "flexible"]'), 'special', TRUE;