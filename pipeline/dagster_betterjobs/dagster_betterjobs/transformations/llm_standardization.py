"""
LLM Data Standardization Transformations

Core processing classes and functions for standardizing LLM-extracted VARIANT data
into normalized relational structures.

Contains:
- SkillStandardizer: Handle skill standardization and deduplication
- LocationStandardizer: Handle location standardization and geographic enrichment
- KeywordClassifier: Handle keyword classification and standardization
- Data quality validation functions
"""

import re
import json
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging

from dagster import get_dagster_logger


@dataclass
class LLMStandardizationConfig:
    """Configuration for LLM data standardization process"""

    # Confidence thresholds
    skill_confidence_threshold: float = 0.7
    location_confidence_threshold: float = 0.8
    keyword_confidence_threshold: float = 0.6

    # Processing parameters
    min_skill_frequency: int = 2  # Minimum appearances to include skill
    max_skill_variants: int = 10  # Maximum variants to track per skill

    # Quality thresholds
    min_coverage_percentage: float = 85.0  # Minimum coverage for quality validation
    max_low_confidence_percentage: float = 15.0  # Maximum low confidence items

    # Batch processing
    batch_size: int = 1000
    parallel_processing: bool = True


class SkillStandardizer:
    """Handle skill standardization and deduplication logic"""

    def __init__(self, snowflake_connection, database_name: str, stage_schema: str, confidence_threshold: float = 0.7):
        self.snowflake = snowflake_connection
        self.database_name = database_name
        self.stage_schema = stage_schema
        self.confidence_threshold = confidence_threshold
        self.logger = get_dagster_logger()

        # Load rules from database
        self.skill_rules = self._load_skill_rules_from_db()
        self.category_patterns = self._load_category_patterns_from_db()

    def _load_skill_rules_from_db(self) -> Dict[str, Dict[str, Any]]:
        """Load skill standardization rules from database"""
        with self.snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(f"""
                SELECT
                    PATTERN,
                    STANDARDIZED_NAME,
                    SKILL_CATEGORY,
                    SKILL_SUBCATEGORY,
                    CONFIDENCE_SCORE,
                    RULE_TYPE
                FROM {self.database_name}.{self.stage_schema}.SKILL_STANDARDIZATION_RULES
                WHERE RULE_TYPE = 'exact_match'
                ORDER BY CONFIDENCE_SCORE DESC
                """)

                results = cursor.fetchall()
                skill_rules = {}

                for row in results:
                    pattern, std_name, category, subcategory, confidence, rule_type = row

                    # Group by pattern (multiple patterns might map to same standardized name)
                    if pattern.lower() not in skill_rules:
                        skill_rules[pattern.lower()] = {
                            "canonical": std_name,
                            "category": category,
                            "subcategory": subcategory or "uncategorized",
                            "confidence": float(confidence),
                            "rule_type": rule_type
                        }

                self.logger.info(f"Loaded {len(skill_rules)} skill standardization rules from database")
                return skill_rules

            except Exception as e:
                self.logger.warning(f"Failed to load skill rules from database: {e}")
                return {}

    def _load_category_patterns_from_db(self) -> Dict[str, List[str]]:
        """Load skill category detection patterns from database"""
        with self.snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(f"""
                SELECT
                    SKILL_CATEGORY,
                    PATTERN,
                    CONFIDENCE_SCORE
                FROM {self.database_name}.{self.stage_schema}.SKILL_CATEGORY_PATTERNS
                WHERE IS_ACTIVE = TRUE
                ORDER BY SKILL_CATEGORY, CONFIDENCE_SCORE DESC
                """)

                results = cursor.fetchall()
                category_patterns = {}

                for row in results:
                    category, pattern, confidence = row

                    if category not in category_patterns:
                        category_patterns[category] = []

                    category_patterns[category].append(pattern)

                self.logger.info(f"Loaded category patterns for {len(category_patterns)} categories from database")
                return category_patterns

            except Exception as e:
                self.logger.warning(f"Failed to load category patterns from database: {e}")
                # Fallback to basic patterns if database query fails
                return {
                    "languages": [
                        r"\bpython\b", r"\bjavascript\b", r"\bjava\b", r"\bc\+\+\b", r"\bc#\b",
                        r"\bruntime\b", r"\bprogramming\b", r"\blanguage\b"
                    ],
                    "frameworks": [
                        r"\bframework\b", r"\breact\b", r"\bangular\b", r"\bvue\b", r"\bdjango\b",
                        r"\bspring\b", r"\bexpress\b", r"\.js$", r"\.ts$"
                    ],
                    "databases": [
                        r"\bdatabase\b", r"\bsql\b", r"\bmysql\b", r"\bpostgresql\b",
                        r"\bmongodb\b", r"\bredis\b", r"\boracle\b"
                    ],
                    "cloud": [
                        r"\baws\b", r"\bazure\b", r"\bgcp\b", r"\bcloud\b",
                        r"\bkubernetes\b", r"\bcontainer\b"
                    ],
                    "tools": [
                        r"\btool\b", r"\bgit\b", r"\bdocker\b", r"\bjenkins\b",
                        r"\bslack\b", r"\bjira\b"
                    ]
                }

    def standardize_skill(self, raw_skill: str, category: str = None) -> Dict[str, Any]:
        """
        Standardize a single skill with confidence scoring using database rules

        Args:
            raw_skill: Raw skill text from LLM
            category: Optional category hint from LLM

        Returns:
            Dict with standardized skill info and confidence
        """
        if not raw_skill or not raw_skill.strip():
            return None

        cleaned_skill = self._clean_skill_text(raw_skill)
        skill_key = cleaned_skill.lower()

        # Check for exact match in database rules
        if skill_key in self.skill_rules:
            rule = self.skill_rules[skill_key]
            return {
                "canonical_name": rule["canonical"],
                "category": rule["category"],
                "subcategory": rule["subcategory"],
                "confidence": rule["confidence"],
                "match_type": "database_rule",
                "original_text": raw_skill
            }

        # Auto-detect category if not provided
        detected_category = category or self.detect_skill_category(cleaned_skill)

        # Use original skill name with detected category
        return {
            "canonical_name": cleaned_skill,
            "category": detected_category,
            "subcategory": "uncategorized",
            "confidence": 0.6,  # Lower confidence for unmatched skills
            "match_type": "auto_categorized",
            "original_text": raw_skill
        }

    def _clean_skill_text(self, skill: str) -> str:
        """Clean and normalize skill text"""
        # Remove extra whitespace and normalize case
        cleaned = re.sub(r'\s+', ' ', skill.strip())

        # Remove common suffixes/prefixes
        cleaned = re.sub(r'\b(programming|language|framework|database|tool)\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # Title case for consistency
        return cleaned.title() if cleaned else skill.strip()

    def detect_skill_category(self, skill: str) -> str:
        """Auto-detect skill category using pattern matching"""
        skill_lower = skill.lower()

        for category, patterns in self.category_patterns.items():
            for pattern in patterns:
                if re.search(pattern, skill_lower):
                    return category

        return "uncategorized"

    def calculate_confidence(self, raw_skill: str, standardized_skill: str) -> float:
        """Calculate standardization confidence score"""
        skill_key = raw_skill.lower().strip()

        # High confidence for database matches
        if skill_key in self.skill_rules:
            return self.skill_rules[skill_key]["confidence"]

        # Medium confidence for similar text
        raw_lower = raw_skill.lower()
        std_lower = standardized_skill.lower()

        if raw_lower == std_lower:
            return 0.8
        elif raw_lower in std_lower or std_lower in raw_lower:
            return 0.7

        # Lower confidence for auto-categorized
        return 0.5

    def process_skills_batch(self, skills_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of skills data"""
        results = []

        for skill_record in skills_data:
            try:
                standardized = self.standardize_skill(
                    skill_record.get("skill_name_raw", ""),
                    skill_record.get("skill_category", "")
                )

                if standardized:
                    standardized.update({
                        "job_uid": skill_record.get("job_uid"),
                        "skill_source": skill_record.get("skill_source"),
                        "original_category": skill_record.get("skill_category")
                    })
                    results.append(standardized)

            except Exception as e:
                self.logger.warning(f"Failed to standardize skill: {skill_record}, error: {e}")
                continue

        return results


class LocationStandardizer:
    """Handle location standardization and geographic enrichment"""

    def __init__(self, snowflake_connection, database_name: str, stage_schema: str, confidence_threshold: float = 0.8):
        self.snowflake = snowflake_connection
        self.database_name = database_name
        self.stage_schema = stage_schema
        self.confidence_threshold = confidence_threshold
        self.logger = get_dagster_logger()

        # Load rules from database
        self.location_rules = self._load_location_rules_from_db()
        self.tech_hubs = self._load_tech_hubs_from_db()

    def _load_location_rules_from_db(self) -> Dict[str, Dict[str, Any]]:
        """Load location standardization rules from database"""
        with self.snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(f"""
                SELECT
                    PATTERN,
                    STANDARDIZED_NAME,
                    CITY,
                    STATE_PROVINCE,
                    COUNTRY,
                    LOCATION_TYPE,
                    CONFIDENCE_SCORE
                FROM {self.database_name}.{self.stage_schema}.LOCATION_STANDARDIZATION_RULES
                ORDER BY CONFIDENCE_SCORE DESC
                """)

                results = cursor.fetchall()
                location_rules = {}

                for row in results:
                    pattern, std_name, city, state, country, loc_type, confidence = row

                    location_rules[pattern.lower()] = {
                        "canonical": std_name,
                        "city": city,
                        "state": state,
                        "country": country,
                        "location_type": loc_type or "office",
                        "confidence": float(confidence)
                    }

                self.logger.info(f"Loaded {len(location_rules)} location standardization rules from database")
                return location_rules

            except Exception as e:
                self.logger.warning(f"Failed to load location rules from database: {e}")
                return {}

    def _load_tech_hubs_from_db(self) -> List[str]:
        """Load tech hub classifications from database"""
        with self.snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(f"""
                SELECT DISTINCT STANDARDIZED_NAME
                FROM {self.database_name}.{self.stage_schema}.LOCATION_STANDARDIZATION_RULES
                WHERE STANDARDIZED_NAME IN (
                    SELECT STANDARDIZED_NAME
                    FROM {self.database_name}.{self.stage_schema}.LOCATION_STANDARDIZATION_RULES
                    WHERE UPPER(STANDARDIZED_NAME) LIKE '%SAN FRANCISCO%'
                       OR UPPER(STANDARDIZED_NAME) LIKE '%NEW YORK%'
                       OR UPPER(STANDARDIZED_NAME) LIKE '%SEATTLE%'
                       OR UPPER(STANDARDIZED_NAME) LIKE '%AUSTIN%'
                       OR UPPER(STANDARDIZED_NAME) LIKE '%BOSTON%'
                )
                """)

                results = cursor.fetchall()
                tech_hubs = [row[0] for row in results] if results else []

                self.logger.info(f"Loaded {len(tech_hubs)} tech hub locations from database")
                return tech_hubs

            except Exception as e:
                self.logger.warning(f"Failed to load tech hubs from database: {e}")
                return []

    def standardize_location(self, raw_location: str) -> Dict[str, Any]:
        """Standardize location with geographic hierarchy using database rules"""
        if not raw_location or not raw_location.strip():
            return None

        cleaned_location = raw_location.strip().lower()

        # Check for exact match in database rules
        if cleaned_location in self.location_rules:
            rule = self.location_rules[cleaned_location]
            return {
                "canonical_name": rule["canonical"],
                "city": rule["city"],
                "state": rule["state"],
                "country": rule["country"],
                "is_remote": rule["canonical"].lower() == "remote",
                "is_tech_hub": rule["canonical"] in self.tech_hubs,
                "confidence": rule["confidence"],
                "match_type": "database_rule",
                "original_text": raw_location
            }

        # Fallback: use original with basic parsing
        return self._parse_location_fallback(raw_location)

    def _parse_location_fallback(self, location: str) -> Dict[str, Any]:
        """Parse location with basic heuristics"""
        # Basic city, state parsing
        parts = [p.strip() for p in location.split(',')]

        city = parts[0] if parts else location
        state = parts[1] if len(parts) > 1 else None
        country = "United States" if state else "Unknown"

        canonical = f"{city}, {state}" if state else city

        return {
            "canonical_name": canonical,
            "city": city,
            "state": state,
            "country": country,
            "is_remote": "remote" in location.lower(),
            "is_tech_hub": False,
            "confidence": 0.6,
            "match_type": "parsed_fallback",
            "original_text": location
        }


class KeywordClassifier:
    """Handle keyword classification and standardization"""

    def __init__(self, snowflake_connection, database_name: str, stage_schema: str, confidence_threshold: float = 0.6):
        self.snowflake = snowflake_connection
        self.database_name = database_name
        self.stage_schema = stage_schema
        self.confidence_threshold = confidence_threshold
        self.logger = get_dagster_logger()

        # Load patterns from database
        self.keyword_patterns = self._load_keyword_patterns_from_db()

    def _load_keyword_patterns_from_db(self) -> Dict[str, List[str]]:
        """Load keyword classification patterns from database"""
        with self.snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                # For now, we'll create a simple table structure for keyword patterns
                # This could be expanded to a proper KEYWORD_CLASSIFICATION_PATTERNS table
                cursor.execute(f"""
                SELECT
                    'industry' as KEYWORD_TYPE,
                    ARRAY_CONSTRUCT(
                        'fintech', 'saas', 'b2b', 'e-commerce', 'healthcare',
                        'finance', 'technology', 'startup', 'enterprise'
                    ) as PATTERNS

                UNION ALL

                SELECT
                    'role_type' as KEYWORD_TYPE,
                    ARRAY_CONSTRUCT(
                        'senior', 'junior', 'lead', 'principal', 'staff',
                        'individual contributor', 'manager', 'director'
                    ) as PATTERNS

                UNION ALL

                SELECT
                    'technology' as KEYWORD_TYPE,
                    ARRAY_CONSTRUCT(
                        'microservices', 'api', 'cloud native', 'devops',
                        'machine learning', 'data science', 'full stack'
                    ) as PATTERNS

                UNION ALL

                SELECT
                    'company_stage' as KEYWORD_TYPE,
                    ARRAY_CONSTRUCT(
                        'series a', 'series b', 'ipo', 'public company',
                        'pre-ipo', 'growth stage', 'early stage'
                    ) as PATTERNS
                """)

                results = cursor.fetchall()
                keyword_patterns = {}

                for row in results:
                    keyword_type, patterns_array = row
                    # Convert Snowflake array to Python list
                    patterns = json.loads(patterns_array) if isinstance(patterns_array, str) else patterns_array
                    keyword_patterns[keyword_type] = [f"\\b{pattern}\\b" for pattern in patterns]

                self.logger.info(f"Loaded keyword patterns for {len(keyword_patterns)} categories from database")
                return keyword_patterns

            except Exception as e:
                self.logger.warning(f"Failed to load keyword patterns from database: {e}")
                # Fallback to basic patterns
                return {
                    "industry": [r"\bfintech\b", r"\bsaas\b", r"\bb2b\b"],
                    "role_type": [r"\bsenior\b", r"\bjunior\b", r"\blead\b"],
                    "technology": [r"\bmicroservices\b", r"\bapi\b", r"\bcloud native\b"],
                    "company_stage": [r"\bseries a\b", r"\bipo\b"]
                }

    def classify_keyword_type(self, keyword: str) -> str:
        """Classify keyword into type (industry, role, technology, etc.)"""
        keyword_lower = keyword.lower()

        for keyword_type, patterns in self.keyword_patterns.items():
            for pattern in patterns:
                if re.search(pattern, keyword_lower):
                    return keyword_type

        return "primary"  # Default type

    def standardize_keyword(self, raw_keyword: str) -> Dict[str, Any]:
        """Standardize keyword with confidence scoring"""
        if not raw_keyword or not raw_keyword.strip():
            return None

        cleaned_keyword = raw_keyword.strip()
        keyword_type = self.classify_keyword_type(cleaned_keyword)

        return {
            "canonical_text": cleaned_keyword,
            "keyword_type": keyword_type,
            "confidence": 0.8,  # Default confidence for keywords
            "original_text": raw_keyword
        }


# Data quality and validation functions
def validate_normalization_completeness(cursor, database_name: str, stage_schema: str) -> Dict[str, Any]:
    """Validate that normalization process completed successfully"""

    # Check skills coverage
    cursor.execute(f"""
    SELECT
        COUNT(DISTINCT ju.JOB_UID) as total_jobs,
        COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
        ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as coverage_pct
    FROM {database_name}.{stage_schema}.JOBS_UNIFIED ju
    LEFT JOIN {database_name}.{stage_schema}.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
    WHERE ju.IS_ENGLISH = TRUE
    """)

    result = cursor.fetchone()
    total_jobs, jobs_with_skills, coverage_pct = result if result else (0, 0, 0.0)

    return {
        "total_jobs": total_jobs,
        "jobs_with_skills": jobs_with_skills,
        "coverage_percentage": coverage_pct,
        "meets_threshold": coverage_pct >= 85.0,
        "validation_timestamp": datetime.now().isoformat()
    }


def calculate_coverage_metrics(cursor, database_name: str, stage_schema: str) -> Dict[str, Any]:
    """Calculate coverage metrics for normalized data"""

    metrics = {}

    # Skills coverage by category
    cursor.execute(f"""
    SELECT
        sn.SKILL_CATEGORY,
        COUNT(DISTINCT sn.SKILL_ID) as unique_skills,
        COUNT(DISTINCT jsb.JOB_UID) as jobs_using_category,
        AVG(sn.CONFIDENCE_SCORE) as avg_confidence
    FROM {database_name}.{stage_schema}.SKILLS_NORMALIZED sn
    LEFT JOIN {database_name}.{stage_schema}.JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
    GROUP BY sn.SKILL_CATEGORY
    ORDER BY jobs_using_category DESC
    """)

    category_results = cursor.fetchall()
    if category_results:
        columns = [desc[0] for desc in cursor.description]
        metrics["skills_by_category"] = [dict(zip(columns, row)) for row in category_results]

    return metrics


def detect_data_quality_issues(cursor, database_name: str, stage_schema: str) -> List[Dict[str, Any]]:
    """Detect and report data quality issues"""

    issues = []

    # Check for orphaned skills
    cursor.execute(f"""
    SELECT COUNT(*) as orphaned_count
    FROM {database_name}.{stage_schema}.SKILLS_NORMALIZED sn
    LEFT JOIN {database_name}.{stage_schema}.JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
    WHERE jsb.SKILL_ID IS NULL
    """)

    result = cursor.fetchone()
    if result and result[0] > 0:
        issues.append({
            "issue_type": "orphaned_skills",
            "count": result[0],
            "severity": "medium",
            "description": f"Found {result[0]} skills with no job associations"
        })

    # Check for low confidence skills
    cursor.execute(f"""
    SELECT COUNT(*) as low_confidence_count
    FROM {database_name}.{stage_schema}.SKILLS_NORMALIZED
    WHERE CONFIDENCE_SCORE < 0.5
    """)

    result = cursor.fetchone()
    if result and result[0] > 0:
        issues.append({
            "issue_type": "low_confidence_skills",
            "count": result[0],
            "severity": "low",
            "description": f"Found {result[0]} skills with low confidence scores"
        })

    return issues