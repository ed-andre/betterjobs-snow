"""
Language Detection Module for Job Postings

This module provides functionality to detect the language of job postings and filter for English-only jobs.
It uses Python-based detection with langdetect library as the primary method and SQL-based pattern
detection as a fallback.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
from langdetect import detect, detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

# Set seed for consistent results
DetectorFactory.seed = 0

logger = logging.getLogger(__name__)


class LanguageDetector:
    """
    A comprehensive language detection system for job postings.
    """

    def __init__(self, confidence_threshold: float = 0.7):
        """
        Initialize the language detector.

        Args:
            confidence_threshold: Minimum confidence score to consider detection reliable
        """
        self.confidence_threshold = confidence_threshold
        self.english_patterns = self._compile_english_patterns()
        self.non_english_patterns = self._compile_non_english_patterns()

    def _compile_english_patterns(self) -> List[re.Pattern]:
        """Compile regex patterns that strongly indicate English text."""
        patterns = [
            # Common English job posting phrases
            r'\b(?:job|position|role|career|opportunity|hiring|employment)\b',
            r'\b(?:responsibilities|requirements|qualifications|experience)\b',
            r'\b(?:skills|education|benefits|salary|location|remote)\b',
            r'\b(?:full-time|part-time|contract|permanent|temporary)\b',
            r'\b(?:company|team|department|manager|employee|candidate)\b',
            r'\b(?:apply|submit|send|email|contact|interview)\b',
            # English articles and prepositions
            r'\b(?:the|and|or|but|in|on|at|to|for|with|by|from|of|as)\b',
            # English auxiliary verbs
            r'\b(?:is|are|was|were|will|would|should|could|must|may|can)\b',
        ]
        return [re.compile(pattern, re.IGNORECASE) for pattern in patterns]

    def _compile_non_english_patterns(self) -> Dict[str, List[re.Pattern]]:
        """Compile regex patterns that indicate non-English languages."""
        patterns = {
            'spanish': [
                r'\b(?:trabajo|empleo|puesto|oportunidad|empresa|requisitos)\b',
                r'\b(?:experiencia|habilidades|educación|salario|ubicación)\b',
                r'\b(?:tiempo completo|medio tiempo|contrato|permanente)\b',
            ],
            'french': [
                r'\b(?:emploi|poste|opportunité|entreprise|exigences|compétences)\b',
                r'\b(?:expérience|éducation|salaire|lieu|temps plein|temps partiel)\b',
            ],
            'german': [
                r'\b(?:arbeit|stelle|position|unternehmen|anforderungen|fähigkeiten)\b',
                r'\b(?:erfahrung|bildung|gehalt|standort|vollzeit|teilzeit)\b',
            ],
            'portuguese': [
                r'\b(?:trabalho|emprego|posição|oportunidade|empresa|requisitos)\b',
                r'\b(?:experiência|habilidades|educação|salário|localização)\b',
            ],
            'italian': [
                r'\b(?:lavoro|impiego|posizione|opportunità|azienda|requisiti)\b',
                r'\b(?:esperienza|competenze|educazione|stipendio|posizione)\b',
            ],
            # Companies like Coupang have job postings in Korean and Chinese
            'korean': [
                r'(?:채용|구인|모집|입사|취업|일자리|직장|직무|업무)',
                r'(?:경력|경험|학력|기술|능력|자격|요건|조건)',
                r'(?:회사|기업|팀|부서|직원|인재|지원자)',
                r'(?:급여|연봉|임금|복리후생|근무|정규직|계약직|파트타임)',
                r'(?:서류전형|면접|지원|접수|마감|문의)',
                # Additional patterns for job titles and company names
                r'(?:쿠팡|개발자|컨설턴트|매니저|엔지니어|디자이너)',
                r'(?:광고|마케팅|영업|기획|운영|분석)',
                r'(?:백엔드|프론트엔드|풀스택|데이터|시스템)',
                r'(?:시니어|주니어|신입|경력|전환|가능)',
                r'(?:년|이상|미만|우대|필수|우선)',
                # Enhanced patterns for common Korean job/employment terms
                r'(?:관리|담당|책임|업무|역할|포지션)',
                r'(?:팀장|부장|과장|대리|주임|사원|실장)',
                r'(?:전문가|스페셜리스트|어드바이저|코디네이터)',
                r'(?:풀타임|정규직|비정규직|계약직|인턴|신입)',
                r'(?:근무지|위치|지역|본사|지점|사무실)',
            ],
            'chinese': [
                r'(?:招聘|求职|工作|职位|岗位|就业|入职)',
                r'(?:经验|学历|技能|能力|资格|要求|条件)',
                r'(?:公司|企业|团队|部门|员工|人才|候选人)',
                r'(?:薪资|工资|待遇|福利|工作|全职|兼职|合同)',
                r'(?:简历|面试|申请|投递|截止|咨询)',
            ],
            'japanese': [
                r'(?:採用|求人|募集|入社|就職|仕事|職場|職種|業務)',
                r'(?:経験|学歴|技術|能力|資格|要件|条件)',
                r'(?:会社|企業|チーム|部署|社員|人材|応募者)',
                r'(?:給与|年収|賃金|福利厚生|勤務|正社員|契約社員|パートタイム)',
                r'(?:書類選考|面接|応募|受付|締切|問合せ)',
            ],
        }

        compiled_patterns = {}
        for lang, pattern_list in patterns.items():
            compiled_patterns[lang] = [re.compile(pattern, re.IGNORECASE) for pattern in pattern_list]

        return compiled_patterns

    def _calculate_english_score(self, text: str) -> float:
        """
        Calculate an English likelihood score based on pattern matching.

        Args:
            text: Input text to analyze

        Returns:
            Score between 0 and 1 indicating likelihood of English
        """
        if not text or len(text.strip()) < 10:
            return 0.0

        # Count English pattern matches
        english_matches = 0
        for pattern in self.english_patterns:
            pattern_count = len(pattern.findall(text))
            english_matches += pattern_count

        # Count non-English pattern matches
        non_english_matches = 0
        for lang, lang_patterns in self.non_english_patterns.items():
            for pattern in lang_patterns:
                pattern_count = len(pattern.findall(text))
                non_english_matches += pattern_count

        # Calculate ratio
        total_matches = english_matches + non_english_matches
        if total_matches == 0:
            return 0.5  # Neutral if no patterns match

        score = english_matches / total_matches
        return score

    def detect_language_python(self, text: str) -> Tuple[str, float]:
        """
        Detect language using langdetect library.

        Args:
            text: Input text to analyze

        Returns:
            Tuple of (detected_language, confidence_score)
        """
        if not text or len(text.strip()) < 10:
            return 'unknown', 0.0

        try:
            # Get language probabilities
            lang_probs = detect_langs(text)

            if lang_probs:
                top_lang = lang_probs[0]
                return top_lang.lang, top_lang.prob
            else:
                return 'unknown', 0.0

        except LangDetectException as e:
            logger.debug(f"Language detection failed: {e}")
            return 'unknown', 0.0

    def detect_language_sql_fallback(self, text: str, debug_info: str = None) -> Tuple[str, float]:
        """
        Fallback language detection using pattern matching.

        Args:
            text: Input text to analyze
            debug_info: Optional debug context information

        Returns:
            Tuple of (detected_language, confidence_score)
        """
        if not text or len(text.strip()) < 10:
            return 'unknown', 0.0

        # Language code mapping for consistency
        lang_codes = {
            'korean': 'ko',
            'chinese': 'zh',
            'japanese': 'ja',
            'spanish': 'es',
            'french': 'fr',
            'german': 'de',
            'portuguese': 'pt',
            'italian': 'it'
        }

        # Check for specific language patterns first (more reliable for CJK languages)
        best_lang = None
        best_score = 0

        for lang, patterns in self.non_english_patterns.items():
            matches = 0
            for pattern in patterns:
                pattern_matches = len(pattern.findall(text))
                matches += pattern_matches

            # For CJK languages, even 1-2 matches can be very confident
            if lang in ['korean', 'chinese', 'japanese']:
                if matches >= 1:
                    confidence = min(0.95, 0.8 + (matches * 0.05))  # High confidence for CJK
                    return lang_codes[lang], confidence
            else:
                # For Latin script languages, need more matches
                if matches >= 2:
                    if matches > best_score:
                        best_score = matches
                        best_lang = lang

        # If we found a Latin script language with good matches
        if best_lang and best_score >= 2:
            confidence = min(0.9, 0.6 + (best_score * 0.1))
            return lang_codes[best_lang], confidence

        # Fallback to English scoring
        english_score = self._calculate_english_score(text)

        if english_score >= 0.7:
            return 'en', english_score
        elif english_score <= 0.3:
            return 'unknown', 0.3
        else:
            return 'en', english_score  # Default to English for mixed content

    def detect_language_comprehensive(self, text: str) -> Dict[str, Union[str, float, bool]]:
        """
        Comprehensive language detection combining multiple methods.

        Args:
            text: Input text to analyze

        Returns:
            Dictionary with detected_language, language_confidence, is_english, and language_detection_method fields
        """
        if not text or len(text.strip()) < 10:
            return {
                'detected_language': 'unknown',
                'language_confidence': 0.0,
                'is_english': False,
                'language_detection_method': 'insufficient_text'
            }

        # Primary detection using langdetect
        primary_lang, primary_confidence = self.detect_language_python(text)

        # Normalize language codes from langdetect
        primary_lang = self._normalize_language_code(primary_lang)

        # Fallback detection using patterns
        fallback_lang, fallback_confidence = self.detect_language_sql_fallback(text)

        # For CJK languages, prioritize pattern detection over langdetect
        # as langdetect often misidentifies these languages
        if fallback_lang in ['ko', 'zh', 'ja'] and fallback_confidence >= 0.8:
            # High confidence pattern detection for CJK languages takes priority
            detected_language = fallback_lang
            confidence = fallback_confidence
            detection_method = 'sql_pattern_fallback'
        elif primary_confidence >= self.confidence_threshold:
            detected_language = primary_lang
            confidence = primary_confidence
            detection_method = 'python_langdetect'
        elif fallback_confidence >= self.confidence_threshold:
            detected_language = fallback_lang
            confidence = fallback_confidence
            detection_method = 'sql_pattern_fallback'
        else:
            # Use the method with higher confidence
            if primary_confidence >= fallback_confidence:
                detected_language = primary_lang
                confidence = primary_confidence
                detection_method = 'python_langdetect_low_confidence'
            else:
                detected_language = fallback_lang
                confidence = fallback_confidence
                detection_method = 'sql_pattern_low_confidence'

        # Determine if English
        is_english = detected_language == 'en' and confidence >= self.confidence_threshold

        return {
            'detected_language': detected_language,
            'language_confidence': round(confidence, 3),
            'is_english': is_english,
            'language_detection_method': detection_method
        }

    def _normalize_language_code(self, lang_code: str) -> str:
        """
        Normalize language codes to consistent 2-letter format.

        Args:
            lang_code: Language code from detection library

        Returns:
            Normalized 2-letter language code
        """
        if not lang_code:
            return 'unknown'

        # Handle common langdetect variations
        lang_map = {
            'zh-cn': 'zh',
            'zh-tw': 'zh',
            'zh-Hans': 'zh',
            'zh-Hant': 'zh',
            'en-us': 'en',
            'en-gb': 'en',
            'ja-jp': 'ja',
            'ko-kr': 'ko'
        }

        # Convert to lowercase and check mappings
        normalized = lang_code.lower()
        if normalized in lang_map:
            return lang_map[normalized]

        # Return first 2 characters for other codes
        return normalized[:2] if len(normalized) >= 2 else normalized

    def detect_language_multi_field(self, job_title: str, job_description: str, job_id: str = None, company_id: str = None, platform: str = None) -> Dict[str, Union[str, float, bool]]:
        """
        Enhanced language detection using multiple text fields with smart fallback logic.

        Detection Priority:
        1. Primary: Analyze job_description (existing logic)
        2. Fallback: Analyze job_title if description detection fails
        3. Combined: Use both fields for confidence boosting

        Args:
            job_title: Job title text
            job_description: Job description text
            job_id: Optional job ID for debugging
            company_id: Optional company ID for debugging
            platform: Optional platform for debugging

        Returns:
            Dictionary with detected_language, language_confidence, is_english,
            language_detection_method, and detection_source fields
        """
        # Handle empty inputs
        job_title = job_title or ""
        job_description = job_description or ""

        # Phase 1: Standard job description detection
        desc_result = self.detect_language_comprehensive(job_description)

        # Phase 2: Job title detection (especially for corrupted descriptions)
        title_result = self.detect_language_comprehensive(job_title)

        # Phase 3: Smart decision logic with enhanced prioritization
        desc_high_conf = desc_result['language_confidence'] >= self.confidence_threshold
        title_high_conf = title_result['language_confidence'] >= self.confidence_threshold

        if desc_high_conf and title_high_conf:
            # Both have high confidence - choose based on language type and reliability
            desc_lang = desc_result['detected_language']
            title_lang = title_result['detected_language']

            # If they agree, use description (more text usually means better accuracy)
            if desc_lang == title_lang:
                final_result = desc_result.copy()
                final_result['detection_source'] = 'job_description'
            # If they disagree, prioritize CJK languages from title (more reliable for short CJK text)
            elif title_lang in ['ko', 'zh', 'ja']:
                final_result = title_result.copy()
                final_result['detection_source'] = 'job_title'
            # Otherwise, use description
            else:
                final_result = desc_result.copy()
                final_result['detection_source'] = 'job_description'
        elif desc_high_conf:
            # Only description has high confidence
            final_result = desc_result.copy()
            final_result['detection_source'] = 'job_description'
        elif title_high_conf:
            # Only title has high confidence
            final_result = title_result.copy()
            final_result['detection_source'] = 'job_title'
        else:
            # Both low confidence - use combined analysis or fallback logic
            combined_text = f"{job_title} {job_description}".strip()
            if combined_text:
                combined_result = self.detect_language_comprehensive(combined_text)

                if combined_result['language_confidence'] >= self.confidence_threshold:
                    final_result = combined_result.copy()
                    final_result['detection_source'] = 'combined'
                else:
                    # All methods failed - choose best available
                    if title_result['language_confidence'] > desc_result['language_confidence']:
                        final_result = title_result.copy()
                        final_result['detection_source'] = 'job_title_fallback'
                    else:
                        final_result = desc_result.copy()
                        final_result['detection_source'] = 'job_description_fallback'
            else:
                # No text available
                final_result = {
                    'detected_language': 'unknown',
                    'language_confidence': 0.0,
                    'is_english': False,
                    'language_detection_method': 'insufficient_text',
                    'detection_source': 'no_text'
                }

        return final_result

    def process_dataframe(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """
        Process a DataFrame to add language detection columns.

        Args:
            df: Input DataFrame
            text_column: Name of the column containing text to analyze

        Returns:
            DataFrame with added language detection columns
        """
        if text_column not in df.columns:
            raise ValueError(f"Column '{text_column}' not found in DataFrame")

        # Apply language detection to each row
        results = df[text_column].apply(self.detect_language_comprehensive)

        # Extract results into separate columns
        df['detected_language'] = results.apply(lambda x: x['detected_language'])
        df['language_confidence'] = results.apply(lambda x: x['language_confidence'])
        df['is_english'] = results.apply(lambda x: x['is_english'])
        df['language_detection_method'] = results.apply(lambda x: x['language_detection_method'])

        return df

    def process_dataframe_multi_field(self, df: pd.DataFrame,
                                     title_column: str = 'job_title',
                                     description_column: str = 'job_description') -> pd.DataFrame:
        """
        Process DataFrame using multi-field language detection.

        Args:
            df: Input DataFrame
            title_column: Name of column containing job title
            description_column: Name of column containing job description

        Returns:
            DataFrame with added language detection columns including detection_source
        """
        # Validate columns exist
        missing_cols = []
        if title_column not in df.columns:
            missing_cols.append(title_column)
        if description_column not in df.columns:
            missing_cols.append(description_column)

        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

        def detect_language_row(row):
            return self.detect_language_multi_field(
                job_title=row[title_column] if pd.notna(row[title_column]) else "",
                job_description=row[description_column] if pd.notna(row[description_column]) else ""
            )

        # Apply multi-field detection
        language_data = df.apply(detect_language_row, axis=1)

        # Extract results into separate columns
        df['detected_language'] = [result['detected_language'] for result in language_data]
        df['language_confidence'] = [result['language_confidence'] for result in language_data]
        df['is_english'] = [result['is_english'] for result in language_data]
        df['language_detection_method'] = [result['language_detection_method'] for result in language_data]
        df['detection_source'] = [result['detection_source'] for result in language_data]

        return df


def get_sql_language_detection_query(debug_job_id: str = None) -> str:
    """
    Generate SQL query for language detection as a fallback method.
    This can be used directly in SQL transformations.

    Args:
        debug_job_id: Optional job ID to add debug logging for
    """
    debug_clause = ""
    if debug_job_id:
        debug_clause = f"""
        -- Debug logging for target job {debug_job_id}
        , CASE WHEN job_id = '{debug_job_id}' THEN
            'DEBUG: Processing job_id {debug_job_id} with title: ' || COALESCE(job_title, 'NULL') ||
            ' and description length: ' || LENGTH(COALESCE(job_description, ''))
          ELSE NULL END AS debug_info
        """

    return f"""
    SELECT *,
        CASE
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(job|position|role|career|opportunity|hiring|employment|responsibilities|requirements|qualifications|experience|skills|education|benefits|salary|location|remote|full-time|part-time|contract|permanent|temporary|company|team|department|manager|employee|candidate|apply|submit|send|email|contact|interview|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 5
                AND REGEXP_COUNT(LOWER(job_description),
                    '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                AND REGEXP_COUNT(job_description,
                    '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就业|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
            THEN 'en'
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(trabajo|empleo|puesto|oportunidad|empresa|requisitos)\\b') > 2
            THEN 'es'
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(emploi|poste|opportunité|entreprise|exigences|compétences)\\b') > 2
            THEN 'fr'
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(arbeit|stelle|position|unternehmen|anforderungen|fähigkeiten)\\b') > 2
            THEN 'de'
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(trabalho|emprego|posição|oportunidade|empresa|requisitos)\\b') > 2
            THEN 'pt'
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(lavoro|impiego|posizione|opportunità|azienda|requisiti)\\b') > 2
            THEN 'it'
            WHEN REGEXP_COUNT(job_description,
                '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|경력|경험|학력|기술|능력|자격|요건|조건|회사|기업|팀|부서|직원|인재|지원자)') > 2
            THEN 'ko'
            WHEN REGEXP_COUNT(job_description,
                '(招聘|求职|工作|职位|岗位|就业|入职|经验|学历|技能|能力|资格|要求|条件|公司|企业|团队|部门|员工|人才|候选人)') > 2
            THEN 'zh'
            WHEN REGEXP_COUNT(job_description,
                '(採用|求人|募集|入社|就職|仕事|職場|職種|業務|経験|学歴|技術|能力|資格|要件|条件|会社|企業|チーム|部署|社員|人材|応募者)') > 2
            THEN 'ja'
            ELSE 'unknown'
        END AS detected_language,
        CASE
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(job|position|role|career|opportunity|hiring|employment|responsibilities|requirements|qualifications|experience|skills|education|benefits|salary|location|remote|full-time|part-time|contract|permanent|temporary|company|team|department|manager|employee|candidate|apply|submit|send|email|contact|interview|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 5
                AND REGEXP_COUNT(LOWER(job_description),
                    '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                AND REGEXP_COUNT(job_description,
                    '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就业|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
            THEN 0.8
            WHEN REGEXP_COUNT(job_description,
                '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|경력|경험|학력|기술|능력|자격|요건|조건|회사|기업|팀|부서|직원|인재|지원자|招聘|求职|工作|职位|岗位|就업|入职|经验|学历|기능|능력|资格|要求|条件|公司|企业|团队|部门|员工|인재|候选人|採用|求人|募集|입사|就직|仕事|직장|職種|업무|경험|학歴|기술|능력|資格|요件|조건|회사|기업|팀|부서|사원|인재|응모자)') > 2
            THEN 0.9
            ELSE 0.3
        END AS language_confidence,
        CASE
            WHEN REGEXP_COUNT(LOWER(job_description),
                '\\b(job|position|role|career|opportunity|hiring|employment|responsibilities|requirements|qualifications|experience|skills|education|benefits|salary|location|remote|full-time|part-time|contract|permanent|temporary|company|team|department|manager|employee|candidate|apply|submit|send|email|contact|interview|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 5
                AND REGEXP_COUNT(LOWER(job_description),
                    '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                AND REGEXP_COUNT(job_description,
                    '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就业|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
            THEN TRUE
            ELSE FALSE
        END AS is_english{debug_clause}
    FROM your_table_name
    """


def get_sql_multi_field_language_detection_query() -> str:
    """
    Generate SQL query for multi-field language detection.
    Uses job_title as fallback when job_description detection has low confidence.
    """
    return """
    WITH language_detection AS (
        SELECT *,
            -- Job Description Language Detection
            CASE
                WHEN REGEXP_COUNT(LOWER(job_description),
                    '\\b(job|position|role|career|opportunity|hiring|employment|responsibilities|requirements|qualifications|experience|skills|education|benefits|salary|location|remote|full-time|part-time|contract|permanent|temporary|company|team|department|manager|employee|candidate|apply|submit|send|email|contact|interview|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 5
                    AND REGEXP_COUNT(LOWER(job_description),
                        '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                    AND REGEXP_COUNT(job_description,
                        '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就업|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
                THEN 'en'
                WHEN REGEXP_COUNT(job_description, '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|경력|경험|학력|기술|능력|자격|요건|조건|회사|기업|팀|부서|직원|인재|지원자)') > 2 THEN 'ko'
                WHEN REGEXP_COUNT(job_description, '(招聘|求职|工作|职位|岗位|就业|入职|经验|学历|技能|能力|资格|要求|条件|公司|企业|团队|部门|员工|人才|候选人)') > 2 THEN 'zh'
                WHEN REGEXP_COUNT(job_description, '(採用|求人|募集|入社|就職|仕事|職場|職種|業務|経験|学歴|技術|能力|資格|要件|条件|会社|企業|チーム|部署|社員|人材|応募者)') > 2 THEN 'ja'
                ELSE 'unknown'
            END AS desc_language,

            CASE
                WHEN REGEXP_COUNT(LOWER(job_description),
                    '\\b(job|position|role|career|opportunity|hiring|employment|responsibilities|requirements|qualifications|experience|skills|education|benefits|salary|location|remote|full-time|part-time|contract|permanent|temporary|company|team|department|manager|employee|candidate|apply|submit|send|email|contact|interview|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 5
                    AND REGEXP_COUNT(LOWER(job_description),
                        '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                    AND REGEXP_COUNT(job_description,
                        '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就업|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
                THEN 0.8
                WHEN REGEXP_COUNT(job_description,
                    '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|경력|경험|학력|기술|능력|자격|요건|조건|회사|기업|팀|부서|직원|인재|지원자|招聘|求职|工作|职位|岗位|就업|入职|经验|学历|기능|능력|资格|要求|条件|公司|企业|团队|部门|员工|인재|候选人|採用|求人|募集|입사|就직|仕事|직장|職種|업무|マネージャー|エンジニア|개발자|アナリスト|スペシャリスト|コーディネーター|アシスタント|ディレクター|リド|シニア|ジュニア|인턴)') > 2
                THEN 0.9
                ELSE 0.3
            END AS desc_confidence,

            -- Job Title Language Detection
            CASE
                WHEN REGEXP_COUNT(LOWER(job_title),
                    '\\b(job|position|role|career|opportunity|hiring|employment|manager|engineer|developer|analyst|specialist|coordinator|assistant|director|lead|senior|junior|intern|the|and|or|but|in|on|at|to|for|with|by|from|of|as|is|are|was|were|will|would|should|could|must|may|can)\\b') > 1
                    AND REGEXP_COUNT(LOWER(job_title),
                        '\\b(trabajo|empleo|puesto|opportunité|entreprise|arbeit|stelle|trabalho|emprego|lavoro|impiego)\\b') = 0
                    AND REGEXP_COUNT(job_title,
                        '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|招聘|求职|工作|职位|岗位|就업|入职|採用|求人|募集|入社|就職|仕事|職場|職種|業務)') = 0
                THEN 'en'
                WHEN REGEXP_COUNT(job_title,
                    '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|매니저|엔지니어|개발자|분석가|전문가|코디네이터|어시스턴트|디렉터|리드|시니어|주니어|인턴|광고|컨설턴트|계약직|전환|가능|마케팅|영업|운영|관리|담당|책임|팀장|부장|과장|대리|주임|사원|실장|스페셜리스트|어드바이저|풀타임|정규직|비정규직|근무지|위치|지역|본사|지점|사무실|招聘|求职|工作|职位|岗位|就업|入职|经理|工程师|开发者|分析师|专家|协调员|助理|总监|负责人|高级|初级|实习生|採用|구인|募集|입사|就職|仕事|직장|職種|업무|マネージャー|エンジニア|개발자|アナリスト|スペシャリスト|コーディネーター|アシスタント|ディレクター|リド|シニア|ジュニア|인턴)') > 0
                THEN 0.95
                ELSE 0.2
            END AS title_confidence
    )
    SELECT *,
        -- Multi-field decision logic
        CASE
            WHEN desc_confidence >= 0.7 THEN desc_language
            WHEN title_confidence >= 0.7 THEN title_language
            WHEN title_confidence > desc_confidence THEN title_language
            ELSE desc_language
        END AS detected_language,

        CASE
            WHEN desc_confidence >= 0.7 THEN desc_confidence
            WHEN title_confidence >= 0.7 THEN title_confidence
            WHEN title_confidence > desc_confidence THEN title_confidence
            ELSE desc_confidence
        END AS language_confidence,

        CASE
            WHEN desc_confidence >= 0.7 THEN 'job_description'
            WHEN title_confidence >= 0.7 THEN 'job_title'
            WHEN title_confidence > desc_confidence THEN 'job_title_fallback'
            ELSE 'job_description_fallback'
        END AS detection_source,

        CASE
            WHEN (CASE
                WHEN desc_confidence >= 0.7 THEN desc_language
                WHEN title_confidence >= 0.7 THEN title_language
                WHEN title_confidence > desc_confidence THEN title_language
                ELSE desc_language
            END) = 'en'
            AND (CASE
                WHEN desc_confidence >= 0.7 THEN desc_confidence
                WHEN title_confidence >= 0.7 THEN title_confidence
                WHEN title_confidence > desc_confidence THEN title_confidence
                ELSE desc_confidence
            END) >= 0.7
            THEN TRUE
            ELSE FALSE
        END AS is_english
    FROM language_detection
    """


# Convenience functions for easy import and usage
def detect_text_language(text: str, confidence_threshold: float = 0.7) -> Dict[str, Union[str, float, bool]]:
    """
    Convenience function to detect language of a single text.

    Args:
        text: Input text to analyze
        confidence_threshold: Minimum confidence score for reliable detection

    Returns:
        Dictionary with detected_language, language_confidence, is_english, and language_detection_method fields
    """
    detector = LanguageDetector(confidence_threshold=confidence_threshold)
    return detector.detect_language_comprehensive(text)


def detect_multi_field_language(job_title: str, job_description: str, confidence_threshold: float = 0.7, job_id: str = None, company_id: str = None, platform: str = None) -> Dict[str, Union[str, float, bool]]:
    """
    Convenience function for multi-field language detection.

    Args:
        job_title: Job title text
        job_description: Job description text
        confidence_threshold: Minimum confidence score for reliable detection
        job_id: Optional job ID for debugging
        company_id: Optional company ID for debugging
        platform: Optional platform for debugging

    Returns:
        Dictionary with detected_language, language_confidence, is_english,
        language_detection_method, and detection_source fields
    """
    detector = LanguageDetector(confidence_threshold=confidence_threshold)
    return detector.detect_language_multi_field(job_title, job_description, job_id, company_id, platform)


def process_job_dataframe(df: pd.DataFrame, text_column: str = 'job_description',
                         confidence_threshold: float = 0.7) -> pd.DataFrame:
    """
    Convenience function to process a job DataFrame with language detection.

    Args:
        df: Input DataFrame containing job data
        text_column: Name of column containing job description text
        confidence_threshold: Minimum confidence score for reliable detection

    Returns:
        DataFrame with added language detection columns
    """
    detector = LanguageDetector(confidence_threshold=confidence_threshold)
    return detector.process_dataframe(df, text_column)


def process_job_dataframe_multi_field(df: pd.DataFrame,
                                     title_column: str = 'job_title',
                                     description_column: str = 'job_description',
                                     confidence_threshold: float = 0.7) -> pd.DataFrame:
    """
    Convenience function to process a job DataFrame with multi-field language detection.

    Args:
        df: Input DataFrame containing job data
        title_column: Name of column containing job title
        description_column: Name of column containing job description text
        confidence_threshold: Minimum confidence score for reliable detection

    Returns:
        DataFrame with added language detection columns including detection_source
    """
    detector = LanguageDetector(confidence_threshold=confidence_threshold)
    return detector.process_dataframe_multi_field(df, title_column, description_column)


def filter_english_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to keep only English job postings.

    Args:
        df: DataFrame with language detection columns

    Returns:
        Filtered DataFrame containing only English jobs
    """
    if 'is_english' not in df.columns:
        raise ValueError("DataFrame must have 'is_english' column. Run language detection first.")

    return df[df['is_english'] == True].copy()


# Example usage and testing
if __name__ == "__main__":
    # Test the language detector
    detector = LanguageDetector()

    test_texts = [
        "We are looking for a Software Engineer to join our team. The candidate should have experience with Python and SQL.",
        "Estamos buscando un Ingeniero de Software para unirse a nuestro equipo. El candidato debe tener experiencia con Python y SQL.",
        "Nous recherchons un Ingénieur Logiciel pour rejoindre notre équipe. Le candidat doit avoir de l'expérience avec Python et SQL.",
        "This is a short text.",
        "",
    ]

    for text in test_texts:
        result = detector.detect_language_comprehensive(text)
        print(f"Text: {text[:50]}...")
        print(f"Result: {result}")
        print("-" * 50)