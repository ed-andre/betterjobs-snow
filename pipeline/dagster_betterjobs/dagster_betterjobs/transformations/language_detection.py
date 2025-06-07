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
            english_matches += len(pattern.findall(text))

        # Count non-English pattern matches
        non_english_matches = 0
        for lang_patterns in self.non_english_patterns.values():
            for pattern in lang_patterns:
                non_english_matches += len(pattern.findall(text))

        # Calculate ratio
        total_matches = english_matches + non_english_matches
        if total_matches == 0:
            return 0.5  # Neutral if no patterns match

        return english_matches / total_matches

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

    def detect_language_sql_fallback(self, text: str) -> Tuple[str, float]:
        """
        Fallback language detection using pattern matching.

        Args:
            text: Input text to analyze

        Returns:
            Tuple of (detected_language, confidence_score)
        """
        if not text or len(text.strip()) < 10:
            return 'unknown', 0.0

        english_score = self._calculate_english_score(text)

        if english_score >= 0.7:
            return 'en', english_score
        elif english_score <= 0.3:
            # Try to identify specific non-English language
            best_lang = 'other'
            best_score = 0.0

            for lang, patterns in self.non_english_patterns.items():
                matches = 0
                for pattern in patterns:
                    matches += len(pattern.findall(text))

                if matches > best_score:
                    best_score = matches
                    best_lang = lang[:2]  # Convert to 2-letter code

            return best_lang, 1.0 - english_score
        else:
            return 'mixed', english_score

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

        # Fallback detection using patterns
        fallback_lang, fallback_confidence = self.detect_language_sql_fallback(text)

        # Combine results and track which method was used
        detection_method = None
        if primary_confidence >= self.confidence_threshold:
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


def get_sql_language_detection_query() -> str:
    """
    Generate SQL query for language detection as a fallback method.
    This can be used directly in SQL transformations.
    """
    return """
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
                '(채용|구인|모집|입사|취업|일자리|직장|직무|업무|경력|경험|학력|기술|능력|자격|요건|조건|회사|기업|팀|부서|직원|인재|지원자|招聘|求职|工作|职位|岗位|就业|入职|经验|学历|技能|能力|资格|要求|条件|公司|企业|团队|部门|员工|人才|候选人|採用|求人|募集|入社|就職|仕事|職場|職種|業務|経験|学歴|技術|能力|資格|要件|条件|会社|企業|チーム|部署|社員|人材|応募者)') > 2
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
        END AS is_english
    FROM your_table_name
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