"""
Test module for language detection functionality.
"""

import pytest
import pandas as pd
from .language_detection import (
    LanguageDetector,
    detect_text_language,
    process_job_dataframe,
    filter_english_jobs,
    get_sql_language_detection_query
)


class TestLanguageDetector:
    """Test cases for the LanguageDetector class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.detector = LanguageDetector(confidence_threshold=0.7)

    def test_english_text_detection(self):
        """Test detection of English text."""
        english_text = """
        We are looking for a Software Engineer to join our dynamic team.
        The ideal candidate should have experience with Python, SQL, and cloud technologies.
        Responsibilities include developing applications, collaborating with team members,
        and ensuring high-quality code delivery.
        """

        result = self.detector.detect_language_comprehensive(english_text)

        assert result['detected_language'] == 'en'
        assert result['is_english'] == True
        assert result['language_confidence'] > 0.7

    def test_spanish_text_detection(self):
        """Test detection of Spanish text."""
        spanish_text = """
        Estamos buscando un Ingeniero de Software para unirse a nuestro equipo dinámico.
        El candidato ideal debe tener experiencia con Python, SQL y tecnologías en la nube.
        Las responsabilidades incluyen desarrollar aplicaciones, colaborar con miembros del equipo
        y garantizar la entrega de código de alta calidad.
        """

        result = self.detector.detect_language_comprehensive(spanish_text)

        assert result['detected_language'] in ['es', 'spanish']
        assert result['is_english'] == False

    def test_short_text_handling(self):
        """Test handling of very short text."""
        short_text = "Hi"

        result = self.detector.detect_language_comprehensive(short_text)

        assert result['detected_language'] == 'unknown'
        assert result['is_english'] == False
        assert result['language_confidence'] == 0.0

    def test_empty_text_handling(self):
        """Test handling of empty text."""
        result = self.detector.detect_language_comprehensive("")

        assert result['detected_language'] == 'unknown'
        assert result['is_english'] == False
        assert result['language_confidence'] == 0.0

    def test_mixed_language_text(self):
        """Test handling of mixed language text."""
        mixed_text = """
        We are looking for a Software Engineer. Estamos buscando un Ingeniero.
        The position requires experience with Python and collaboration skills.
        """

        result = self.detector.detect_language_comprehensive(mixed_text)

        # Result should indicate some confidence level
        assert 'detected_language' in result
        assert 'language_confidence' in result
        assert 'is_english' in result


class TestConvenienceFunctions:
    """Test cases for convenience functions."""

    def test_detect_text_language(self):
        """Test the convenience function for single text detection."""
        text = "We are hiring a Data Scientist with machine learning experience."

        result = detect_text_language(text)

        assert 'detected_language' in result
        assert 'language_confidence' in result
        assert 'is_english' in result

    def test_process_job_dataframe(self):
        """Test processing a DataFrame with job descriptions."""
        # Create test DataFrame
        test_data = {
            'job_id': [1, 2, 3],
            'job_title': ['Software Engineer', 'Data Scientist', 'Product Manager'],
            'job_description': [
                'We are looking for a Software Engineer with Python experience.',
                'Estamos buscando un Científico de Datos con experiencia en Python.',
                'We need a Product Manager to lead our product development team.'
            ]
        }
        df = pd.DataFrame(test_data)

        # Process the DataFrame
        result_df = process_job_dataframe(df, 'job_description')

        # Check that new columns were added
        assert 'detected_language' in result_df.columns
        assert 'language_confidence' in result_df.columns
        assert 'is_english' in result_df.columns

        # Check that we have the correct number of rows
        assert len(result_df) == 3

        # Check that at least some jobs are detected as English
        english_jobs = result_df[result_df['is_english'] == True]
        assert len(english_jobs) >= 1

    def test_filter_english_jobs(self):
        """Test filtering DataFrame for English jobs only."""
        # Create test DataFrame with language detection results
        test_data = {
            'job_id': [1, 2, 3],
            'job_title': ['Software Engineer', 'Data Scientist', 'Product Manager'],
            'detected_language': ['en', 'es', 'en'],
            'language_confidence': [0.85, 0.92, 0.78],
            'is_english': [True, False, True]
        }
        df = pd.DataFrame(test_data)

        # Filter for English jobs
        english_df = filter_english_jobs(df)

        # Check that only English jobs remain
        assert len(english_df) == 2
        assert all(english_df['is_english'] == True)
        assert list(english_df['job_id']) == [1, 3]

    def test_filter_english_jobs_missing_column(self):
        """Test error handling when is_english column is missing."""
        test_data = {
            'job_id': [1, 2, 3],
            'job_title': ['Software Engineer', 'Data Scientist', 'Product Manager']
        }
        df = pd.DataFrame(test_data)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame must have 'is_english' column"):
            filter_english_jobs(df)


class TestSQLGeneration:
    """Test cases for SQL query generation."""

    def test_get_sql_language_detection_query(self):
        """Test that SQL query generation returns valid SQL."""
        sql_query = get_sql_language_detection_query()

        # Basic checks for SQL structure
        assert 'SELECT' in sql_query
        assert 'detected_language' in sql_query
        assert 'language_confidence' in sql_query
        assert 'is_english' in sql_query
        assert 'CASE' in sql_query
        assert 'REGEXP_COUNT' in sql_query


class TestPerformance:
    """Test cases for performance characteristics."""

    def test_large_text_processing(self):
        """Test processing of large text."""
        # Create a large text sample
        large_text = "We are looking for a Software Engineer. " * 1000

        detector = LanguageDetector()
        result = detector.detect_language_comprehensive(large_text)

        # Should still detect as English
        assert result['detected_language'] == 'en'
        assert result['is_english'] == True

    def test_batch_processing_performance(self):
        """Test processing multiple texts in batch."""
        # Create a list of test texts
        texts = [
            f"We are looking for a Software Engineer position {i}. "
            f"The candidate should have experience with Python and SQL."
            for i in range(100)
        ]

        detector = LanguageDetector()

        # Process all texts
        results = [detector.detect_language_comprehensive(text) for text in texts]

        # Check that all were processed
        assert len(results) == 100

        # Check that most are detected as English
        english_count = sum(1 for r in results if r['is_english'])
        assert english_count >= 95  # At least 95% should be detected as English


if __name__ == "__main__":
    # Run some basic tests
    print("Running basic language detection tests...")

    # Test English detection
    result = detect_text_language(
        "We are looking for a Software Engineer to join our team. "
        "The candidate should have experience with Python and SQL."
    )
    print(f"English text result: {result}")

    # Test Spanish detection
    result = detect_text_language(
        "Estamos buscando un Ingeniero de Software para unirse a nuestro equipo. "
        "El candidato debe tener experiencia con Python y SQL."
    )
    print(f"Spanish text result: {result}")

    # Test DataFrame processing
    test_data = {
        'job_description': [
            'We are looking for a Software Engineer with Python experience.',
            'Estamos buscando un Científico de Datos con experiencia en machine learning.',
            'We need a Product Manager to lead our development team.'
        ]
    }
    df = pd.DataFrame(test_data)
    processed_df = process_job_dataframe(df)
    print(f"DataFrame processing result:\n{processed_df[['detected_language', 'language_confidence', 'is_english']]}")

    print("Basic tests completed successfully!")