"""
Unit Tests for Skill Consolidation Utilities

Tests for ENHANCEMENT-023 Intelligent Skills Variant Consolidation
"""

import pytest
from typing import Dict, List
from dagster_betterjobs.utils.skill_consolidation import (
    SkillData,
    ConsolidationConfig,
    generate_skill_variants,
    consolidate_skill_variants,
    choose_preferred_form,
    get_consolidation_summary
)
from dagster_betterjobs.utils.skill_domain_rules import (
    is_protected_term,
    get_preferred_form,
    validate_consolidation_result
)


class TestGenerateSkillVariants:
    """Test variant generation functionality."""

    def test_simple_pluralization(self):
        """Test basic plural/singular generation."""
        variants = generate_skill_variants("Framework")
        assert "Framework" in variants
        assert "Frameworks" in variants

    def test_compound_phrases(self):
        """Test compound phrase handling."""
        variants = generate_skill_variants("AI Tools")
        assert "AI Tools" in variants
        assert "AI Tool" in variants

    def test_domain_specific_rules(self):
        """Test domain-specific rule application."""
        variants = generate_skill_variants("APIs")
        assert "API" in variants
        assert "APIs" in variants

    def test_protected_terms(self):
        """Test that protected terms are handled correctly."""
        variants = generate_skill_variants("Analysis")
        assert "Analysis" in variants

    def test_empty_input(self):
        """Test handling of empty or invalid input."""
        assert generate_skill_variants("") == []
        assert generate_skill_variants("   ") == []

    def test_domain_rules_integration(self):
        """Test that domain rules from skill_domain_rules.py are properly used."""
        # Test that protected terms are handled correctly
        assert is_protected_term("Analysis")
        assert is_protected_term("Business")

        # Test that preferred forms are applied
        assert get_preferred_form("apis") == "API"
        assert get_preferred_form("methodologies") == "Methodology"

        # Test domain rules in variant generation
        variants = generate_skill_variants("APIs")
        assert "API" in variants  # Should include the preferred form


class TestConsolidateSkillVariants:
    """Test skill consolidation functionality."""

    def setup_method(self):
        """Set up test data."""
        self.sample_skills = {
            "Framework": SkillData(
                skill_name="Framework",
                skill_category="tools",
                skill_subcategory="development",
                frequency_count=100,
                confidence_score=0.9,
                original_variants=["Framework"],
                first_seen_date="2023-01-01",
                last_seen_date="2023-12-31"
            ),
            "Frameworks": SkillData(
                skill_name="Frameworks",
                skill_category="tools",
                skill_subcategory="development",
                frequency_count=75,
                confidence_score=0.8,
                original_variants=["Frameworks"],
                first_seen_date="2023-02-01",
                last_seen_date="2023-11-30"
            ),
            "Database": SkillData(
                skill_name="Database",
                skill_category="data",
                skill_subcategory="storage",
                frequency_count=200,
                confidence_score=0.95,
                original_variants=["Database"],
                first_seen_date="2023-01-15",
                last_seen_date="2023-12-15"
            )
        }

    def test_basic_consolidation(self):
        """Test basic consolidation of plural/singular variants."""
        config = ConsolidationConfig(preferred_form="singular")
        consolidated = consolidate_skill_variants(self.sample_skills, config)

        # Should consolidate Framework/Frameworks into one entry
        assert len(consolidated) == 2  # Database + consolidated Framework

        # Check that consolidation combined frequencies
        framework_found = False
        for skill_name, skill_data in consolidated.items():
            if "framework" in skill_name.lower():
                framework_found = True
                assert skill_data.frequency_count == 175  # 100 + 75
                assert len(skill_data.original_variants) >= 2
                break
        assert framework_found

    def test_disabled_consolidation(self):
        """Test that consolidation can be disabled."""
        config = ConsolidationConfig(enabled=False)
        consolidated = consolidate_skill_variants(self.sample_skills, config)

        # Should return original skills unchanged
        assert len(consolidated) == len(self.sample_skills)
        assert consolidated == self.sample_skills


if __name__ == "__main__":
    pytest.main([__file__])