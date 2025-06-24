"""
Skill Consolidation Utilities for ENHANCEMENT-023

Intelligent consolidation of skill variants (plural/singular forms) using linguistic
libraries to eliminate duplicates while preserving data integrity.

Functions:
- generate_skill_variants: Generate potential plural/singular variants
- consolidate_skill_variants: Find and merge skill groups with matching variants
- choose_preferred_form: Select canonical form using business rules

Dependencies:
- inflect: For robust English pluralization handling
"""

import re
import inflect
from typing import Dict, List, Tuple, Optional, Any, NamedTuple
from dataclasses import dataclass
from collections import defaultdict

from .skill_domain_rules import get_preferred_form, is_protected_term, validate_consolidation_result


class SkillData(NamedTuple):
    """Data structure representing a skill with its metadata."""
    skill_name: str
    skill_category: str
    skill_subcategory: Optional[str]
    frequency_count: int
    confidence_score: float
    original_variants: List[str]
    first_seen_date: Optional[str]
    last_seen_date: Optional[str]


@dataclass
class ConsolidationConfig:
    """Configuration for skill consolidation behavior."""
    enabled: bool = True
    preferred_form: str = "singular"  # "singular", "plural", "most_frequent"
    min_frequency_threshold: int = 2


# Initialize inflect engine
p = inflect.engine()




def generate_skill_variants(skill_name: str) -> List[str]:
    """
    Generate potential plural/singular variants using inflect library.

    Handles:
    - Simple pluralization (skill -> skills)
    - Compound phrases (AI Tool -> AI Tools)
    - Domain-specific rules

    Args:
        skill_name: Original skill name to generate variants for

    Returns:
        List of all potential variants including original
    """
    if not skill_name or not skill_name.strip():
        return []

    skill_name = skill_name.strip()
    variants = [skill_name]
    skill_lower = skill_name.lower()

    # Check domain-specific rules first
    domain_preferred = get_preferred_form(skill_name)
    if domain_preferred != skill_name:
        if domain_preferred not in variants:
            variants.append(domain_preferred)
        # If it's a protected term, return early with just the preferred form
        if is_protected_term(skill_name):
            return list(set(variants))

    # Handle compound phrases (e.g., "AI Tools" -> "AI Tool")
    words = skill_name.split()
    if len(words) > 1:
        # Try pluralizing/singularizing the last word
        last_word = words[-1]
        rest_words = " ".join(words[:-1])

        try:
            # Generate singular form of last word
            singular_last = p.singular_noun(last_word)
            if singular_last:
                singular_compound = f"{rest_words} {singular_last}"
                if singular_compound not in variants:
                    variants.append(singular_compound)

            # Generate plural form of last word
            plural_last = p.plural(last_word)
            if plural_last and plural_last != last_word:
                plural_compound = f"{rest_words} {plural_last}"
                if plural_compound not in variants:
                    variants.append(plural_compound)

        except Exception:
            # If inflect fails, continue with other methods
            pass

    # Handle single words
    try:
        # Generate singular form
        singular = p.singular_noun(skill_name)
        if singular and singular not in variants:
            variants.append(singular)

        # Generate plural form
        plural = p.plural(skill_name)
        if plural and plural != skill_name and plural not in variants:
            variants.append(plural)

    except Exception:
        # If inflect fails, just return original
        pass

    return list(set(variants))


def consolidate_skill_variants(skills_dict: Dict[str, SkillData], config: ConsolidationConfig = None) -> Dict[str, SkillData]:
    """
    Find skill groups with matching variants and merge their data.

    Process:
    1. Generate variants for all skills
    2. Group skills by overlapping variants
    3. Merge frequency counts and metadata for each group
    4. Choose preferred canonical form

    Args:
        skills_dict: Dictionary mapping skill names to SkillData
        config: Consolidation configuration options

    Returns:
        Dictionary with consolidated skills using canonical forms as keys
    """
    if config is None:
        config = ConsolidationConfig()

    if not config.enabled:
        return skills_dict

    # Group skills by their variants
    variant_to_skills = defaultdict(list)
    skill_to_variants = {}

    # Generate variants for each skill
    for skill_name, skill_data in skills_dict.items():
        if skill_data.frequency_count < config.min_frequency_threshold:
            continue

        variants = generate_skill_variants(skill_name)
        skill_to_variants[skill_name] = variants

        # Map each variant to the skills that can generate it
        for variant in variants:
            variant_lower = variant.lower().strip()
            variant_to_skills[variant_lower].append((skill_name, skill_data))

        # Find groups of skills that share variants
    skill_groups = []
    processed_skills = set()

    for variant_lower, skill_list in variant_to_skills.items():
        if len(skill_list) > 1:  # Only consolidate if multiple skills share a variant
            # Collect all skills in this consolidation group
            group_skill_names = set()
            for skill_name, skill_data in skill_list:
                if skill_name not in processed_skills:
                    group_skill_names.add(skill_name)

            if len(group_skill_names) > 1:
                # Convert back to skill data tuples for processing
                group_skills = [(name, skills_dict[name]) for name in group_skill_names]
                skill_groups.append(group_skills)
                processed_skills.update(group_skill_names)

    # Consolidate each group
    consolidated_skills = {}

    # Add non-consolidated skills first
    for skill_name, skill_data in skills_dict.items():
        if skill_name not in processed_skills:
            consolidated_skills[skill_name] = skill_data

    # Process each consolidation group
    for group in skill_groups:
        consolidated_skill = _merge_skill_group(group, config)
        if consolidated_skill:
            canonical_name, skill_data = consolidated_skill
            consolidated_skills[canonical_name] = skill_data

    return consolidated_skills


def choose_preferred_form(variants: List[Tuple[str, SkillData]], config: ConsolidationConfig = None) -> str:
    """
    Choose the preferred canonical form from a list of skill variants.

    Business rules:
    - Check domain-specific rules first
    - Most frequent form if prefer_most_frequent
    - Singular form if prefer_singular
    - Plural form if prefer_plural
    - Alphabetically first as fallback

    Args:
        variants: List of (skill_name, skill_data) tuples
        config: Configuration for form preference

    Returns:
        Preferred canonical skill name
    """
    if not variants:
        return ""

    if config is None:
        config = ConsolidationConfig()

    # Check domain-specific rules first
    for skill_name, _ in variants:
        domain_preferred = get_preferred_form(skill_name)
        if domain_preferred != skill_name:
            return domain_preferred

    # Sort by frequency (descending) then alphabetically
    sorted_variants = sorted(variants, key=lambda x: (-x[1].frequency_count, x[0]))

    if config.preferred_form == "most_frequent":
        return sorted_variants[0][0]

    elif config.preferred_form == "singular":
        # Try to find singular forms
        for skill_name, _ in sorted_variants:
            try:
                # Check if this appears to be singular
                plural_form = p.plural(skill_name)
                if plural_form != skill_name:  # If plural is different, this is likely singular
                    return skill_name
            except Exception:
                pass
        # Fallback to most frequent
        return sorted_variants[0][0]

    elif config.preferred_form == "plural":
        # Try to find plural forms
        for skill_name, _ in sorted_variants:
            try:
                singular_form = p.singular_noun(skill_name)
                if singular_form:  # If singular exists, this is plural
                    return skill_name
            except Exception:
                pass
        # Fallback to most frequent
        return sorted_variants[0][0]

    # Default: return most frequent
    return sorted_variants[0][0]


def _merge_skill_group(group: List[Tuple[str, SkillData]], config: ConsolidationConfig) -> Optional[Tuple[str, SkillData]]:
    """
    Merge a group of related skill variants into a single consolidated skill.

    Args:
        group: List of (skill_name, skill_data) tuples to merge
        config: Consolidation configuration

    Returns:
        Tuple of (canonical_name, consolidated_skill_data) or None if merge fails
    """
    if not group:
        return None

    # Choose canonical form
    canonical_name = choose_preferred_form(group, config)

    # Validate consolidation results against domain rules
    for skill_name, _ in group:
        if not validate_consolidation_result(skill_name, canonical_name):
            # If validation fails, fall back to most frequent original form
            canonical_name = max(group, key=lambda x: x[1].frequency_count)[0]
            break

    # Merge all data
    total_frequency = sum(skill_data.frequency_count for _, skill_data in group)
    all_variants = []
    earliest_date = None
    latest_date = None
    avg_confidence = 0.0

    # Use the most common category and subcategory
    categories = [skill_data.skill_category for _, skill_data in group if skill_data.skill_category]
    subcategories = [skill_data.skill_subcategory for _, skill_data in group if skill_data.skill_subcategory]

    primary_category = max(set(categories), key=categories.count) if categories else ""
    primary_subcategory = max(set(subcategories), key=subcategories.count) if subcategories else None

    for skill_name, skill_data in group:
        # Collect all original variants
        all_variants.extend(skill_data.original_variants)
        all_variants.append(skill_name)  # Include the skill name itself

        # Track date ranges
        if skill_data.first_seen_date:
            if earliest_date is None or skill_data.first_seen_date < earliest_date:
                earliest_date = skill_data.first_seen_date

        if skill_data.last_seen_date:
            if latest_date is None or skill_data.last_seen_date > latest_date:
                latest_date = skill_data.last_seen_date

        # Average confidence weighted by frequency
        avg_confidence += skill_data.confidence_score * skill_data.frequency_count

    avg_confidence = avg_confidence / total_frequency if total_frequency > 0 else 0.0

    # Remove duplicates from variants and sort
    unique_variants = sorted(list(set(all_variants)))

    # Create consolidated skill data
    consolidated_skill = SkillData(
        skill_name=canonical_name,
        skill_category=primary_category,
        skill_subcategory=primary_subcategory,
        frequency_count=total_frequency,
        confidence_score=avg_confidence,
        original_variants=unique_variants,
        first_seen_date=earliest_date,
        last_seen_date=latest_date
    )

    return canonical_name, consolidated_skill


def get_consolidation_summary(original_skills: Dict[str, SkillData],
                            consolidated_skills: Dict[str, SkillData]) -> Dict[str, Any]:
    """
    Generate a summary of the consolidation process for monitoring and validation.

    Args:
        original_skills: Skills before consolidation
        consolidated_skills: Skills after consolidation

    Returns:
        Dictionary with consolidation metrics and examples
    """
    summary = {
        "original_skill_count": len(original_skills),
        "consolidated_skill_count": len(consolidated_skills),
        "consolidation_ratio": len(consolidated_skills) / len(original_skills) if original_skills else 0,
        "skills_merged": len(original_skills) - len(consolidated_skills),
        "merge_examples": []
    }

    # Find examples of merged skills
    original_names = set(original_skills.keys())
    consolidated_names = set(consolidated_skills.keys())

    for consolidated_name, consolidated_data in consolidated_skills.items():
        if len(consolidated_data.original_variants) > 1:
            # This skill was formed by consolidation
            variants_in_original = [v for v in consolidated_data.original_variants if v in original_names]
            if len(variants_in_original) > 1:
                summary["merge_examples"].append({
                    "canonical_form": consolidated_name,
                    "merged_variants": variants_in_original,
                    "total_frequency": consolidated_data.frequency_count
                })

    # Limit examples to top 10 by frequency
    summary["merge_examples"] = sorted(
        summary["merge_examples"],
        key=lambda x: x["total_frequency"],
        reverse=True
    )[:10]

    return summary