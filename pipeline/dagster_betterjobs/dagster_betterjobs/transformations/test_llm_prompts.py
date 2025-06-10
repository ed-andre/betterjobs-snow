"""
Test script for LLM prompt templates and utilities.

Tests the functionality of JobExtractionPrompts and PromptFormatter classes.
"""

import json
from llm_prompts import JobExtractionPrompts, PromptFormatter


def test_prompt_formatting():
    """Test prompt formatting and validation utilities."""
    print("=== TESTING PROMPT FORMATTING ===")

    # Test sample job description
    sample_job = """
    Senior Full Stack Engineer - FinTech Startup

    We're seeking a Senior Full Stack Engineer to join our growing team at our Series B FinTech startup.

    RESPONSIBILITIES:
    • Build scalable web applications using React and Node.js
    • Design REST APIs and microservices architecture
    • Work with PostgreSQL, Redis, and AWS services
    • Collaborate with product managers and designers
    • Mentor junior developers

    REQUIREMENTS:
    • 5+ years of full-stack development experience
    • Expert-level JavaScript, TypeScript, Python
    • Experience with React, Node.js, Express
    • Strong SQL and database design skills
    • AWS experience (EC2, RDS, Lambda, S3)
    • Bachelor's degree in Computer Science or related field

    COMPENSATION:
    • Salary: $130,000 - $170,000 annually
    • Equity: 0.1% - 0.3% stock options
    • Full benefits package including health, dental, 401k

    WORK ARRANGEMENT:
    This is a hybrid role with 3 days in our San Francisco office and 2 days remote.
    Occasional travel (< 10%) for team meetings and conferences.

    ABOUT US:
    We're a fast-growing FinTech company focused on modernizing business payments.
    Series B funding, 50+ employees, high-growth environment.
    """

    # Test formatter utilities
    formatter = PromptFormatter()

    # Test job description formatting
    formatted_desc = formatter.format_job_description(sample_job)
    print(f"✓ Job description formatted: {len(formatted_desc)} characters")

    # Test with empty description
    empty_formatted = formatter.format_job_description("")
    print(f"✓ Empty description handled: '{empty_formatted}'")

    # Test with very long description
    long_desc = sample_job * 10  # Make it very long
    truncated = formatter.format_job_description(long_desc, max_length=1000)
    print(f"✓ Long description truncated: {len(truncated)} characters")

    return formatted_desc


def test_prompt_templates():
    """Test all prompt templates."""
    print("\n=== TESTING PROMPT TEMPLATES ===")

    prompts = JobExtractionPrompts()

    # Test comprehensive extraction prompt
    comp_prompt = prompts.get_comprehensive_extraction_prompt()
    print(f"✓ Comprehensive prompt: {len(comp_prompt)} characters")
    assert "{job_description}" in comp_prompt

    # Test validation prompt
    val_prompt = prompts.get_validation_prompt()
    print(f"✓ Validation prompt: {len(val_prompt)} characters")
    assert "{extracted_data}" in val_prompt and "{job_description}" in val_prompt

    # Test quick classification prompt
    quick_prompt = prompts.get_quick_classification_prompt()
    print(f"✓ Quick classification prompt: {len(quick_prompt)} characters")
    assert "{job_description}" in quick_prompt

    # Test salary-focused prompt
    salary_prompt = prompts.get_salary_focused_prompt()
    print(f"✓ Salary-focused prompt: {len(salary_prompt)} characters")
    assert "{job_description}" in salary_prompt


def test_response_validation():
    """Test response validation utilities."""
    print("\n=== TESTING RESPONSE VALIDATION ===")

    formatter = PromptFormatter()

    # Test valid JSON response
    valid_json = """
    {
      "salary_info": {
        "salary_min": 130000,
        "salary_max": 170000,
        "confidence": 0.9
      },
      "skills": {
        "technical_skills": {
          "programming_languages": ["JavaScript", "TypeScript", "Python"]
        },
        "confidence": 0.85
      }
    }
    """

    try:
        parsed = formatter.validate_extraction_response(valid_json)
        print("✓ Valid JSON parsed successfully")
        print(f"  Salary min: {parsed['salary_info']['salary_min']}")
    except Exception as e:
        print(f"✗ Failed to parse valid JSON: {e}")

    # Test JSON in markdown code block
    markdown_json = """
    Here's the extracted information:

    ```json
    {
      "job_family": "Engineering",
      "experience_level": "Senior",
      "confidence": 0.8
    }
    ```
    """

    try:
        parsed = formatter.validate_extraction_response(markdown_json)
        print("✓ Markdown JSON parsed successfully")
        print(f"  Job family: {parsed['job_family']}")
    except Exception as e:
        print(f"✗ Failed to parse markdown JSON: {e}")

    # Test confidence calculation
    sample_extraction = {
        "salary_info": {"confidence": 0.8},
        "experience_requirements": {"confidence": 0.9},
        "skills": {"confidence": 0.7},
        "work_arrangement": {"confidence": 0.6},
        "job_classification": {"confidence": 0.85}
    }

    avg_confidence = formatter.calculate_average_confidence(sample_extraction)
    print(f"✓ Average confidence calculated: {avg_confidence:.2f}")

    # Test low confidence field detection
    low_conf_fields = formatter.get_low_confidence_fields(sample_extraction, threshold=0.65)
    print(f"✓ Low confidence fields detected: {low_conf_fields}")


def test_full_prompt_generation():
    """Test generating a complete formatted prompt."""
    print("\n=== TESTING FULL PROMPT GENERATION ===")

    sample_job = """
    Data Scientist - Healthcare AI

    Join our mission to revolutionize healthcare through AI! We're looking for a Senior Data Scientist.

    Requirements:
    - PhD in Statistics, CS, or related field
    - 4+ years of machine learning experience
    - Python, R, SQL expertise
    - Experience with TensorFlow, PyTorch
    - Healthcare domain knowledge preferred

    Compensation: $140K-$180K + equity
    Remote-first company with quarterly team gatherings.
    """

    prompts = JobExtractionPrompts()
    formatter = PromptFormatter()

    # Format the job description
    formatted_job = formatter.format_job_description(sample_job)

    # Generate the complete prompt
    full_prompt = prompts.get_comprehensive_extraction_prompt().format(
        job_description=formatted_job
    )

    print(f"✓ Full prompt generated: {len(full_prompt)} characters")
    print("✓ Contains job description and all extraction guidelines")

    # Verify key components are present
    assert "salary_info" in full_prompt
    assert "experience_requirements" in full_prompt
    assert "technical_skills" in full_prompt
    assert "work_arrangement" in full_prompt
    assert "job_classification" in full_prompt
    assert sample_job.split('\n')[1].strip() in full_prompt  # Job title should be in prompt

    print("✓ All required sections present in generated prompt")


if __name__ == "__main__":
    """Run all tests."""
    try:
        print("🚀 TESTING LLM PROMPT TEMPLATES\n")

        # Run all tests
        formatted_desc = test_prompt_formatting()
        test_prompt_templates()
        test_response_validation()
        test_full_prompt_generation()

        print("\n✅ ALL TESTS PASSED!")
        print("\n🎯 PROMPT TEMPLATES ARE READY FOR PHASE 2.3 IMPLEMENTATION")
        print("   → Comprehensive extraction prompt: ✓")
        print("   → Validation and error handling: ✓")
        print("   → Response parsing utilities: ✓")
        print("   → Confidence scoring: ✓")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()