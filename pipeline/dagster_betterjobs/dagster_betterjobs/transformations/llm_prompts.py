"""
LLM Prompt Templates for Job Information Extraction

This module contains optimized prompt templates for extracting structured information
from job descriptions using Gemini LLM. Templates are designed for single comprehensive
extraction to minimize API calls and improve consistency.
"""

import json
from typing import Dict, Any, Optional


class JobExtractionPrompts:
    """
    Collection of prompt templates for extracting structured job information.

    Designed for gemini-2.5-flash-lite-preview-06-17 model with JSON response format.
    """

    @staticmethod
    def get_comprehensive_extraction_prompt() -> str:
        """
        Master prompt template for extracting ALL structured information in a single API call.

        Extracts:
        - Salary information (8 fields)
        - Experience requirements (6 fields)
        - Technical & soft skills (3 consolidated fields)
        - Work arrangement (6 fields)
        - Job classification (8 fields)

        Returns: Formatted prompt string
        """
        return '''
Analyze this job posting and extract ALL relevant structured information. Return a JSON object with exactly this structure:

{{
  "salary_info": {{
    "salary_min": number or null,
    "salary_max": number or null,
    "salary_currency": "USD" (default) or "EUR"|"GBP"|"CAD",
    "salary_period": "annually"|"hourly"|"monthly" or null,
    "salary_type": "base"|"total"|"contract" or null,
    "equity_mentioned": true/false,
    "bonus_mentioned": true/false,
    "confidence": 0.0-1.0
  }},
  "experience_requirements": {{
    "min_years_experience": number or null,
    "max_years_experience": number or null,
    "experience_level": "Entry"|"Mid"|"Senior"|"Executive"|"Internship",
    "specific_technologies_years": {{"Python": 3, "React": 2, "AWS": 1}},
    "education_requirements": ["Bachelor's degree", "Master's preferred", etc],
    "certifications": ["AWS Certified", "PMP", "Scrum Master"],
    "confidence": 0.0-1.0
  }},
  "skills": {{
    "technical_skills": ["Python", "JavaScript", "PostgreSQL", "AWS", "React", "Docker"],
    "soft_skills": ["Communication", "Leadership", "Problem Solving"],
    "confidence": 0.0-1.0
  }},
  "work_arrangement": {{
    "work_type": "Remote"|"Hybrid"|"On-site"|"Flexible",
    "remote_flexibility": "description of remote policy" or null,
    "travel_requirements": "percentage or description" or null,
    "office_locations": ["San Francisco, CA", "New York, NY"],
    "timezone_requirements": "Pacific Time preferred" or null,
    "confidence": 0.0-1.0
  }},
  "job_classification": {{
    "job_family": "Engineering"|"Data"|"Product"|"Sales"|"Marketing"|"Operations"|"Design"|"Finance"|"HR"|"Legal"|"Other",
    "job_sub_family": "Backend Engineering"|"Frontend Engineering"|"Data Science"|"Product Management", etc,
    "seniority_level": "Entry"|"Mid"|"Senior"|"Staff"|"Principal"|"Director"|"VP"|"C-Level",
    "primary_keywords": ["keyword1", "keyword2", "keyword3"],
    "industry_keywords": ["FinTech", "Healthcare", "E-commerce"],
    "role_type": "Individual Contributor"|"Manager"|"Director"|"VP"|"C-Level",
    "team_size": "5-10 engineers" or null,
    "confidence": 0.0-1.0
  }}
}}

EXTRACTION GUIDELINES:
1. SALARY: Look for explicit salary ranges, hourly rates, or compensation mentions. Include ranges like "$80K-$120K", "$45/hr", "up to $150,000". Set equity_mentioned=true if stock options, RSUs, or equity are mentioned.

2. EXPERIENCE: Extract years of experience required (e.g., "3-5 years" → min=3, max=5). For experience_level, use: Entry (0-2 years), Mid (3-5 years), Senior (6+ years), Executive (10+ years). For specific_technologies_years, extract technology requirements with years (e.g., "3+ years Python" → {{"Python": 3}}).

3. TECHNICAL SKILLS: Extract specific technologies, software, tools, languages, frameworks, technical abilities as a flat array. Use EXACT NAMES from the Lightcast Open Skills Taxonomy whenever possible. Do not group by category or create nested objects. Be precise - don't include general terms like "programming" or "software".

4. SOFT SKILLS: Extract interpersonal and professional skills like "communication", "leadership", "problem-solving", "teamwork". Use standardized terms from Lightcast Open Skills Taxonomy when available.

5. WORK ARRANGEMENT: Identify remote policy, office requirements, travel needs, timezone preferences. For work_type: Remote (100% remote), Hybrid (mix of remote/office), On-site (office required).

6. CLASSIFICATION: Determine job family and seniority. Primary keywords should be the 3-5 most important technical/role terms. Industry keywords identify the business domain. Extract team_size if mentioned (e.g., "join our 8-person engineering team").

7. CERTIFICATIONS: Look for required or preferred certifications, licenses, or professional credentials.

8. CONFIDENCE: Rate your confidence in each section based on clarity of information in the job posting (1.0 = very clear, 0.0 = not mentioned/unclear).

CRITICAL RULES:
- Return ONLY valid JSON
- Use null for missing numeric values, empty arrays [] for missing list values, empty objects {{}} for missing object values
- All confidence scores must be between 0.0 and 1.0
- Currency defaults to "USD" if not specified
- Be conservative with confidence scores - only use >0.8 for very clear information
- For specific_technologies_years, only include technologies with explicit year requirements
- For technical_skills and soft_skills, use EXACT NAMES from Lightcast Open Skills Taxonomy whenever possible

Job posting to analyze:

{job_description}
'''

    @staticmethod
    def get_validation_prompt() -> str:
        """
        Validation prompt for reviewing low-confidence extractions.

        Used as a second-pass review for extractions with confidence < 0.6

        Returns: Formatted validation prompt string
        """
        return """
Review this extracted job information and verify its accuracy against the original job posting.
Focus on correcting any obvious errors or missed information.

EXTRACTED DATA:
{extracted_data}

ORIGINAL JOB POSTING:
{job_description}

Return a JSON object with corrections and validation results:

{
  "validation_results": {
    "salary_info": {
      "accurate": true/false,
      "corrections": "specific corrections needed" or null,
      "revised_confidence": 0.0-1.0
    },
    "experience_requirements": {
      "accurate": true/false,
      "corrections": "specific corrections needed" or null,
      "revised_confidence": 0.0-1.0
    },
    "skills": {
      "accurate": true/false,
      "corrections": "specific corrections needed" or null,
      "revised_confidence": 0.0-1.0
    },
    "work_arrangement": {
      "accurate": true/false,
      "corrections": "specific corrections needed" or null,
      "revised_confidence": 0.0-1.0
    },
    "job_classification": {
      "accurate": true/false,
      "corrections": "specific corrections needed" or null,
      "revised_confidence": 0.0-1.0
    }
  },
  "corrected_extraction": {
    // Include only the sections that need corrections
    // Use the same JSON structure as the original extraction
  },
  "overall_confidence": 0.0-1.0,
  "needs_manual_review": true/false,
  "validation_notes": "general observations about the extraction quality"
}

VALIDATION GUIDELINES:
1. Check if extracted salary ranges are realistic and actually mentioned in the job posting
2. Verify that technical skills are explicitly mentioned and match Lightcast taxonomy terms
3. Ensure experience levels match the language used (e.g., "senior" vs "junior")
4. Validate specific_technologies_years contains only technologies with explicit year requirements
5. Confirm work arrangement details are clearly stated, not assumed
6. Check that certifications and timezone_requirements are explicitly stated
7. Validate that job classification aligns with the job title and description
8. Track the percentage of technical skills that match Lightcast taxonomy terms


Return ONLY valid JSON.
"""

    @staticmethod
    def get_quick_classification_prompt() -> str:
        """
        Lightweight prompt for basic job classification only.

        Used for initial job categorization or when full extraction isn't needed.

        Returns: Formatted classification prompt string
        """
        return """
Analyze this job posting and provide basic classification information.

Return a JSON object with this structure:

{
  "job_family": "Engineering"|"Data"|"Product"|"Sales"|"Marketing"|"Operations"|"Design"|"Finance"|"HR"|"Legal"|"Other",
  "experience_level": "Entry"|"Mid"|"Senior"|"Executive"|"Internship",
  "work_type": "Remote"|"Hybrid"|"On-site"|"Flexible",
  "has_salary_info": true/false,
  "primary_keywords": ["keyword1", "keyword2", "keyword3"],
  "confidence": 0.0-1.0
}

GUIDELINES:
- Focus on job title and key responsibilities
- Use conservative confidence scores
- Primary keywords should be the 5 most relevant technical/role terms from Lightcast Open Skills Taxonomy
- If a keyword isn't found in Lightcast taxonomy, use the most similar term that is

Job posting:

{job_description}
"""

    @staticmethod
    def get_salary_focused_prompt() -> str:
        """
        Specialized prompt for extracting salary information only.

        Higher accuracy for salary-specific extraction when needed.

        Returns: Formatted salary extraction prompt string
        """
        return """
Extract ONLY salary and compensation information from this job posting.

Return a JSON object with this structure:

{
  "salary_min": number or null,
  "salary_max": number or null,
  "salary_currency": "USD"|"EUR"|"GBP"|"CAD",
  "salary_period": "annually"|"hourly"|"monthly"|"weekly",
  "salary_type": "base"|"total"|"contract"|"commission",
  "equity_mentioned": true/false,
  "bonus_mentioned": true/false,
  "benefits_mentioned": true/false,
  "compensation_details": "any additional compensation details",
  "confidence": 0.0-1.0
}

SALARY EXTRACTION RULES:
1. Look for explicit numbers: "$80,000", "$80K", "$80-120K", "$45/hour"
2. Handle ranges: "80-120k" → min=80000, max=120000
3. Convert abbreviated forms: "80K" → 80000, "1.2M" → 1200000
4. Identify period: "per year"→annually, "/hr"→hourly, "/month"→monthly
5. Detect equity: "stock options", "RSUs", "equity", "shares"
6. Detect bonuses: "bonus", "performance pay", "commission"
7. Currency defaults to "USD" if not specified

Be conservative with confidence - only use >0.8 for very clear salary information.

Job posting:

{job_description}
"""

    @staticmethod
    def get_skill_taxonomy_prompt() -> str:
        """
        Specialized prompt for categorizing skills using Lightcast taxonomy.

        Used for second-pass categorization of orphaned or uncategorized skills.
        Designed to map skills to Lightcast categories and subcategories.
        Supports bulk processing of multiple skills.

        Returns: Formatted skill taxonomy prompt string
        """
        return '''
You are a Lightcast skill taxonomy expert. Categorize each skill in the provided list using the official Lightcast categories and subcategories.

LIGHTCAST TAXONOMY (category ⇢ subcategory pairs):
{taxonomy_list}

TASK:
1. Read each skill name from the provided list.
2. For each skill:
   - Select the most appropriate category and subcategory from the taxonomy list
   - Provide a confidence score between 0.0-1.0
   - Briefly justify your decision (max 15 words)

Return ONLY valid JSON with exactly this structure:
{{
  "skill_mappings": [
    {{
      "skill_name": "first skill name",
      "lightcast_category": "matched category name or 'Unknown'",
      "lightcast_subcategory": "matched subcategory name or 'Unknown'",
      "match_confidence": 0.0-1.0,
      "notes": "brief explanation"
    }},
    {{
      "skill_name": "second skill name",
      "lightcast_category": "matched category name or 'Unknown'",
      "lightcast_subcategory": "matched subcategory name or 'Unknown'",
      "match_confidence": 0.0-1.0,
      "notes": "brief explanation"
    }},
    // ... one mapping object for each input skill
  ]
}}

CRITICAL RULES:
• Use category & subcategory names exactly as they appear in the taxonomy list
• If no good match, set both to "Unknown" and confidence ≤ 0.4
• Return JSON only – no additional commentary
• Process EVERY skill in the input list
• Maintain exact skill names as provided

Skills to analyze:
{skill_name}
'''


class PromptFormatter:
    """
    Utility class for formatting and validating prompt inputs.
    """

    @staticmethod
    def format_job_description(job_description: str, max_length: int = 8000) -> str:
        """
        Clean and format job description for optimal LLM processing.

        Args:
            job_description: Raw job description text
            max_length: Maximum length to avoid token limits

        Returns:
            Cleaned and truncated job description
        """
        if not job_description or job_description.strip() == "":
            return "No job description provided"

        # Clean the text
        cleaned = job_description.strip()

        # Truncate if too long (leave room for prompt tokens)
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "...[truncated]"

        return cleaned

    @staticmethod
    def validate_extraction_response(response_text: str) -> Dict[str, Any]:
        """
        Validate and parse LLM extraction response.

        Args:
            response_text: Raw response from LLM

        Returns:
            Parsed JSON dict or raises ValueError

        Raises:
            ValueError: If response is not valid JSON
        """
        if not response_text or response_text.strip() == "":
            raise ValueError("Empty response from LLM")

        # RECURSION FIX: Add response size limit to prevent recursion issues
        MAX_RESPONSE_SIZE = 50000  # 50KB limit for response parsing
        if len(response_text) > MAX_RESPONSE_SIZE:
            # Truncate response to prevent recursion in parsing
            response_text = response_text[:MAX_RESPONSE_SIZE]

        # Try direct JSON parsing first
        try:
            return json.loads(response_text.strip())
        except json.JSONDecodeError:
            pass

        # Try to extract JSON from markdown code blocks
        import re

        # Look for JSON in code blocks
        code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if code_block_match:
            try:
                json_content = code_block_match.group(1)
                # RECURSION FIX: Add size limit for code block content
                if len(json_content) > MAX_RESPONSE_SIZE:
                    json_content = json_content[:MAX_RESPONSE_SIZE]
                return json.loads(json_content)
            except json.JSONDecodeError:
                pass

        # Use balanced bracket matching with recursion protection
        json_text = _extract_json_with_balanced_brackets(response_text)
        if json_text:
            try:
                # RECURSION FIX: Add size limit for balanced bracket extraction
                if len(json_text) > MAX_RESPONSE_SIZE:
                    json_text = json_text[:MAX_RESPONSE_SIZE]
                return json.loads(json_text)
            except json.JSONDecodeError:
                pass

        raise ValueError(f"No valid JSON found in response: {response_text[:200]}...")

    @staticmethod
    def calculate_average_confidence(extraction_data: Dict[str, Any]) -> float:
        """
        Calculate average confidence score across all extraction categories.

        Args:
            extraction_data: Parsed extraction response

        Returns:
            Average confidence score (0.0-1.0)
        """
        confidence_scores = []

        # Extract confidence scores from each section
        sections = ['salary_info', 'experience_requirements', 'skills', 'work_arrangement', 'job_classification']

        for section in sections:
            if section in extraction_data and 'confidence' in extraction_data[section]:
                confidence = extraction_data[section]['confidence']
                if isinstance(confidence, (int, float)) and 0 <= confidence <= 1:
                    confidence_scores.append(confidence)

        if not confidence_scores:
            return 0.0

        return sum(confidence_scores) / len(confidence_scores)

    @staticmethod
    def get_low_confidence_fields(extraction_data: Dict[str, Any], threshold: float = 0.6) -> list:
        """
        Identify fields with confidence scores below threshold.

        Args:
            extraction_data: Parsed extraction response
            threshold: Confidence threshold (default 0.6)

        Returns:
            List of field names with low confidence
        """
        low_confidence_fields = []

        sections = ['salary_info', 'experience_requirements', 'skills', 'work_arrangement', 'job_classification']

        for section in sections:
            if section in extraction_data and 'confidence' in extraction_data[section]:
                confidence = extraction_data[section]['confidence']
                if isinstance(confidence, (int, float)) and confidence < threshold:
                    low_confidence_fields.append(section)

        return low_confidence_fields


def _extract_json_with_balanced_brackets(text: str) -> Optional[str]:
    """
    Extract JSON object using balanced bracket matching to avoid regex recursion issues.

    Args:
        text: Text containing JSON object

    Returns:
        Extracted JSON string or None if no valid JSON found
    """
    # RECURSION FIX: Add safety limits to prevent recursion and infinite loops
    MAX_TEXT_SIZE = 100000  # 100KB limit
    MAX_ITERATIONS = 10000   # Maximum iterations to prevent infinite loops
    MAX_BRACKET_DEPTH = 50   # Maximum nesting depth

    # Truncate input if too large
    if len(text) > MAX_TEXT_SIZE:
        text = text[:MAX_TEXT_SIZE]

    # Find the first opening brace
    start_idx = text.find('{')
    if start_idx == -1:
        return None

    # Track bracket depth for balanced matching
    bracket_count = 0
    in_string = False
    escape_next = False
    iteration_count = 0

    for i, char in enumerate(text[start_idx:], start_idx):
        # RECURSION FIX: Prevent infinite loops with iteration limit
        iteration_count += 1
        if iteration_count > MAX_ITERATIONS:
            return None

        if escape_next:
            escape_next = False
            continue

        if char == '\\':
            escape_next = True
            continue

        if char == '"' and not escape_next:
            in_string = not in_string
            continue

        if not in_string:
            if char == '{':
                bracket_count += 1
                # RECURSION FIX: Prevent excessive nesting
                if bracket_count > MAX_BRACKET_DEPTH:
                    return None
            elif char == '}':
                bracket_count -= 1

                # Found matching closing bracket
                if bracket_count == 0:
                    extracted = text[start_idx:i+1]
                    # RECURSION FIX: Validate extracted size
                    if len(extracted) > MAX_TEXT_SIZE:
                        return extracted[:MAX_TEXT_SIZE]
                    return extracted

    # No balanced JSON found
    return None


# Example usage and testing
if __name__ == "__main__":
    # Example job description for testing
    sample_job_description = """
    Senior Software Engineer - Backend

    We are looking for a Senior Software Engineer to join our growing engineering team.

    Responsibilities:
    - Design and build scalable backend services using Python and Go
    - Work with PostgreSQL and Redis for data storage
    - Deploy applications on AWS using Docker and Kubernetes
    - Collaborate with frontend engineers and product managers

    Requirements:
    - 5+ years of software engineering experience
    - Strong experience with Python, Go, or similar languages
    - Experience with SQL databases and cloud platforms
    - Bachelor's degree in Computer Science or related field
    - Excellent communication and problem-solving skills

    Compensation:
    - Salary range: $140,000 - $180,000 annually
    - Equity package included
    - Comprehensive benefits

    This is a hybrid role - 3 days in office (San Francisco), 2 days remote.
    """

    # Test prompt formatting
    prompts = JobExtractionPrompts()
    formatter = PromptFormatter()

    # Get the comprehensive extraction prompt
    extraction_prompt = prompts.get_comprehensive_extraction_prompt()
    # Format the job description first
    formatted_job_desc = formatter.format_job_description(sample_job_description)

    # Then format the prompt
    formatted_prompt = extraction_prompt.format(job_description=formatted_job_desc)

    print("=== COMPREHENSIVE EXTRACTION PROMPT PREVIEW ===")
    print(formatted_prompt[:1000] + "...[truncated]")
    print(f"\nPrompt length: {len(formatted_prompt)} characters")
    print("\n=== PROMPT TEMPLATES READY FOR GEMINI INTEGRATION ===")