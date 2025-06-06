"""
Simple test script for text cleaning functions.
"""

from text_cleaning import (
    clean_html_tags,
    normalize_whitespace,
    standardize_job_title,
    standardize_location,
    validate_url,
    clean_job_description,
    clean_company_name,
    clean_text_fields
)

def test_text_cleaning():
    """Test all text cleaning functions with sample data."""

    print("Testing Text Cleaning Functions")
    print("=" * 50)

    # Test HTML cleaning
    html_text = "<p>Software <strong>Engineer</strong> - &amp; Development</p>"
    cleaned_html = clean_html_tags(html_text)
    print(f"HTML cleaning:")
    print(f"  Input:  {html_text}")
    print(f"  Output: {cleaned_html}")
    print()

    # Test job title standardization
    raw_title = "  Sr. SW Dev - AI/ML Engineer  "
    clean_title = standardize_job_title(raw_title)
    print(f"Job title standardization:")
    print(f"  Input:  '{raw_title}'")
    print(f"  Output: '{clean_title}'")
    print()

    # Test location standardization
    raw_location = "San Francisco, CA, United States"
    location_info = standardize_location(raw_location)
    print(f"Location standardization:")
    print(f"  Input:  {raw_location}")
    print(f"  Output: {location_info}")
    print()

    # Test remote location
    remote_location = "Remote - Work from anywhere"
    remote_info = standardize_location(remote_location)
    print(f"Remote location:")
    print(f"  Input:  {remote_location}")
    print(f"  Output: {remote_info}")
    print()

    # Test URL validation
    test_url = "greenhouse.io/careers"
    url_info = validate_url(test_url)
    print(f"URL validation:")
    print(f"  Input:  {test_url}")
    print(f"  Output: {url_info}")
    print()

    # Test company name cleaning
    company_name = "  Acme Corp Inc.  "
    clean_company = clean_company_name(company_name)
    print(f"Company name cleaning:")
    print(f"  Input:  '{company_name}'")
    print(f"  Output: '{clean_company}'")
    print()

    # Test job description cleaning
    job_desc = """<p>We are looking for a <strong>Software Engineer</strong>.</p>

    <ul>
    <li>5+ years experience</li>
    <li>Python skills</li>
    </ul>

    We are an equal opportunity employer."""
    clean_desc = clean_job_description(job_desc)
    print(f"Job description cleaning:")
    print(f"  Input:  {job_desc}")
    print(f"  Output: {clean_desc}")
    print()

    # Test batch processing
    sample_data = {
        'job_title': 'Sr. Software Eng.',
        'job_description': '<p>Looking for a Python developer</p>',
        'company_name': 'Tech Corp Inc.',
        'location': 'New York, NY',
        'job_url': 'company.com/jobs/123'
    }

    cleaned_data = clean_text_fields(sample_data)
    print(f"Batch cleaning:")
    print(f"  Input:  {sample_data}")
    print(f"  Output: {cleaned_data}")

if __name__ == "__main__":
    test_text_cleaning()