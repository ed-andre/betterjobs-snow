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

    # ========== BUG REPRODUCTION TESTS ==========
    print("\n" + "=" * 60)
    print("BUG REPRODUCTION TESTS - BUG-010")
    print("=" * 60)

    # Test 1: Line break concatenation issue
    print("\n1. LINE BREAK CONCATENATION BUG:")
    line_break_text = """React etc.
N-tier application architecture
Strong background in multiple disciplines with an engineering mindset
In-depth knowledge of one of the following RDBMS: Oracle or MS SQL Server
Experience working in an agile environment"""

    clean_line_breaks = clean_job_description(line_break_text)
    print(f"  Input (with natural line breaks):")
    print(f"    {repr(line_break_text)}")
    print(f"  Output (should preserve word boundaries):")
    print(f"    {repr(clean_line_breaks)}")
    print(f"  Issue: Words concatenated? {('React etc.N-tier' in clean_line_breaks or 'architectureStrong' in clean_line_breaks)}")
    print()

    # Test 2: HTML list structure destruction
    print("2. HTML LIST STRUCTURE DESTRUCTION BUG:")
    html_list_text = """<ul>
<li>Perform qualitative and quantitative analysis</li>
<li>Work with editorial tools to classify web pages</li>
<li>Collaborate with data scientists</li>
</ul>"""

    clean_html_list = clean_job_description(html_list_text)
    print(f"  Input (structured HTML list):")
    print(f"    {repr(html_list_text)}")
    print(f"  Output (should preserve logical separation):")
    print(f"    {repr(clean_html_list)}")
    print(f"  Issue: Words concatenated? {'analysisWork' in clean_html_list}")
    print()

    # Test 3: Character encoding issues
    print("3. CHARACTER ENCODING ISSUES:")
    encoding_text = "We are looking for developers with good experience in data analytics."
    # Simulate common encoding corruption
    corrupted_text = "We are looking for developers' with good experience in data analytics."

    clean_encoding = clean_job_description(corrupted_text)
    print(f"  Input (corrupted encoding):")
    print(f"    {repr(corrupted_text)}")
    print(f"  Output (should fix encoding):")
    print(f"    {repr(clean_encoding)}")
    print(f"  Issue: Still corrupted? {'â€' in clean_encoding}")
    print()

    # Test 4: Complex real-world example
    print("4. COMPLEX REAL-WORLD EXAMPLE:")
    complex_text = """<p>We are seeking a Senior Software Engineer with:</p>
<ul>
<li>5+ years of experience with JavaScript
React, Node.js</li>
<li>Strong SQL Server
Experience with databases</li>
<li>Bachelor's degree in Computer Science
OR equivalent experience</li>
</ul>
<p>This role offers competitive salary and benefits.</p>

We are an equal opportunity employer."""

    clean_complex = clean_job_description(complex_text)
    print(f"  Input (complex HTML with line breaks):")
    print(f"    {repr(complex_text)}")
    print(f"  Output (should be readable):")
    print(f"    {repr(clean_complex)}")
    print(f"  Issues detected:")
    print(f"    - JavaScript+React concatenation: {'JavaScriptReact' in clean_complex}")
    print(f"    - SQL Server+Experience concatenation: {'ServerExperience' in clean_complex}")
    print(f"    - Degree+OR concatenation: {'ScienceOR' in clean_complex}")
    print()

    print("=" * 60)
    print("END BUG REPRODUCTION TESTS")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    test_text_cleaning()