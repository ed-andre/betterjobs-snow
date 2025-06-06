"""
Text cleaning and standardization functions for STAGE layer transformations.

This module provides reusable functions for cleaning and standardizing text fields
from job postings across different ATS platforms.
"""

import re
import html
from typing import Optional, Dict, List
from urllib.parse import urlparse
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean_html_tags(text: Optional[str]) -> str:
    """
    Remove HTML tags and decode HTML entities from text.

    Args:
        text: Raw text that may contain HTML tags and entities

    Returns:
        Cleaned text with HTML tags removed and entities decoded
    """
    if not text or not isinstance(text, str):
        return ""

    # Decode HTML entities first (e.g., &amp; -> &, &lt; -> <)
    text = html.unescape(text)

    # Remove HTML tags using regex
    # This pattern matches opening and closing tags, including self-closing tags
    clean_text = re.sub(r'<[^>]+>', '', text)

    # Remove HTML comments
    clean_text = re.sub(r'<!--.*?-->', '', clean_text, flags=re.DOTALL)

    # Handle common HTML artifacts
    clean_text = clean_text.replace('&nbsp;', ' ')
    clean_text = clean_text.replace('\xa0', ' ')  # Non-breaking space

    return clean_text.strip()


def normalize_whitespace(text: Optional[str]) -> str:
    """
    Normalize whitespace in text by removing extra spaces, tabs, and newlines.

    Args:
        text: Text to normalize

    Returns:
        Text with normalized whitespace
    """
    if not text or not isinstance(text, str):
        return ""

    # Replace multiple whitespace characters (spaces, tabs, newlines) with single space
    normalized = re.sub(r'\s+', ' ', text.strip())

    return normalized


def standardize_job_title(title: Optional[str]) -> str:
    """
    Standardize job title formatting.

    Args:
        title: Raw job title

    Returns:
        Standardized job title
    """
    if not title or not isinstance(title, str):
        return ""

    # First clean HTML and normalize whitespace
    clean_title = normalize_whitespace(clean_html_tags(title))

    # Remove common unwanted characters and patterns
    clean_title = re.sub(r'[^\w\s\-\(\)\/\&\+\.]', '', clean_title)

    # Standardize common abbreviations and formats
    standardizations = {
        r'\bSr\.?\b': 'Senior',
        r'\bJr\.?\b': 'Junior',
        r'\bMgr\.?\b': 'Manager',
        r'\bDir\.?\b': 'Director',
        r'\bVP\b': 'Vice President',
        r'\bCTO\b': 'Chief Technology Officer',
        r'\bCEO\b': 'Chief Executive Officer',
        r'\bCFO\b': 'Chief Financial Officer',
        r'\bCOO\b': 'Chief Operating Officer',
        r'\bEng\.?\b': 'Engineer',
        r'\bDev\.?\b': 'Developer',
        r'\bSW\b': 'Software',
        r'\bHW\b': 'Hardware',
        r'\bQA\b': 'Quality Assurance',
        r'\bUI/UX\b': 'UI/UX',
        r'\bFT\b': 'Full-Time',
        r'\bPT\b': 'Part-Time'
    }

    for pattern, replacement in standardizations.items():
        clean_title = re.sub(pattern, replacement, clean_title, flags=re.IGNORECASE)

    # Normalize case - Title Case for most words, but preserve acronyms
    words = clean_title.split()
    normalized_words = []

    for word in words:
        # Keep acronyms (all caps words) as is
        if word.isupper() and len(word) > 1:
            normalized_words.append(word)
        # Keep known tech terms in specific case
        elif word.lower() in ['api', 'ui', 'ux', 'ai', 'ml', 'devops', 'saas', 'paas', 'iaas']:
            normalized_words.append(word.upper())
        else:
            normalized_words.append(word.title())

    return ' '.join(normalized_words)


def standardize_location(location: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Parse and standardize location information.

    Args:
        location: Raw location string

    Returns:
        Dictionary with standardized location components:
        {
            'standardized': 'Full standardized location string',
            'city': 'City name',
            'state': 'State code or name',
            'country': 'Country name',
            'is_remote': 'Boolean indicating if remote work'
        }
    """
    if not location or not isinstance(location, str):
        return {
            'standardized': '',
            'city': None,
            'state': None,
            'country': None,
            'is_remote': False
        }

    # Clean and normalize
    clean_loc = normalize_whitespace(clean_html_tags(location))

    # Check for remote work indicators
    remote_patterns = [
        r'\bremote\b', r'\bwork from home\b', r'\bwfh\b',
        r'\btelecommute\b', r'\bvirtual\b', r'\bdistributed\b',
        r'\banywhere\b', r'\bflexible location\b'
    ]

    is_remote = any(re.search(pattern, clean_loc, re.IGNORECASE) for pattern in remote_patterns)

    if is_remote:
        return {
            'standardized': 'Remote',
            'city': None,
            'state': None,
            'country': None,
            'is_remote': True
        }

    # Common US state abbreviations and names
    us_states = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
    }

    # Parse location components
    city = None
    state = None
    country = None

    # Common patterns for US locations
    # "City, State" or "City, State Code"
    us_pattern = r'^(.+?),\s*([A-Z]{2}|[A-Za-z\s]+)(?:,\s*(United States|USA|US))?'
    us_match = re.search(us_pattern, clean_loc.strip())

    if us_match:
        city = us_match.group(1).strip()
        state_raw = us_match.group(2).strip()
        country = 'United States'

        # Standardize state
        if state_raw.upper() in us_states:
            state = state_raw.upper()
        else:
            # Try to find state by full name
            for code, name in us_states.items():
                if name.lower() == state_raw.lower():
                    state = code
                    break
            if not state:
                state = state_raw  # Keep as is if not found
    else:
        # Try international or other formats
        parts = [part.strip() for part in clean_loc.split(',')]
        if len(parts) >= 2:
            city = parts[0]
            if len(parts) >= 3:
                state = parts[1]
                country = parts[2]
            else:
                country = parts[1]
        else:
            city = clean_loc

    # Build standardized string
    standardized_parts = []
    if city:
        standardized_parts.append(city)
    if state:
        standardized_parts.append(state)
    if country:
        standardized_parts.append(country)

    standardized = ', '.join(standardized_parts) if standardized_parts else clean_loc

    return {
        'standardized': standardized,
        'city': city,
        'state': state,
        'country': country,
        'is_remote': False
    }


def validate_url(url: Optional[str]) -> Dict[str, any]:
    """
    Validate and normalize URLs.

    Args:
        url: URL string to validate

    Returns:
        Dictionary with validation results:
        {
            'is_valid': Boolean,
            'normalized_url': Cleaned URL,
            'domain': Domain name,
            'scheme': URL scheme (http/https)
        }
    """
    if not url or not isinstance(url, str):
        return {
            'is_valid': False,
            'normalized_url': '',
            'domain': None,
            'scheme': None
        }

    # Clean the URL
    clean_url = url.strip()

    # Add scheme if missing
    if not clean_url.startswith(('http://', 'https://')):
        clean_url = 'https://' + clean_url

    try:
        parsed = urlparse(clean_url)

        # Basic validation
        if not parsed.netloc:
            return {
                'is_valid': False,
                'normalized_url': clean_url,
                'domain': None,
                'scheme': None
            }

        return {
            'is_valid': True,
            'normalized_url': clean_url,
            'domain': parsed.netloc.lower(),
            'scheme': parsed.scheme
        }

    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        return {
            'is_valid': False,
            'normalized_url': clean_url,
            'domain': None,
            'scheme': None
        }


def clean_job_description(description: Optional[str]) -> str:
    """
    Comprehensive cleaning of job descriptions.

    Args:
        description: Raw job description text

    Returns:
        Cleaned and standardized job description
    """
    if not description or not isinstance(description, str):
        return ""

    # Remove HTML tags and normalize whitespace
    clean_desc = normalize_whitespace(clean_html_tags(description))

    # Remove common boilerplate patterns
    boilerplate_patterns = [
        r'We are an equal opportunity employer.*',
        r'Equal opportunity employer.*',
        r'EOE.*',
        r'This employer participates in E-Verify.*',
        r'All qualified applicants will receive consideration.*',
        r'To perform this job successfully.*',
        r'The above statements are intended to describe.*'
    ]

    for pattern in boilerplate_patterns:
        clean_desc = re.sub(pattern, '', clean_desc, flags=re.IGNORECASE | re.DOTALL)

    # Remove excessive bullet points and formatting artifacts
    clean_desc = re.sub(r'[•·▪▫‣⁃]\s*', '• ', clean_desc)  # Standardize bullet points
    clean_desc = re.sub(r'\n\s*\n\s*\n+', '\n\n', clean_desc)  # Remove excessive line breaks

    # Final whitespace normalization
    clean_desc = normalize_whitespace(clean_desc)

    return clean_desc


def clean_company_name(company_name: Optional[str]) -> str:
    """
    Standardize company name formatting.

    Args:
        company_name: Raw company name

    Returns:
        Cleaned and standardized company name
    """
    if not company_name or not isinstance(company_name, str):
        return ""

    # Clean HTML and normalize whitespace
    clean_name = normalize_whitespace(clean_html_tags(company_name))

    # Remove common legal suffixes for consistency (but preserve them)
    # This helps with matching while keeping the full legal name
    legal_suffixes = [
        r'\s+Inc\.?$', r'\s+LLC\.?$', r'\s+Corp\.?$', r'\s+Corporation$',
        r'\s+Ltd\.?$', r'\s+Limited$', r'\s+Co\.?$', r'\s+Company$',
        r'\s+LP$', r'\s+LLP$', r'\s+&\s*Co\.?$'
    ]

    # Don't remove, but standardize the format
    standardizations = {
        r'\s+Inc\.?$': ' Inc.',
        r'\s+LLC\.?$': ' LLC',
        r'\s+Corp\.?$': ' Corp.',
        r'\s+Corporation$': ' Corporation',
        r'\s+Ltd\.?$': ' Ltd.',
        r'\s+Limited$': ' Limited',
        r'\s+Co\.?$': ' Co.',
        r'\s+Company$': ' Company'
    }

    for pattern, replacement in standardizations.items():
        clean_name = re.sub(pattern, replacement, clean_name, flags=re.IGNORECASE)

    return clean_name.strip()


# Utility function for batch processing
def clean_text_fields_dict(data: Dict[str, any]) -> Dict[str, any]:
    """
    Apply text cleaning to all relevant fields in a data dictionary.

    Args:
        data: Dictionary containing job data fields

    Returns:
        Dictionary with cleaned text fields
    """
    cleaned_data = data.copy()

    # Apply cleaning functions to relevant fields
    if 'job_title' in cleaned_data:
        cleaned_data['job_title_clean'] = standardize_job_title(cleaned_data['job_title'])

    if 'job_description' in cleaned_data:
        cleaned_data['job_description_clean'] = clean_job_description(cleaned_data['job_description'])

    if 'company_name' in cleaned_data:
        cleaned_data['company_name_clean'] = clean_company_name(cleaned_data['company_name'])

    if 'location' in cleaned_data:
        location_info = standardize_location(cleaned_data['location'])
        cleaned_data['location_standardized'] = location_info['standardized']
        cleaned_data['location_city'] = location_info['city']
        cleaned_data['location_state'] = location_info['state']
        cleaned_data['location_country'] = location_info['country']
        cleaned_data['is_remote_location'] = location_info['is_remote']

    if 'job_url' in cleaned_data:
        url_info = validate_url(cleaned_data['job_url'])
        cleaned_data['job_url_valid'] = url_info['is_valid']
        cleaned_data['job_url_normalized'] = url_info['normalized_url']
        cleaned_data['job_url_domain'] = url_info['domain']

    return cleaned_data


def clean_text_fields_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply text cleaning to all relevant fields in a pandas DataFrame.

    Args:
        df: DataFrame containing job data fields

    Returns:
        DataFrame with cleaned text fields added
    """
    cleaned_df = df.copy()

    # Apply cleaning functions to relevant fields
    if 'job_title' in cleaned_df.columns:
        cleaned_df['job_title_clean'] = cleaned_df['job_title'].apply(
            lambda x: standardize_job_title(x) if pd.notnull(x) else ""
        )

    if 'job_description' in cleaned_df.columns:
        cleaned_df['job_description_clean'] = cleaned_df['job_description'].apply(
            lambda x: clean_job_description(x) if pd.notnull(x) else ""
        )

    if 'company_name' in cleaned_df.columns:
        cleaned_df['company_name_clean'] = cleaned_df['company_name'].apply(
            lambda x: clean_company_name(x) if pd.notnull(x) else ""
        )

    if 'location' in cleaned_df.columns:
        location_results = cleaned_df['location'].apply(
            lambda x: standardize_location(x) if pd.notnull(x) else {
                'standardized': '', 'city': None, 'state': None,
                'country': None, 'is_remote': False
            }
        )

        cleaned_df['location_standardized'] = location_results.apply(lambda x: x['standardized'])
        cleaned_df['location_city'] = location_results.apply(lambda x: x['city'])
        cleaned_df['location_state'] = location_results.apply(lambda x: x['state'])
        cleaned_df['location_country'] = location_results.apply(lambda x: x['country'])
        cleaned_df['is_remote_location'] = location_results.apply(lambda x: x['is_remote'])

    if 'job_url' in cleaned_df.columns:
        url_results = cleaned_df['job_url'].apply(
            lambda x: validate_url(x) if pd.notnull(x) else {
                'is_valid': False, 'normalized_url': '', 'domain': None, 'scheme': None
            }
        )

        cleaned_df['job_url_valid'] = url_results.apply(lambda x: x['is_valid'])
        cleaned_df['job_url_normalized'] = url_results.apply(lambda x: x['normalized_url'])
        cleaned_df['job_url_domain'] = url_results.apply(lambda x: x['domain'])

    return cleaned_df


def clean_text_fields(data):
    """
    Apply text cleaning to all relevant fields in data (dict or DataFrame).

    Args:
        data: Dictionary or DataFrame containing job data fields

    Returns:
        Cleaned data in the same format as input
    """
    if isinstance(data, pd.DataFrame):
        return clean_text_fields_dataframe(data)
    elif isinstance(data, dict):
        return clean_text_fields_dict(data)
    else:
        raise ValueError(f"Unsupported data type: {type(data)}. Expected dict or DataFrame.")