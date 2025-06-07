import json
import logging
import datetime
import requests
import time
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs
from datetime import timezone
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class GreenhouseScraper(BaseScraper):
    """
    Scraper for Greenhouse-based career sites.

    Extracts job listings from Greenhouse job boards using embedded JSON data.
    Supports both direct Greenhouse boards and embedded job boards.
    """

    def __init__(self, career_url: str, dagster_log=None, ats_url=None, cutoff_date=None, **kwargs):
        """
        Initialize the Greenhouse scraper.

        Args:
            career_url: Base URL of the Greenhouse career site
            dagster_log: Optional Dagster logger for integration
            ats_url: Optional direct ATS URL (https://boards.greenhouse.io/[tenant])
            cutoff_date: Optional datetime for job freshness filter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(**kwargs)

        self.dagster_log = dagster_log
        self.original_url = career_url

        self.log_message("info", f"Initializing GreenhouseScraper with career_url: {career_url}")

        # Set cutoff date (default: 14 days ago) and ensure it's timezone-aware
        if cutoff_date:
            # If cutoff_date is already timezone-aware, use it as is
            if cutoff_date.tzinfo is not None:
                self.cutoff_date = cutoff_date
            else:
                # Otherwise, make it timezone-aware with UTC
                self.cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
        else:
            # Default cutoff date - 14 days ago with UTC timezone
            self.cutoff_date = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=14)

        self.log_message("info", f"Setting cutoff date for jobs to {self.cutoff_date.strftime('%Y-%m-%d')}")

        # Use direct ATS URL if provided and it's a Greenhouse URL
        if ats_url and 'greenhouse.io' in ats_url:
            self.log_message("info", f"Using provided ATS URL: {ats_url}")
            parsed = urlparse(ats_url)
            domain = parsed.netloc
            path = parsed.path.strip('/')
            query = parsed.query

            # Check for embedded job board pattern more specifically
            self.is_embedded = (
                'embed' in path or
                'embed' in domain or
                'job_board?for=' in ats_url or
                'embed/job_board' in ats_url
            )

            if domain.endswith('.greenhouse.io'):
                if self.is_embedded:
                    # Handle embedded job board URL format
                    self.tenant = self._extract_tenant_from_embed_url(ats_url)
                    self.log_message("info", f"Detected embedded Greenhouse board with tenant: {self.tenant}")

                    # Fix: Use the correct embedded endpoint format
                    if 'boards.greenhouse.io' in domain:
                        self.career_url = ats_url  # Use the provided embedded URL directly
                    else:
                        self.career_url = f"https://boards.greenhouse.io/embed/job_board?for={self.tenant}"

                    self.is_direct_api = False
                else:
                    # Direct Greenhouse job board
                    self.tenant = path
                    self.career_url = f"https://job-boards.greenhouse.io/{self.tenant}"
                    self.is_direct_api = True

                self.log_message("info", f"Set Greenhouse career URL: {self.career_url}")
                self._init_session()
                return

        # Process URL normally if no direct ATS URL provided or ATS URL is not a Greenhouse URL
        self.career_url = self.normalize_url(career_url)
        parsed_url = urlparse(self.career_url)
        domain = parsed_url.netloc
        path = parsed_url.path.strip('/')

        # Determine if this is an embedded job board
        self.is_embedded = 'embed' in path or 'embed' in domain

        if 'greenhouse.io' in domain:
            if self.is_embedded:
                # Handle embedded job board
                self.tenant = self._extract_tenant_from_embed_url(self.career_url)
                self.log_message("info", f"Detected embedded Greenhouse board with tenant: {self.tenant}")
                self.career_url = f"https://boards.greenhouse.io/embed/job_board?for={self.tenant}"
                self.is_direct_api = False
            else:
                # Direct Greenhouse job board
                self.tenant = path
                self.career_url = f"https://job-boards.greenhouse.io/{self.tenant}"
                self.is_direct_api = True
        else:
            self.log_message("warning", f"Could not determine Greenhouse tenant for {self.career_url} - scraper may not work")
            self.is_direct_api = False
            self.tenant = None

        self._init_session()

    def _init_session(self):
        """Initialize session with Greenhouse-specific headers."""
        self.session = requests.Session()

        # Set headers that work well with Greenhouse
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })

    def _extract_tenant_from_embed_url(self, url: str) -> str:
        """Extract tenant name from an embedded Greenhouse job board URL."""
        # Try to extract from "for" parameter first
        if "for=" in url:
            tenant = url.split("for=")[1].split("&")[0]
            return tenant

        # Parse the URL to get query parameters
        parsed = urlparse(url)
        if parsed.query:
            query_params = parse_qs(parsed.query)
            if 'for' in query_params:
                tenant = query_params['for'][0]
                return tenant

        # Otherwise, try to extract from path
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) > 0:
            tenant = path_parts[-1]
            return tenant

        self.log_message("warning", f"Could not extract tenant from URL: {url}")
        return None

    def _extract_json_from_response(self, response_text: str) -> Optional[Dict]:
        """
        Extract embedded JSON data from the HTML response.

        This looks for the JSON data embedded in the __remixContext variable or other
        specific patterns in the HTML content.
        """
        # Try to extract from __remixContext
        remix_context_pattern = r'window\.__remixContext\s*=\s*({.*?});'
        matches = re.search(remix_context_pattern, response_text, re.DOTALL)

        if matches:
            try:
                json_str = matches.group(1)
                json_data = json.loads(json_str)
                self.log_message("info", "Successfully extracted JSON from __remixContext")

                # Check for jobPost in the loaderData
                if "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]
                    for key, value in loader_data.items():
                        if "jobPost" in str(value):
                            self.log_message("info", f"Found jobPost data in key: {key}")

                return json_data
            except (json.JSONDecodeError, IndexError) as e:
                self.log_message("warning", f"Failed to parse JSON from __remixContext: {str(e)}")

        # Alternative pattern for job board data
        job_data_pattern = r'routes/\$url_token.*?"data":\s*({.*?])}'
        matches = re.search(job_data_pattern, response_text, re.DOTALL)

        if matches:
            try:
                # Need to process this differently since it's not a complete JSON object
                matched_text = matches.group(0)
                # Find the JSON portion that starts with the job_posts field
                job_posts_match = re.search(r'"jobPosts":\s*({.*?})', matched_text, re.DOTALL)
                if job_posts_match:
                    json_str = job_posts_match.group(1)
                    # Clean up the JSON string to make it valid
                    json_str = json_str.replace('undefined', 'null')
                    json_data = json.loads(json_str)
                    self.log_message("info", "Successfully extracted JSON from jobPosts field")
                    return {"jobPosts": json_data}
            except (json.JSONDecodeError, IndexError) as e:
                self.log_message("warning", f"Failed to parse JSON from job data: {str(e)}")

        # Try to find any embedded JSON with job data - improved pattern to catch more variants
        try:
            # Look for any JSON object containing jobPost - use looser pattern to match more variants
            job_post_pattern = r'routes/\$url_token_.*?job.*?_id":\s*({.*?"jobPost".*?})'
            job_post_match = re.search(job_post_pattern, response_text, re.DOTALL)

            if job_post_match:
                # Extract just the matched JSON text
                job_post_text = job_post_match.group(1)
                # Clean up the matched text to make it valid JSON
                # Add brackets if they're not already there
                if not job_post_text.startswith('{'):
                    job_post_text = '{' + job_post_text
                if not job_post_text.endswith('}'):
                    job_post_text = job_post_text + '}'

                job_post_text = job_post_text.replace('undefined', 'null')

                # Try to extract the key name from the original text
                key_match = re.search(r'(routes/\$url_token_.*?job.*?_id)', response_text, re.DOTALL)
                key_name = "routes/$url_token_.jobs_.$job_post_id"  # Default fallback
                if key_match:
                    key_name = key_match.group(1)

                # Build a complete JSON structure
                constructed_json = {
                    "state": {
                        "loaderData": {
                            key_name: json.loads(job_post_text)
                        }
                    }
                }
                self.log_message("info", f"Successfully constructed JSON from jobPost pattern with key: {key_name}")
                return constructed_json
        except (json.JSONDecodeError, IndexError) as e:
            self.log_message("warning", f"Failed to parse JSON from jobPost pattern: {str(e)}")

        # Try with a broader pattern for Greenhouse boards
        try:
            # Look for direct jobPost data
            direct_pattern = r'jobPost":\s*({.*?"content".*?})'
            direct_match = re.search(direct_pattern, response_text, re.DOTALL)

            if direct_match:
                job_data = direct_match.group(1)
                job_data = job_data.replace('undefined', 'null')

                # Create a simple structure
                constructed_json = {
                    "state": {
                        "loaderData": {
                            "routes/$url_token_.jobs_.$job_post_id": {
                                "jobPost": json.loads(job_data)
                            }
                        }
                    }
                }
                self.log_message("info", "Successfully constructed JSON from direct jobPost content")
                return constructed_json
        except (json.JSONDecodeError, IndexError) as e:
            self.log_message("warning", f"Failed to parse direct jobPost data: {str(e)}")

        # New pattern: Try to extract the raw content directly
        try:
            # This pattern looks for "content":"..." in the HTML - handles both escaped HTML and regular text
            # Use a more flexible pattern that can catch both escaped and regular HTML content
            content_pattern = r'"content":\s*"((?:\\u003c|\\n|\\"|[^"]).*?)(?:"[,}])'
            content_match = re.search(content_pattern, response_text, re.DOTALL)
            if content_match:
                content_text = content_match.group(1)
                # Unescape unicode escape sequences
                content_text = bytes(content_text, "utf-8").decode("unicode_escape")

                # Create a simple structure with just the content
                constructed_json = {
                    "state": {
                        "loaderData": {
                            "routes/$url_token_.jobs_.$job_post_id": {
                                "jobPost": {
                                    "content": content_text
                                }
                            }
                        }
                    }
                }
                self.log_message("info", "Successfully extracted direct HTML content")
                return constructed_json
        except Exception as e:
            self.log_message("warning", f"Failed to extract direct content: {str(e)}")

        # Try a more aggressive approach to find any content containing HTML-like structure
        try:
            # Look for anything that seems like HTML content
            html_pattern = r'"([^"]*<div[^>]*>[^"]*<\/div>[^"]*)"'
            html_match = re.search(html_pattern, response_text, re.DOTALL)
            if html_match:
                html_content = html_match.group(1)
                # Unescape if it contains escaped HTML
                if "\\u003c" in html_content:
                    html_content = bytes(html_content, "utf-8").decode("unicode_escape")

                constructed_json = {
                    "state": {
                        "loaderData": {
                            "routes/$url_token_.jobs_.$job_post_id": {
                                "jobPost": {
                                    "content": html_content
                                }
                            }
                        }
                    }
                }
                self.log_message("info", "Successfully extracted HTML content using aggressive pattern")
                return constructed_json
        except Exception as e:
            self.log_message("warning", f"Failed to extract HTML content with aggressive pattern: {str(e)}")

        # Last resort: Try to extract content directly from HTML structure
        try:
            # Look for job description in the HTML - typically inside a div with class="job__description"
            job_desc_pattern = r'<div\s+class="job__description.*?>(.*?)</div>\s*</div>'
            job_desc_match = re.search(job_desc_pattern, response_text, re.DOTALL)
            if job_desc_match:
                content_html = job_desc_match.group(1)
                constructed_json = {
                    "state": {
                        "loaderData": {
                            "routes/$url_token_.jobs_.$job_post_id": {
                                "jobPost": {
                                    "content": content_html
                                }
                            }
                        }
                    }
                }
                self.log_message("info", "Successfully extracted job description from HTML structure")
                return constructed_json

            # Try additional HTML selectors
            for selector_pattern in [
                r'<div\s+class="s-themed-content-body.*?>(.*?)</div>',
                r'<div\s+id="content".*?>(.*?)</div>',
                r'<div\s+class="content".*?>(.*?)</div>'
            ]:
                selector_match = re.search(selector_pattern, response_text, re.DOTALL)
                if selector_match:
                    content_html = selector_match.group(1)
                    constructed_json = {
                        "state": {
                            "loaderData": {
                                "routes/$url_token_.jobs_.$job_post_id": {
                                    "jobPost": {
                                        "content": content_html
                                    }
                                }
                            }
                        }
                    }
                    self.log_message("info", f"Successfully extracted content from HTML selector: {selector_pattern}")
                    return constructed_json
        except Exception as e:
            self.log_message("warning", f"Failed to extract description from HTML: {str(e)}")

        self.log_message("warning", "All JSON extraction patterns failed")
        return None

    def make_greenhouse_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make a request to Greenhouse with specialized handling and retries.
        """
        try:
            # Apply rate limiting
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)

            kwargs.setdefault("timeout", self.timeout)

            # Set required headers for Greenhouse
            if "headers" not in kwargs:
                kwargs["headers"] = {}

            greenhouse_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.career_url
            }
            kwargs["headers"].update(greenhouse_headers)

            # Implement retry logic
            for attempt in range(self.max_retries):
                try:
                    self.last_request_time = time.time()
                    response = self.session.request(method, url, **kwargs)

                    self.log_message("info", f"Request to {url} returned status {response.status_code}")
                    if response.status_code != 200:
                        self.log_message("warning", f"Non-200 response: {response.status_code} - {response.text[:200]}")

                    response.raise_for_status()
                    return response

                except requests.RequestException as e:
                    self.log_message("warning", f"Request failed (attempt {attempt+1}/{self.max_retries}): {str(e)}")

                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise

        except Exception as e:
            self.log_message("error", f"Error making Greenhouse request to {url}: {str(e)}")
            raise

        return None

    def search_jobs(self, keyword: str = None, location: Optional[str] = None, max_age_days: int = None) -> List[Dict]:
        """
        Search for jobs on a Greenhouse career site.

        Args:
            keyword: Optional search term filter
            location: Optional location filter
            max_age_days: Optional max age of job postings in days

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        self.log_message("info", f"Starting job search - keyword: {keyword}, location: {location}")

        if not self.is_direct_api and not self.is_embedded:
            self.log_message("warning", "This instance is not properly configured for Greenhouse scraping")
            return []

        # Direct API implementation
        if self.is_direct_api:
            self.log_message("info", "Using direct API implementation")
            try:
                self.log_message("info", f"Requesting job listings from: {self.career_url}")

                # Make a request to the job board
                response = self.make_greenhouse_request(self.career_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {self.career_url}")
                    return []

                self.log_message("info", f"Response status: {response.status_code}")

                # Extract JSON data from the response
                json_data = self._extract_json_from_response(response.text)

                if not json_data:
                    self.log_message("error", "Could not extract job data from response")
                    return []

                # Navigate to the job posts data
                job_list = []
                if "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]

                    # Try standard pattern first
                    if "routes/$url_token" in loader_data:
                        job_posts_data = loader_data["routes/$url_token"]
                        if "jobPosts" in job_posts_data:
                            job_posts = job_posts_data["jobPosts"]
                            if "data" in job_posts:
                                job_list = job_posts["data"]
                                self.log_message("info", f"Found {len(job_list)} job listings via standard pattern")

                    # If no jobs found, look for alternate pattern with underscore
                    if not job_list:
                        alternate_jobs = []
                        for key, value in loader_data.items():
                            if key.startswith("routes/$url_token_") and "jobs_" in key:
                                # This is typically a single job detail page
                                if isinstance(value, dict) and "jobPost" in value:
                                    job_post = value["jobPost"]
                                    alternate_jobs.append(job_post)

                        if alternate_jobs:
                            job_list = alternate_jobs
                            self.log_message("info", f"Found {len(job_list)} job listings via alternate pattern")

                # Alternative data structure
                elif "jobPosts" in json_data:
                    job_posts = json_data["jobPosts"]
                    if "data" in job_posts:
                        job_list = job_posts["data"]
                        self.log_message("info", f"Found {len(job_list)} job listings via jobPosts structure")
                    else:
                        return []

                if not job_list:
                    self.log_message("warning", "Could not locate job posts data in JSON")
                    return []

                self.log_message("info", f"Processing {len(job_list)} jobs from API response")

                # Process each job
                for idx, job_item in enumerate(job_list):
                    # Log the job structure for the first item
                    if len(jobs) == 0:
                        self.log_message("info", f"First job item keys: {list(job_item.keys())}")

                    # Extract job details
                    job_id = job_item.get("id")
                    job_title = job_item.get("title")
                    location = job_item.get("location")
                    job_url = job_item.get("absolute_url")
                    published_at = job_item.get("published_at")
                    updated_at = job_item.get("updated_at")
                    requisition_id = job_item.get("requisition_id")

                    # Extract remote work type from job_post_location if available
                    work_type = None
                    if "employment_type" in job_item:
                        work_type = job_item.get("employment_type")
                    elif "work_type" in job_item:
                        work_type = job_item.get("work_type")
                    elif "remote_status" in job_item:
                        work_type = job_item.get("remote_status")
                    elif job_title and ("remote" in job_title.lower() or "hybrid" in job_title.lower()):
                        # Infer work type from job title
                        if "remote" in job_title.lower():
                            work_type = "Remote"
                        elif "hybrid" in job_title.lower():
                            work_type = "Hybrid"

                    # Extract compensation from pay_ranges if available
                    compensation = None
                    if "pay_ranges" in job_item:
                        pay_ranges = job_item.get("pay_ranges")
                        if pay_ranges:
                            # Handle potential array structure
                            if isinstance(pay_ranges, list) and len(pay_ranges) > 0:
                                # Extract the first pay range
                                pay_range = pay_ranges[0]
                                if isinstance(pay_range, dict):
                                    min_amount = pay_range.get("min_amount")
                                    max_amount = pay_range.get("max_amount")
                                    currency = pay_range.get("currency", "USD")
                                    interval = pay_range.get("interval", "year")

                                    if min_amount and max_amount:
                                        compensation = f"{currency} {min_amount}-{max_amount}/{interval}"
                                    elif min_amount:
                                        compensation = f"{currency} {min_amount}+/{interval}"
                                    elif max_amount:
                                        compensation = f"Up to {currency} {max_amount}/{interval}"
                                else:
                                    # Handle string or other format
                                    compensation = str(pay_range)
                            else:
                                # Handle non-array format
                                compensation = str(pay_ranges)

                    # Extract job description/content
                    job_description = job_item.get("content", "")

                    # Skip if missing essential fields
                    if not job_id or not job_title or not job_url:
                        self.log_message("warning", f"Skipping job due to missing essential fields - ID: {job_id}, Title: {job_title}")
                        continue

                    # Create department info if available
                    department = None
                    if "department" in job_item and job_item["department"]:
                        department = {
                            "name": job_item["department"].get("name"),
                            "id": job_item["department"].get("id")
                        }

                    # Check if the job posting is recent enough
                    is_recent = True
                    if published_at:
                        try:
                            # Parse the date string and handle timezone
                            # Greenhouse uses ISO format with timezone offset
                            published_date = None

                            # Handle the case where timezone info is already in string
                            if 'Z' in published_at or '+' in published_at or '-' in published_at:
                                if 'Z' in published_at:
                                    # UTC time indicated by Z
                                    published_date = datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                                else:
                                    # Already has timezone offset
                                    published_date = datetime.datetime.fromisoformat(published_at)
                            else:
                                # No timezone info, assume UTC
                                published_date = datetime.datetime.fromisoformat(published_at).replace(tzinfo=timezone.utc)

                            # Now both dates are timezone-aware
                            is_recent = published_date >= self.cutoff_date

                            # Calculate days difference for better context
                            days_diff = (self.cutoff_date - published_date).days
                        except Exception as e:
                            self.log_message("warning", f"Error parsing date {published_at} for job {job_id}: {str(e)}")
                            # When in doubt, include the job
                            is_recent = True

                    # Create job record with all necessary details
                    job = {
                        "job_id": str(job_id),
                        "job_title": job_title,
                        "location": location,
                        "job_url": job_url,
                        "job_description": job_description,
                        "content": job_description,  # Add content field with same value for consistency
                        "published_at": published_at,
                        "updated_at": updated_at,
                        "requisition_id": requisition_id,
                        "department": department,
                        "is_recent": is_recent,
                        "work_type": work_type,
                        "compensation": compensation,
                        "raw_data": job_item
                    }

                    # Add job_post_location separately if it exists (but use it as location, not work_type)
                    if "job_post_location" in job_item:
                        job_post_loc = job_item.get("job_post_location")
                        if job_post_loc:
                            job["location"] = job_post_loc

                    # Filter by keyword if specified
                    if keyword and job_title and keyword.lower() not in job_title.lower():
                        continue

                    # Filter by location if specified
                    if location and job.get("location") and location.lower() not in job["location"].lower():
                        continue

                    jobs.append(job)

                self.log_message("info", f"Processed {len(jobs)} jobs after filtering via direct API")
                return jobs

            except Exception as e:
                self.log_message("error", f"Error searching Greenhouse jobs via direct API: {str(e)}")
                return []
        # Embedded job board implementation
        elif self.is_embedded:
            self.log_message("info", "Using embedded job board implementation")
            try:
                self.log_message("info", f"Requesting embedded job board listings from: {self.career_url}")

                # First, try the original URL
                response = self.make_greenhouse_request(self.career_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {self.career_url}")
                    return []

                # Try to extract JSON data first - this contains all the detailed job information
                json_data = self._extract_json_from_response(response.text)

                # Check if we have usable job board data - if not, try the alternate URL
                should_try_alternate = False
                if not json_data:
                    should_try_alternate = True
                elif "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]
                    # Check if we have the proper embedded job board structure
                    if "routes/embed.job_board" not in loader_data:
                        should_try_alternate = True

                # If no usable job board data found and this is a boards.greenhouse.io URL, try the job-boards.greenhouse.io variant
                if should_try_alternate and "boards.greenhouse.io/embed/job_board?for=" in self.career_url:
                    self.log_message("info", "Trying job-boards.greenhouse.io variant for better job board data")

                    # Transform URL from boards.greenhouse.io to job-boards.greenhouse.io
                    alternate_url = self.career_url.replace("boards.greenhouse.io/embed/job_board?for=", "job-boards.greenhouse.io/embed/job_board?for=")

                    # Make request to alternate URL
                    alternate_response = self.make_greenhouse_request(alternate_url)

                    if alternate_response:
                        # Try to extract JSON from alternate response
                        alternate_json_data = self._extract_json_from_response(alternate_response.text)

                        if alternate_json_data:
                            # Check if the alternate response has proper job board data
                            if ("state" in alternate_json_data and "loaderData" in alternate_json_data["state"] and
                                "routes/embed.job_board" in alternate_json_data["state"]["loaderData"]):
                                self.log_message("info", "Successfully found proper job board data from alternate URL")
                                json_data = alternate_json_data
                                response = alternate_response  # Use the alternate response going forward

                if json_data:
                    self.log_message("info", "Successfully extracted JSON data from embedded job board")

                    # Look for job posts in the JSON data structure
                    job_list = []
                    if "state" in json_data and "loaderData" in json_data["state"]:
                        loader_data = json_data["state"]["loaderData"]

                        # Look for the embed.job_board route which contains full job data
                        if "routes/embed.job_board" in loader_data:
                            job_board_data = loader_data["routes/embed.job_board"]
                            if "jobPosts" in job_board_data and "data" in job_board_data["jobPosts"]:
                                job_list = job_board_data["jobPosts"]["data"]
                                self.log_message("info", f"Found {len(job_list)} jobs from embedded JSON data")

                        # Alternative: Look for root level data that might contain job posts
                        if not job_list and "root" in loader_data:
                            root_data = loader_data["root"]
                            if isinstance(root_data, dict) and "jobPosts" in root_data:
                                if "data" in root_data["jobPosts"]:
                                    job_list = root_data["jobPosts"]["data"]
                                    self.log_message("info", f"Found {len(job_list)} jobs from root jobPosts data")

                    # Process jobs from JSON data (contains all necessary fields)
                    for idx, job_item in enumerate(job_list):
                        try:
                            # Extract all fields directly from the JSON response
                            job_id = job_item.get("id")
                            job_title = job_item.get("title", "")
                            location = job_item.get("location", "")
                            job_url = job_item.get("absolute_url", "")
                            published_at = job_item.get("published_at")
                            updated_at = job_item.get("updated_at")
                            requisition_id = job_item.get("requisition_id")

                            # Ensure job_id is a clean string without URL parameters
                            if job_id:
                                job_id = str(job_id)
                                # Remove any URL parameters that might be appended
                                if '?' in job_id:
                                    job_id = job_id.split('?')[0]
                            else:
                                job_id = ""

                            # Extract job description from content field
                            job_description = job_item.get("content", "")

                            # Handle escaped HTML content
                            if job_description and isinstance(job_description, str):
                                # Unescape HTML entities and unicode escapes
                                import html
                                if "\\u003c" in job_description or "&lt;" in job_description:
                                    # Handle unicode escapes first
                                    try:
                                        job_description = bytes(job_description, "utf-8").decode("unicode_escape")
                                    except:
                                        pass
                                    # Then handle HTML entities
                                    job_description = html.unescape(job_description)

                            # Extract department info
                            department = None
                            if "department" in job_item and job_item["department"]:
                                department_data = job_item["department"]
                                if isinstance(department_data, dict):
                                    department = {
                                        "name": department_data.get("name"),
                                        "id": department_data.get("id")
                                    }
                                elif isinstance(department_data, str):
                                    department = {"name": department_data}

                            # Check if the job posting is recent enough
                            is_recent = True
                            if published_at:
                                try:
                                    # Parse the date string and handle timezone
                                    # Greenhouse uses ISO format with timezone offset
                                    published_date = None

                                    # Handle the case where timezone info is already in string
                                    if 'Z' in published_at or '+' in published_at or '-' in published_at:
                                        if 'Z' in published_at:
                                            # UTC time indicated by Z
                                            published_date = datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                                        else:
                                            # Already has timezone offset
                                            published_date = datetime.datetime.fromisoformat(published_at)
                                    else:
                                        # No timezone info, assume UTC
                                        published_date = datetime.datetime.fromisoformat(published_at).replace(tzinfo=timezone.utc)

                                    # Now both dates are timezone-aware
                                    is_recent = published_date >= self.cutoff_date

                                    # Calculate days difference for better context
                                    days_diff = (self.cutoff_date - published_date).days
                                except Exception as e:
                                    self.log_message("warning", f"Error parsing date {published_at} for job {job_id}: {str(e)}")
                                    # When in doubt, include the job
                                    is_recent = True

                            # Skip if missing essential fields
                            if not job_id or not job_title:
                                self.log_message("warning", f"Skipping embedded job with missing essential fields: id={job_id}, title={job_title}")
                                continue

                            # Create comprehensive job record with all data from JSON
                            job = {
                                "job_id": job_id,
                                "job_title": job_title,
                                "location": location,
                                "job_url": job_url,
                                "job_description": job_description,
                                "content": job_description,  # Add content field with same value for consistency
                                "published_at": published_at,
                                "updated_at": updated_at,
                                "requisition_id": requisition_id,
                                "department": department,
                                "is_recent": is_recent,
                                "raw_data": job_item
                            }

                            # Filter by keyword if specified
                            if keyword and job_title and keyword.lower() not in job_title.lower():
                                continue

                            # Filter by location if specified
                            if location and job.get("location") and location.lower() not in job["location"].lower():
                                continue

                            jobs.append(job)

                        except Exception as e:
                            self.log_message("warning", f"Error processing embedded job from JSON: {str(e)}")
                            continue

                    if jobs:
                        self.log_message("info", f"Successfully extracted {len(jobs)} jobs from embedded JSON data")
                        return jobs

                # Fallback to HTML parsing if JSON extraction fails
                self.log_message("info", "DEBUG: JSON extraction failed, falling back to HTML parsing")

                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings - embedded Greenhouse boards use elements with class="opening"
                job_elements = soup.select('.opening')

                self.log_message("info", f"Found {len(job_elements)} job listings on embedded board via HTML parsing")

                # Process each job listing
                for job_element in job_elements:
                    try:
                        # Extract job title from the link
                        title_element = job_element.select_one('a')
                        if not title_element:
                            continue

                        job_title = title_element.text.strip()
                        job_url = title_element.get('href')

                        # Extract location if available
                        location_element = job_element.select_one('.location')
                        job_location = location_element.text.strip() if location_element else ""

                        # Extract job ID from the URL
                        job_id = None
                        if 'gh_jid=' in job_url:
                            # Handle both normal and malformed URLs with duplicate gh_jid parameters
                            self.log_message("info", f"DEBUG: Extracting job ID from URL: {job_url}")

                            # First try: Direct regex extraction to handle malformed URLs
                            gh_jid_match = re.search(r'gh_jid=(\d+)', job_url)
                            if gh_jid_match:
                                job_id = gh_jid_match.group(1)
                                self.log_message("info", f"DEBUG: Extracted job ID via regex: {job_id}")
                            else:
                                # Fallback: Try standard URL parsing
                                try:
                                    parsed_url = urlparse(job_url)
                                    query_params = parse_qs(parsed_url.query)
                                    if 'gh_jid' in query_params:
                                        job_id = query_params['gh_jid'][0]
                                        self.log_message("info", f"DEBUG: Extracted job ID via URL parsing: {job_id}")
                                except Exception as e:
                                    self.log_message("warning", f"DEBUG: Error parsing URL {job_url}: {str(e)}")

                        if not job_id:
                            self.log_message("warning", f"Could not extract job ID from URL: {job_url}")
                            continue

                        # Create basic job record
                        job = {
                            "job_id": str(job_id),
                            "job_title": job_title,
                            "location": job_location,
                            "job_url": job_url,
                            "is_recent": True,  # We'll assume it's recent until we get detailed info
                        }

                        # Filter by keyword if specified
                        if keyword and job_title and keyword.lower() not in job_title.lower():
                            continue

                        # Filter by location if specified
                        if location and job_location and location.lower() not in job_location.lower():
                            continue

                        jobs.append(job)

                    except Exception as e:
                        self.log_message("warning", f"Error processing job listing from HTML: {str(e)}")
                        continue

                self.log_message("info", f"Processed {len(jobs)} jobs after filtering")
                return jobs

            except Exception as e:
                self.log_message("error", f"Error searching embedded Greenhouse jobs: {str(e)}")
                return []

        return jobs

    def _find_job_data_anywhere(self, json_data, depth=0, max_depth=5):
        """
        Recursively search the JSON data structure for any object that looks like job data.
        This is a last resort method when standard paths fail.

        Args:
            json_data: The JSON data to search
            depth: Current recursion depth
            max_depth: Maximum recursion depth to prevent infinite loops

        Returns:
            Dict with job data if found, None otherwise
        """
        if depth > max_depth:
            return None

        if not isinstance(json_data, dict):
            return None

        # Check if this dict looks like job data
        job_data_indicators = ['title', 'content', 'description', 'job_title', 'location']
        indicator_count = sum(1 for indicator in job_data_indicators if indicator in json_data)
        content_length = 0

        # Check if there's a long content field
        if 'content' in json_data and isinstance(json_data['content'], str):
            content_length = len(json_data['content'])
        elif 'description' in json_data and isinstance(json_data['description'], str):
            content_length = len(json_data['description'])

        # If this dict has multiple job indicators or a long content field, it's likely job data
        if indicator_count >= 2 or content_length > 100:
            self.log_message("info", f"Found potential job data at depth {depth} with indicators: {indicator_count}")
            return json_data

        # Recursively search all dict values
        for key, value in json_data.items():
            if isinstance(value, dict):
                result = self._find_job_data_anywhere(value, depth + 1, max_depth)
                if result:
                    return result
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        result = self._find_job_data_anywhere(item, depth + 1, max_depth)
                        if result:
                            return result

        return None

    def get_job_details(self, job_url: str) -> Dict:
        """
        Get detailed information about a specific job.

        Args:
            job_url: URL of the job posting

        Returns:
            Dictionary containing job details
        """
        job_details = {
            'job_url': job_url
        }

        # For direct API jobs
        if self.is_direct_api:
            try:
                self.log_message("info", f"Getting details for job URL: {job_url}")

                # Make a request to the job detail page
                response = self.make_greenhouse_request(job_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {job_url}")
                    return job_details

                # Extract JSON data from the response
                json_data = self._extract_json_from_response(response.text)

                if not json_data:
                    self.log_message("error", "Could not extract job detail data from response")
                    return job_details

                # Navigate to the job data
                job_data = None
                if "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]

                    # First, try to find keys that follow the standard pattern
                    for key, value in loader_data.items():
                        if key.startswith("routes/$url_token/jobs"):
                            job_data = value
                            break

                    # If not found, try the alternative pattern with underscore (routes/$url_token_.jobs_.$job_post_id)
                    if not job_data:
                        for key, value in loader_data.items():
                            if key.startswith("routes/$url_token_") and "jobs_" in key:
                                if isinstance(value, dict):
                                    if "jobPost" in value:
                                        # Extract the jobPost object which contains the content
                                        job_data = value["jobPost"]
                                    else:
                                        # Use the value as is if jobPost is not present
                                        job_data = value
                                break

                # Additional specific check for the exact pattern from user's example
                if not job_data:
                    exact_key = "routes/$url_token_.jobs_.$job_post_id"
                    if exact_key in json_data.get("state", {}).get("loaderData", {}):
                        direct_data = json_data["state"]["loaderData"][exact_key]
                        if isinstance(direct_data, dict) and "jobPost" in direct_data:
                            job_data = direct_data["jobPost"]

                if not job_data:
                    self.log_message("warning", "Could not locate job detail data in JSON")
                    # As a last resort, try to extract job data from anywhere in the JSON
                    found_data = self._find_job_data_anywhere(json_data)
                    if found_data:
                        job_data = found_data
                    else:
                        return job_details

                # IMMEDIATELY extract dates after job_data is determined - BEFORE other processing
                # This ensures dates are captured regardless of which pattern was used
                if job_data:
                    if "published_at" in job_data:
                        job_details["published_at"] = job_data["published_at"]

                        # Check if the job posting is recent enough
                        try:
                            # Parse the date string and handle timezone
                            # Greenhouse uses ISO format with timezone offset
                            published_date = None

                            # Handle the case where timezone info is already in string
                            if 'Z' in job_data["published_at"] or '+' in job_data["published_at"] or '-' in job_data["published_at"]:
                                if 'Z' in job_data["published_at"]:
                                    # UTC time indicated by Z
                                    published_date = datetime.datetime.fromisoformat(job_data["published_at"].replace('Z', '+00:00'))
                                else:
                                    # Already has timezone offset
                                    published_date = datetime.datetime.fromisoformat(job_data["published_at"])
                            else:
                                # No timezone info, assume UTC
                                published_date = datetime.datetime.fromisoformat(job_data["published_at"]).replace(tzinfo=timezone.utc)

                            # Now both dates are timezone-aware
                            job_details["is_recent"] = published_date >= self.cutoff_date
                            self.log_message("info", f"Job published date: {published_date}, Is recent: {job_details['is_recent']}")

                            # Calculate days difference for better context
                            days_diff = (self.cutoff_date - published_date).days
                        except Exception as e:
                            self.log_message("warning", f"Error parsing published_at date {job_data['published_at']}: {str(e)}")
                            # When in doubt, include the job
                            job_details["is_recent"] = True
                    else:
                        job_details["is_recent"] = True  # Assume recent if no date

                    if "updated_at" in job_data:
                        job_details["updated_at"] = job_data["updated_at"]
                else:
                    self.log_message("error", "job_data is None - cannot extract date fields!")
                    job_details["is_recent"] = True  # Assume recent if no data

                # Extract job ID from URL if not already present
                if not job_details.get("job_id") and '/jobs/' in job_url:
                    job_id = job_url.split('/jobs/')[1].split('/')[0]
                    job_details["job_id"] = job_id

                # Process job detail fields
                if "title" in job_data:
                    job_details["job_title"] = job_data["title"]

                # Get location information - correctly handled as location, not work_type
                if "job_post_location" in job_data:
                    job_details["location"] = job_data["job_post_location"]
                elif "location" in job_data:
                    # Handle both string and object formats for location
                    if isinstance(job_data["location"], dict):
                        job_details["location"] = job_data["location"].get("name", "")
                    else:
                        job_details["location"] = job_data["location"]

                # Extract work type if available (correctly separated from location)
                # Look for any fields that might indicate remote/hybrid work arrangement
                for field_name in ["employment_type", "work_type", "remote_status", "work_arrangement"]:
                    if field_name in job_data:
                        job_details["work_type"] = job_data[field_name]
                        break

                # Try to infer work type from the job title or description if not found
                if not job_details.get("work_type") and job_details.get("job_title"):
                    title = job_details["job_title"].lower()
                    if "remote" in title:
                        job_details["work_type"] = "Remote"
                    elif "hybrid" in title:
                        job_details["work_type"] = "Hybrid"

                # Try to infer work type from the content/job description if not already found
                if not job_details.get("work_type") and "content" in job_data:
                    content = job_data["content"].lower()
                    if "remote" in content:
                        job_details["work_type"] = "Remote"
                    elif "hybrid" in content:
                        job_details["work_type"] = "Hybrid"
                    elif "onsite" in content or "on-site" in content or "on site" in content:
                        job_details["work_type"] = "On-site"

                # Default to On-site if we couldn't determine work type
                if not job_details.get("work_type"):
                    job_details["work_type"] = "Unspecified"

                # Extract job description - try multiple possible fields and formats
                if "job_description" in job_data:
                    job_details["job_description"] = job_data["job_description"]
                elif "content" in job_data:
                    # The main content typically contains the job description
                    job_details["job_description"] = job_data["content"]
                    content_preview = job_data["content"][:100].replace('\n', ' ')
                    self.log_message("info", f"Found job description in 'content' field: {content_preview}...")
                else:
                    # As a fallback, look for any large text fields
                    large_text_fields = {}
                    for key, value in job_data.items():
                        if isinstance(value, str) and len(value) > 100:
                            large_text_fields[key] = value

                    if large_text_fields:
                        # Pick the largest text field as the description
                        largest_field = max(large_text_fields.items(), key=lambda x: len(x[1]))
                        job_details["job_description"] = largest_field[1]
                        self.log_message("info", f"Using {largest_field[0]} as job description (length: {len(largest_field[1])})")

                # Store the full raw data for reference
                job_details["raw_data"] = job_data

                # Extract other important metadata
                # Extract department info
                if "departments" in job_data and job_data["departments"] and len(job_data["departments"]) > 0:
                    dept = job_data["departments"][0]
                    job_details["department"] = {
                        "name": dept.get("name"),
                        "id": dept.get("id")
                    }
                elif "department" in job_data and job_data["department"]:
                    job_details["department"] = {
                        "name": job_data["department"].get("name"),
                        "id": job_data["department"].get("id")
                    }

                if "requisition_id" in job_data:
                    job_details["requisition_id"] = job_data["requisition_id"]

                # Extract compensation from pay_ranges or salaryDescription
                if "pay_ranges" in job_data:
                    pay_ranges = job_data.get("pay_ranges")
                    if pay_ranges:
                        # Handle potential array structure
                        if isinstance(pay_ranges, list) and len(pay_ranges) > 0:
                            # Extract the first pay range
                            pay_range = pay_ranges[0]
                            if isinstance(pay_range, dict):
                                min_amount = pay_range.get("min_amount")
                                max_amount = pay_range.get("max_amount")
                                currency = pay_range.get("currency", "USD")
                                interval = pay_range.get("interval", "year")

                                if min_amount and max_amount:
                                    job_details["compensation"] = f"{currency} {min_amount}-{max_amount}/{interval}"
                                elif min_amount:
                                    job_details["compensation"] = f"{currency} {min_amount}+/{interval}"
                                elif max_amount:
                                    job_details["compensation"] = f"Up to {currency} {max_amount}/{interval}"
                            else:
                                # Handle string or other format
                                job_details["compensation"] = str(pay_range)
                        else:
                            # Handle non-array format
                            job_details["compensation"] = str(pay_ranges)

                # Check for salaryDescription as alternative source
                if not job_details.get("compensation") and "salaryDescription" in job_data:
                    job_details["compensation"] = job_data["salaryDescription"]

                return job_details

            except Exception as e:
                self.log_message("error", f"Error getting Greenhouse job details via direct API: {str(e)}")
                return job_details

        # For embedded job board jobs
        elif self.is_embedded and 'gh_jid=' in job_url:
            try:
                self.log_message("info", f"Getting details for embedded job URL: {job_url}")

                # Extract the job ID from the URL
                parsed_url = urlparse(job_url)
                query_params = parse_qs(parsed_url.query)

                if 'gh_jid' not in query_params:
                    self.log_message("error", f"Could not extract job ID from URL: {job_url}")
                    return job_details

                job_id = query_params['gh_jid'][0]
                job_details["job_id"] = job_id

                # Construct the modified URL to get structured job details
                # Format: https://job-boards.greenhouse.io/embed/job_app?for=TENANT&token=JOB_ID
                detail_url = f"https://job-boards.greenhouse.io/embed/job_app?for={self.tenant}&token={job_id}"

                self.log_message("info", f"Requesting job details from: {detail_url}")

                # Make request to the structured job detail endpoint
                response = self.make_greenhouse_request(detail_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {detail_url}")
                    return job_details

                # FALLBACK 1: Try extracting job data from window.__remixContext
                try:
                    remix_context_pattern = r'window\.__remixContext\s*=\s*({.*?});'
                    remix_match = re.search(remix_context_pattern, response.text, re.DOTALL)
                    if remix_match:
                        remix_json_str = remix_match.group(1)
                        remix_data = json.loads(remix_json_str)
                        self.log_message("info", "Successfully extracted data from __remixContext")

                        # Navigate through the remix data structure to find jobPost
                        if "state" in remix_data and "loaderData" in remix_data["state"]:
                            for route_key, route_data in remix_data["state"]["loaderData"].items():
                                if route_key != "root" and isinstance(route_data, dict):
                                    # Check for jobPost in this route data
                                    if "jobPost" in route_data:
                                        job_post_data = route_data["jobPost"]

                                        if "title" in job_post_data and not job_details.get("job_title"):
                                            job_details["job_title"] = job_post_data["title"]

                                        if "job_post_location" in job_post_data and not job_details.get("location"):
                                            job_details["location"] = job_post_data["job_post_location"]

                                        # Extract job description from remixContext
                                        if not job_details.get("job_description"):
                                            # Check for content field
                                            if "content" in job_post_data:
                                                job_details["job_description"] = job_post_data["content"]
                                                job_details["content"] = job_post_data["content"]
                                            # Check for introduction field
                                            elif "introduction" in job_post_data:
                                                job_details["job_description"] = job_post_data["introduction"]
                                                job_details["content"] = job_post_data["introduction"]
                                            # Check for conclusion field (often has content too)
                                            elif "conclusion" in job_post_data:
                                                job_details["job_description"] = job_post_data["conclusion"]
                                                job_details["content"] = job_post_data["conclusion"]

                                        # Extract department information if available
                                        if "department" in job_post_data and not job_details.get("department"):
                                            department_data = job_post_data["department"]
                                            if isinstance(department_data, dict):
                                                job_details["department"] = department_data
                                            elif isinstance(department_data, str):
                                                job_details["department"] = {"name": department_data}

                                        # Extract published_at and updated_at from remixContext jobPost data
                                        if "published_at" in job_post_data and not job_details.get("published_at"):
                                            job_details["published_at"] = job_post_data["published_at"]

                                            # Parse the date for recency check
                                            try:
                                                if 'Z' in job_post_data["published_at"] or '+' in job_post_data["published_at"] or '-' in job_post_data["published_at"]:
                                                    if 'Z' in job_post_data["published_at"]:
                                                        published_date = datetime.datetime.fromisoformat(job_post_data["published_at"].replace('Z', '+00:00'))
                                                    else:
                                                        published_date = datetime.datetime.fromisoformat(job_post_data["published_at"])
                                                else:
                                                    published_date = datetime.datetime.fromisoformat(job_post_data["published_at"]).replace(tzinfo=timezone.utc)

                                                job_details["is_recent"] = published_date >= self.cutoff_date
                                                self.log_message("info", f"Embedded job published date: {published_date}, Is recent: {job_details['is_recent']}")

                                                # Calculate days difference for better context
                                                days_diff = (self.cutoff_date - published_date).days
                                            except Exception as e:
                                                self.log_message("warning", f"Error parsing embedded job published_at date: {str(e)}")
                                                job_details["is_recent"] = True

                                        if "updated_at" in job_post_data and not job_details.get("updated_at"):
                                            job_details["updated_at"] = job_post_data["updated_at"]
                except Exception as remix_error:
                    self.log_message("warning", f"Failed to extract data from remixContext: {str(remix_error)}")

                # FALLBACK 2: Direct HTML extraction for critical fields (title, location)
                try:
                    # Only attempt HTML extraction if fields are still missing
                    if not job_details.get("job_title"):
                        # Extract job title from HTML
                        title_pattern = r'<h1[^>]*class="[^"]*section-header[^"]*"[^>]*>(.*?)</h1>'
                        title_match = re.search(title_pattern, response.text, re.DOTALL)
                        if title_match:
                            job_title = title_match.group(1).strip()
                            job_details["job_title"] = job_title

                    if not job_details.get("location"):
                        # Extract location from HTML
                        location_pattern = r'<div class="job__location">.*?<div>(.*?)</div>'
                        location_match = re.search(location_pattern, response.text, re.DOTALL)
                        if location_match:
                            location = location_match.group(1).strip()
                            job_details["location"] = location
                except Exception as html_error:
                    self.log_message("warning", f"Error extracting fields from HTML: {str(html_error)}")

                # FALLBACK 3: Try to parse JSON response
                try:
                    job_data = response.json()
                    self.log_message("info", f"Successfully parsed JSON from embedded job")

                    # Extract relevant fields if they're not already set by previous methods
                    if "title" in job_data and not job_details.get("job_title"):
                        job_details["job_title"] = job_data["title"]

                    if "location" in job_data and not job_details.get("location"):
                        job_details["location"] = job_data["location"]["name"] if isinstance(job_data["location"], dict) else job_data["location"]

                    # Extract job description - try multiple possible fields and formats
                    if not job_details.get("job_description"):
                        # First check direct content field
                        if "content" in job_data:
                            job_details["job_description"] = job_data["content"]
                            job_details["content"] = job_data["content"]
                        elif "description" in job_data:
                            job_details["job_description"] = job_data["description"]
                            job_details["content"] = job_data["description"]

                        # If not found, check nested fields
                        if not job_details.get("job_description"):
                            # Check in jobPostingData
                            if "jobPostingData" in job_data:
                                job_posting_data = job_data["jobPostingData"]
                                if isinstance(job_posting_data, dict):
                                    if "description" in job_posting_data:
                                        job_details["job_description"] = job_posting_data["description"]
                                        job_details["content"] = job_posting_data["description"]
                                    elif "content" in job_posting_data:
                                        job_details["job_description"] = job_posting_data["content"]
                                        job_details["content"] = job_posting_data["content"]

                            # Check in jobPost
                            if not job_details.get("job_description") and "jobPost" in job_data:
                                job_post = job_data["jobPost"]
                                if isinstance(job_post, dict):
                                    if "content" in job_post:
                                        job_details["job_description"] = job_post["content"]
                                        job_details["content"] = job_post["content"]
                                    elif "description" in job_post:
                                        job_details["job_description"] = job_post["description"]
                                        job_details["content"] = job_post["description"]

                        # If we still don't have a description, try finding any key ending with "description" or "content"
                        if not job_details.get("job_description"):
                            for key, value in job_data.items():
                                if isinstance(value, str) and (key.lower().endswith("description") or key.lower().endswith("content")):
                                    job_details["job_description"] = value
                                    job_details["content"] = value
                                    break

                    # Extract department info
                    if "departments" in job_data and job_data["departments"] and len(job_data["departments"]) > 0:
                        dept = job_data["departments"][0]
                        job_details["department"] = {
                            "name": dept.get("name"),
                            "id": dept.get("id")
                        }
                    elif "department" in job_data and job_data["department"]:
                        job_details["department"] = {
                            "name": job_data["department"].get("name"),
                            "id": job_data["department"].get("id")
                        }
                except Exception as e:
                    self.log_message("warning", f"Failed to parse JSON response: {str(e)}")

                return job_details

            except Exception as e:
                self.log_message("error", f"Error getting embedded Greenhouse job details: {str(e)}")
                return job_details

        return job_details

    def log_message(self, level: str, message: str):
        """
        Log a message using either the Dagster logger (if available) or the standard Python logger.

        Args:
            level: Log level (info, warning, error)
            message: Message to log
        """
        # First print to console for direct visibility
        print(f"GREENHOUSE_SCRAPER: {level.upper()} - {message}")

        # Use Dagster logger if available
        if self.dagster_log:
            if level == "info":
                self.dagster_log.info(message)
            elif level == "warning":
                self.dagster_log.warning(message)
            elif level == "error":
                self.dagster_log.error(message)
            elif level == "debug":
                self.dagster_log.debug(message)
        # Fall back to standard logging
        else:
            if level == "info":
                logging.info(message)
            elif level == "warning":
                logging.warning(message)
            elif level == "error":
                logging.error(message)
            elif level == "debug":
                logging.debug(message)