# scripts/extraction.py

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from airflow.decorators import task

from scripts.models import API_CONFIG, LOGGER, update_metadata


@task
def fetch_all_job_data(last_listing_date: Optional[str]) -> List[Dict]:
    """
    Fetches all job data from the API, starting from the last_listing_date.
    """
    if last_listing_date:
        # Calculate daterange as the number of days between today and last_listing_date
        today = datetime.now()
        last_date = datetime.strptime(last_listing_date, "%Y-%m-%d")
        daterange = (today - last_date).days
        if daterange < 1:
            daterange = 1  # Minimum daterange is 1
    else:
        # If no last_listing_date, use a default daterange of 7 days
        daterange = 7

    # Ensure daterange is rounded up to make sure we receive all roles
    LOGGER.info(f"Calculated daterange: {daterange} days")

    page_number = 1
    all_job_data = []
    metadata = None
    cookies = None

    while True:
        job_data, new_cookies = fetch_job_data(page_number, metadata, cookies, daterange)
        if job_data:
            if new_cookies:
                cookies = new_cookies

            metadata = update_metadata(job_data, cookies)
            all_job_data.append(job_data)

            # If no more pages, break
            if page_number >= job_data.get("totalPages", 1):
                break
            else:
                page_number += 1
        else:
            break

    LOGGER.info(f"Fetched {len(all_job_data)} pages of job data.")
    return all_job_data


def fetch_job_data(
    page_number: int = 1,
    metadata: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    daterange: int = API_CONFIG['default_daterange'],
) -> Tuple[Optional[Dict], Optional[Dict]]:
    params = _construct_params(page_number, metadata, daterange)
    headers = {}

    if cookies:
        headers['Cookie'] = _construct_cookie_header(cookies)

    try:
        response = requests.get(API_CONFIG['base_url'], headers=headers, params=params)
        response.raise_for_status()
        cookies = response.cookies.get_dict()
        return response.json(), cookies
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Failed to retrieve data: {e}")
        return None, None


def _construct_params(page_number: int, metadata: Optional[Dict[str, str]], daterange: int) -> Dict:
    return {
        'siteKey': API_CONFIG['default_sitekey'],
        'userqueryid': metadata.get("userqueryid", '') if metadata else None,
        'userid': metadata.get('userid', '') if metadata else None,
        'usersessionid': metadata.get('usersessionid', '') if metadata else None,
        'eventCaptureSessionId': metadata.get('eventCaptureSessionId', '') if metadata else None,
        "where": "All Sydney NSW",
        "page": page_number,
        "seekSelectAllPages": "true",
        "keywords": "Data Engineer",
        "daterange": daterange,
        "hadPremiumListings": metadata.get("hadPremiumListings", True) if metadata else True,
        "pageSize": metadata.get('pageSize', API_CONFIG['default_page_size'])
        if metadata
        else API_CONFIG['default_page_size'],
        "include": metadata.get('include', '') if metadata else None,
        "locale": API_CONFIG['default_locale'],
        "solId": metadata.get("solId") if metadata else None,
    }


def _construct_cookie_header(cookies: Dict[str, str]) -> str:
    return '; '.join([f"{key}={value}" for key, value in cookies.items()])
