import datetime
from typing import Dict, List, Optional, Tuple

import requests
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from scripts.models import API_CONFIG, DB_CONFIG, LOGGER, update_metadata


def get_last_listing_date(connection_id: str) -> datetime.datetime:
    pg_hook = PostgresHook(postgres_conn_id=connection_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(listing_date) FROM job_data;")
    result = cursor.fetchone()
    cursor.close()
    connection.close()

    if result and result[0]:
        return result[0]
    else:
        # If no records are found, default to 30 days ago
        return datetime.datetime.now() - datetime.timedelta(days=30)


@task
def fetch_all_job_data() -> List[Dict]:
    # Fetch the last listing_date from the database
    last_listing_date = get_last_listing_date(DB_CONFIG["connection_id"])
    LOGGER.info(f"Last listing date from DB: {last_listing_date}")

    # Calculate the daterange
    today = datetime.datetime.now()
    delta = today - last_listing_date
    daterange = max(delta.days, 1)  # Ensure at least 1 day

    LOGGER.info(f"Calculated daterange: {daterange} days")

    # Use the calculated daterange in API parameters
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
    page_number: int,
    metadata: Optional[Dict[str, str]],
    cookies: Optional[Dict[str, str]],
    daterange: int,
) -> Tuple[Optional[Dict], Optional[Dict]]:
    params = _construct_params(page_number, metadata, daterange)
    headers = {}

    if cookies:
        headers["Cookie"] = _construct_cookie_header(cookies)

    try:
        response = requests.get(API_CONFIG["base_url"], headers=headers, params=params)
        response.raise_for_status()
        cookies = response.cookies.get_dict()
        return response.json(), cookies
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Failed to retrieve data: {e}")
        return None, None


def _construct_params(page_number: int, metadata: Optional[Dict[str, str]], daterange: int) -> Dict:
    return {
        "siteKey": API_CONFIG["default_sitekey"],
        "userqueryid": metadata.get("userqueryid", "") if metadata else None,
        "userid": metadata.get("userid", "") if metadata else None,
        "usersessionid": metadata.get("usersessionid", "") if metadata else None,
        "eventCaptureSessionId": metadata.get("eventCaptureSessionId", "") if metadata else None,
        "where": "All Sydney NSW",
        "page": page_number,
        "seekSelectAllPages": "true",
        "keywords": "Data Engineer",
        "daterange": daterange,
        "hadPremiumListings": metadata.get("hadPremiumListings", True) if metadata else True,
        "pageSize": metadata.get("pageSize", API_CONFIG["default_page_size"])
        if metadata
        else API_CONFIG["default_page_size"],
        "include": metadata.get("include", "") if metadata else None,
        "locale": API_CONFIG["default_locale"],
        "solId": metadata.get("solId") if metadata else None,
    }


def _construct_cookie_header(cookies: Dict[str, str]) -> str:
    return "; ".join([f"{key}={value}" for key, value in cookies.items()])
