# scripts/models.py
import logging
from typing import Optional

import yaml

# Load configuration
with open('/opt/airflow/config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

API_CONFIG = config['api']
DB_CONFIG = config['database']
LOGGING_CONFIG = config['logging']

# Set up logging
logging.basicConfig(
    level=LOGGING_CONFIG['level'],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

LOGGER = logging.getLogger(__name__)


def update_metadata(job_data, cookies):
    return {
        "userqueryid": job_data.get('userQueryId'),
        "userid": cookies.get("JobseekerVisitorId"),
        "usersessionid": cookies.get("JobseekerSessionId"),
        "eventCaptureSessionId": cookies.get("JobseekerSessionId"),
        "solId": job_data.get("searchParams", {}).get("solid"),
        "hadPremiumListings": job_data.get("paginationParameters", {}).get("hadPremiumListings", True),
        "include": job_data.get("searchParams", {}).get('include', 'seodata'),
    }


# Pydantic model for data validation
from pydantic import BaseModel


class JobData(BaseModel):
    job_id: str
    job_title: Optional[str]
    company: Optional[str]
    company_logo_url: Optional[str]
    location: Optional[str]
    area: Optional[str]
    suburb: Optional[str]
    work_type: Optional[str]
    work_arrangement: Optional[str]
    salary: Optional[str]
    listing_date: Optional[str]
    teaser: Optional[str]
    classification: Optional[str]
    sub_classification: Optional[str]
    is_premium: Optional[bool]
    is_standout: Optional[bool]
    job_location_label: Optional[str]
    job_advertiser_id: Optional[str]
    request_token: Optional[str]
