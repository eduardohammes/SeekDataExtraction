# scripts/transformation.py

import logging
from datetime import datetime
from typing import Dict, List

from airflow.decorators import task

from scripts.models import JobData, LOGGER


@task
def extract_transform(all_job_data: List[Dict]) -> List[Dict]:
    extracted_data = []
    for job_data in all_job_data:
        for job in job_data.get("data", []):
            listing_date_str = job.get('listingDate')
            listing_date = (
                datetime.strptime(listing_date_str, "%Y-%m-%dT%H:%M:%SZ")
                if listing_date_str
                else datetime.now()
            )

            job_info = {
                "job_id": job.get('id'),
                "job_title": job.get('title'),
                "company": job.get('advertiser', {}).get('description'),
                "company_logo_url": job.get('branding', {})
                .get('assets', {})
                .get('logo', {})
                .get('strategies', {})
                .get('serpLogo'),
                "location": job.get('location'),
                "area": job.get('area'),
                "suburb": job.get('suburb'),
                "work_type": job.get('workType'),
                "work_arrangement": job.get('workArrangements', {})
                .get('data', [{}])[0]
                .get('label', {})
                .get('text'),
                "salary": job.get('salary', 'Not provided'),
                "listing_date": listing_date.strftime("%Y-%m-%d %H:%M:%S"),
                "teaser": job.get('teaser'),
                "classification": job.get('classification', {}).get('description'),
                "sub_classification": job.get('subClassification', {}).get('description'),
                "is_premium": job.get('isPremium'),
                "is_standout": job.get('isStandOut'),
                "job_location_label": job.get('jobLocation', {}).get('label'),
                "job_advertiser_id": job.get('advertiser', {}).get('id'),
                "request_token": job_data.get('solMetadata', {}).get('requestToken'),
            }

            try:
                job_data_instance = JobData(**job_info)
                extracted_data.append(job_data_instance.dict())
            except ValueError as e:
                LOGGER.error(f"Data validation error: {e}")

    LOGGER.info(f"Extracted and transformed {len(extracted_data)} job records.")
    return extracted_data
