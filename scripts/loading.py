import logging
from typing import List

from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from scripts.models import DB_CONFIG, LOGGER

@task
def load_data(all_jobs: List[dict]):
    pg_hook = PostgresHook(postgres_conn_id=DB_CONFIG['connection_id'])

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS job_data (
        job_id VARCHAR PRIMARY KEY,
        job_title VARCHAR,
        company VARCHAR,
        company_logo_url VARCHAR,
        location VARCHAR,
        area VARCHAR,
        suburb VARCHAR,
        work_type VARCHAR,
        work_arrangement VARCHAR,
        salary VARCHAR,
        listing_date TIMESTAMP,
        teaser TEXT,
        classification VARCHAR,
        sub_classification VARCHAR,
        is_premium BOOLEAN,
        is_standout BOOLEAN,
        job_location_label VARCHAR,
        job_advertiser_id VARCHAR,
        request_token VARCHAR
    );
    """
    pg_hook.run(create_table_sql)
    LOGGER.info("Ensured job_data table exists in PostgreSQL.")

    insert_sql = """
    INSERT INTO job_data (
        job_id, job_title, company, company_logo_url, location, area, suburb,
        work_type, work_arrangement, salary, listing_date, teaser, classification,
        sub_classification, is_premium, is_standout, job_location_label,
        job_advertiser_id, request_token
    ) VALUES (
        %(job_id)s, %(job_title)s, %(company)s, %(company_logo_url)s,
        %(location)s, %(area)s, %(suburb)s, %(work_type)s,
        %(work_arrangement)s, %(salary)s, %(listing_date)s, %(teaser)s,
        %(classification)s, %(sub_classification)s, %(is_premium)s,
        %(is_standout)s, %(job_location_label)s, %(job_advertiser_id)s,
        %(request_token)s
    )
    ON CONFLICT (job_id)
    DO UPDATE SET
        job_title = EXCLUDED.job_title,
        company = EXCLUDED.company,
        company_logo_url = EXCLUDED.company_logo_url,
        location = EXCLUDED.location,
        area = EXCLUDED.area,
        suburb = EXCLUDED.suburb,
        work_type = EXCLUDED.work_type,
        work_arrangement = EXCLUDED.work_arrangement,
        salary = EXCLUDED.salary,
        listing_date = EXCLUDED.listing_date,
        teaser = EXCLUDED.teaser,
        classification = EXCLUDED.classification,
        sub_classification = EXCLUDED.sub_classification,
        is_premium = EXCLUDED.is_premium,
        is_standout = EXCLUDED.is_standout,
        job_location_label = EXCLUDED.job_location_label,
        job_advertiser_id = EXCLUDED.job_advertiser_id,
        request_token = EXCLUDED.request_token;
    """

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    for job in all_jobs:
        cursor.execute(insert_sql, job)
    connection.commit()
    cursor.close()
    connection.close()
    LOGGER.info("Data loaded into PostgreSQL successfully.")
