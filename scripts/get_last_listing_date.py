# scripts/get_last_listing_date.py

from datetime import datetime
from typing import Optional

from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from scripts.models import DB_CONFIG, LOGGER


@task
def get_last_listing_date() -> Optional[str]:
    """
    Retrieves the latest listing_date from the job_data table in the database.
    Returns the date as a string in the format 'YYYY-MM-DD'.
    If no data is found, returns None.
    """
    pg_hook = PostgresHook(postgres_conn_id=DB_CONFIG['connection_id'])
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    query = "SELECT MAX(listing_date) FROM job_data;"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    max_date = result[0] if result and result[0] else None
    if max_date:
        max_date_str = max_date.strftime("%Y-%m-%d")
        LOGGER.info(f"Last listing_date in database: {max_date_str}")
        return max_date_str
    else:
        LOGGER.info("No listing_date found in database.")
        return None
