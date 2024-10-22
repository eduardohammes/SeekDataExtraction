FROM apache/airflow:2.2.5-python3.8

# Install additional dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY config/ /opt/airflow/config/
COPY tests/ /opt/airflow/tests/

# Set environment variables
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
