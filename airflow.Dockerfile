FROM apache/airflow:2.8.1
USER airflow
RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    openmeteo-requests==1.2.0 \
    requests \
    requests-cache \
    retry-requests \
    pandas \
    psycopg2-binary \
    boto3 \
    python-dotenv
