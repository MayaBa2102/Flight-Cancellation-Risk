from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="risk_dag",
    description="Refresh weather forecasts and re-score flight risk every 30 min",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 4, 19),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "risk"],
) as dag:

    start = EmptyOperator(task_id="start")

    enrich_weather = BashOperator(
        task_id="enrich_weather",
        bash_command="python /opt/airflow/scripts/enrich_weather.py",
        doc_md="""
        Reads flights_schedule from PostgreSQL.
        Calls Open-Meteo ONCE per unique airport (deduplication).
        Matches each flight's scheduled hour to the forecast.
        Publishes to Kafka topic: enriched-flights.
        Writes /app/data/risk_{ts}/enriched_flights.json.
        """,
    )

    transform_risk = BashOperator(
        task_id="transform_risk",
        bash_command="python /opt/airflow/scripts/transform_risk.py",
        doc_md="""
        Reads enriched_flights.json.
        Scores risk for both departure and arrival airports.
        Worst score wins. Writes risk.json.
        """,
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="python /opt/airflow/scripts/load_postgres.py",
        doc_md="""
        Upserts risk.json into PostgreSQL flight_risk table.
        ON CONFLICT DO UPDATE — always shows latest risk score.
        """,
    )

    load_minio = BashOperator(
        task_id="load_minio",
        bash_command="python /opt/airflow/scripts/load_minio.py",
        doc_md="""
        Uploads enriched_flights.json and risk.json to MinIO.
        Hive-style partitioning: year=/month=/day=/hour=/
        Append-only — historical archive, nothing ever deleted.
        """,
    )

    done = EmptyOperator(task_id="done")

    start >> enrich_weather >> transform_risk >> [load_postgres, load_minio] >> done