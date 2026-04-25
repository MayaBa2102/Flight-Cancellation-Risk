from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="schedule_dag",
    description="Fetch flight schedules from AeroDataBox (RapidAPI) → Kafka → PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 19),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "schedule"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_schedule = BashOperator(
        task_id="fetch_schedule",
        bash_command="python /opt/airflow/scripts/fetch_schedule.py",
        doc_md="""
        Calls AeroDataBox API (via RapidAPI) for today + SCHEDULE_DAYS_AHEAD days.
        Publishes each flight to Kafka topic: raw-flights.
        Writes /app/data/schedule_{date}/raw_flights.json.
        """,
    )

    load_schedule_pg = BashOperator(
        task_id="load_schedule_pg",
        bash_command="python /opt/airflow/scripts/load_schedule_pg.py",
        doc_md="""
        Reads raw_flights.json and upserts into PostgreSQL flights_schedule table.
        Uses ON CONFLICT DO NOTHING — safe to re-run.
        """,
    )

    done = EmptyOperator(task_id="done")

    start >> fetch_schedule >> load_schedule_pg >> done