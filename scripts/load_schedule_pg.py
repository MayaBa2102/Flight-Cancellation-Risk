from __future__ import annotations

import json
import os
from datetime import date

import psycopg2
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

TOPIC = "raw-flights"
NUM_PARTITIONS = 3

def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def consume_flights() -> list[dict]:
    group_id = f"load-schedule-{date.today()}"
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":          group_id,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": "true",
        "enable.auto.commit": "true",
    })
    consumer.subscribe([TOPIC])

    flights = []
    eof_partitions: set[int] = set()
    silence_ticks = 0

    print(f"  Consuming from Kafka topic '{TOPIC}' (group: {group_id})...")
    while True:
        msg = consumer.poll(timeout=2.0)

        if msg is None:
            silence_ticks += 1
            if silence_ticks >= 5:
                break
            continue

        silence_ticks = 0

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof_partitions.add(msg.partition())
                if len(eof_partitions) >= NUM_PARTITIONS:
                    break
            else:
                raise Exception(f"Kafka error: {msg.error()}")
        else:
            flights.append(json.loads(msg.value()))

    consumer.close()
    return flights


def main():
    flights = consume_flights()

    if not flights:
        print("No messages in Kafka topic raw-flights. "
              "Run fetch_schedule.py first.")
        return

    print(f"  Consumed {len(flights)} messages from Kafka topic {TOPIC}.")

    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO flights_schedule
            (flight_iata, airline, dep_iata, dep_lat, dep_lon,
             arr_iata, arr_lat, arr_lon, scheduled_dep, scheduled_arr, fetched_date)
        VALUES
            (%(flight_iata)s, %(airline)s, %(dep_iata)s, %(dep_lat)s, %(dep_lon)s,
             %(arr_iata)s, %(arr_lat)s, %(arr_lon)s, %(scheduled_dep)s, %(scheduled_arr)s,
             %(fetched_date)s)
        ON CONFLICT (flight_iata, scheduled_dep) DO NOTHING;
    """

    inserted = 0
    for flight in flights:
        clean = {k: (v if v != "" else None) for k, v in flight.items()}
        cur.execute(insert_sql, clean)
        inserted += cur.rowcount

    conn.commit()
    skipped = len(flights) - inserted

    cur.close()
    conn.close()

    print(f"\nInserted {inserted} rows into flights_schedule "
          f"({skipped} duplicates skipped).")

if __name__ == "__main__":
    main()