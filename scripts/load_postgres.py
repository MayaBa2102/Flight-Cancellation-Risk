import json
import os
import sys
from glob import glob

import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def find_latest_run_dir() -> str:
    dirs = sorted(glob("/app/data/risk_*"))
    if not dirs:
        raise FileNotFoundError("No risk_* directory found. Run the pipeline first.")
    return dirs[-1]

def main():
    run_dir = find_latest_run_dir()
    in_path = f"{run_dir}/risk.json"

    if not os.path.exists(in_path):
        print(f"File not found: {in_path}")
        sys.exit(1)

    with open(in_path) as f:
        records = [json.loads(line) for line in f if line.strip()]

    conn = get_connection()
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO flight_risk
            (flight_iata, airline, dep_iata, arr_iata,
             scheduled_dep, scheduled_arr,
             dep_wind_kmh, dep_rain_mm_h,
             arr_wind_kmh, arr_rain_mm_h,
             risk_level, risk_reason, risk_airport, last_updated)
        VALUES
            (%(flight_iata)s, %(airline)s, %(dep_iata)s, %(arr_iata)s,
             %(scheduled_dep)s, %(scheduled_arr)s,
             %(dep_wind_kmh)s, %(dep_rain_mm_h)s,
             %(arr_wind_kmh)s, %(arr_rain_mm_h)s,
             %(risk_level)s, %(risk_reason)s, %(risk_airport)s, NOW())
        ON CONFLICT (flight_iata, scheduled_dep)
        DO UPDATE SET
            dep_wind_kmh  = EXCLUDED.dep_wind_kmh,
            dep_rain_mm_h = EXCLUDED.dep_rain_mm_h,
            arr_wind_kmh  = EXCLUDED.arr_wind_kmh,
            arr_rain_mm_h = EXCLUDED.arr_rain_mm_h,
            risk_level    = EXCLUDED.risk_level,
            risk_reason   = EXCLUDED.risk_reason,
            risk_airport  = EXCLUDED.risk_airport,
            last_updated  = NOW();
    """

    for record in records:
        cur.execute(upsert_sql, record)

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nUpserted {len(records)} rows into flight_risk.")

if __name__ == "__main__":
    main()