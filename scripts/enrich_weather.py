from __future__ import annotations

import json
import os
from datetime import datetime, timezone
import pandas as pd

import openmeteo_requests
import psycopg2
import requests_cache
from confluent_kafka import Producer
from dotenv import load_dotenv
from retry_requests import retry

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def make_openmeteo_client():
    cache_session = requests_cache.CachedSession(".weather_cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def fetch_forecast(client, lat: float, lon: float) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["wind_speed_10m", "rain", "showers"],
        "forecast_days": 3,
        "wind_speed_unit": "kmh",
    }
    responses = client.weather_api(url, params=params)
    hourly = responses[0].Hourly()

    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    forecast = {}
    for i, t in enumerate(times):
        hour_key = t.strftime("%Y-%m-%dT%H:00")
        forecast[hour_key] = {
            "wind_speed_kmh": float(hourly.Variables(0).ValuesAsNumpy()[i]),
            "rain_mm_h":      float(hourly.Variables(1).ValuesAsNumpy()[i]),
            "showers_mm_h":   float(hourly.Variables(2).ValuesAsNumpy()[i]),
        }
    return forecast

def get_weather_at_time(forecast: dict, scheduled_time_str: str) -> dict:
    if not scheduled_time_str:
        return {"wind_speed_kmh": 0.0, "rain_mm_h": 0.0, "showers_mm_h": 0.0}

    dt = datetime.fromisoformat(scheduled_time_str.replace("Z", "+00:00"))
    hour_key = dt.strftime("%Y-%m-%dT%H:00")

    return forecast.get(hour_key, {"wind_speed_kmh": 0.0, "rain_mm_h": 0.0, "showers_mm_h": 0.0})

def load_schedule_from_pg(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT flight_iata, airline, dep_iata, dep_lat, dep_lon,
               arr_iata, arr_lat, arr_lon, scheduled_dep, scheduled_arr
        FROM flights_schedule
        WHERE scheduled_dep >= NOW()
        ORDER BY scheduled_dep;
    """)
    rows = cur.fetchall()
    cur.close()

    columns = ["flight_iata", "airline", "dep_iata", "dep_lat", "dep_lon",
               "arr_iata", "arr_lat", "arr_lon", "scheduled_dep", "scheduled_arr"]
    return [dict(zip(columns, row)) for row in rows]

def main():
    conn = get_connection()
    flights = load_schedule_from_pg(conn)
    conn.close()

    if not flights:
        print("No upcoming flights found in flights_schedule. "
              "Run fetch_schedule.py + load_schedule_pg.py first.")
        return

    client = make_openmeteo_client()

    airports = {}
    for f in flights:
        airports[f["dep_iata"]] = (f["dep_lat"], f["dep_lon"])
        airports[f["arr_iata"]] = (f["arr_lat"], f["arr_lon"])

    print(f"  Fetching weather for {len(airports)} unique airports...")
    forecasts = {}
    for iata, (lat, lon) in airports.items():
        forecasts[iata] = fetch_forecast(client, lat, lon)
        print(f"    → {iata} ({lat:.2f}, {lon:.2f}) OK")

    enriched = []
    for f in flights:
        dep_time = str(f["scheduled_dep"]) if f["scheduled_dep"] else None
        arr_time = str(f["scheduled_arr"]) if f["scheduled_arr"] else None

        dep_wx = get_weather_at_time(forecasts[f["dep_iata"]], dep_time)
        arr_wx = get_weather_at_time(forecasts[f["arr_iata"]], arr_time)

        record = {
            "flight_iata":    f["flight_iata"],
            "airline":        f["airline"],
            "dep_iata":       f["dep_iata"],
            "arr_iata":       f["arr_iata"],
            "scheduled_dep":  dep_time,
            "scheduled_arr":  arr_time,
            "dep_wind_kmh":   dep_wx["wind_speed_kmh"],
            "dep_rain_mm_h":  dep_wx["rain_mm_h"] + dep_wx["showers_mm_h"],
            "arr_wind_kmh":   arr_wx["wind_speed_kmh"],
            "arr_rain_mm_h":  arr_wx["rain_mm_h"] + arr_wx["showers_mm_h"],
        }
        enriched.append(record)

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "client.id": "enrich-weather",
    })
    for record in enriched:
        producer.produce(
            topic="enriched-flights",
            key=record["flight_iata"],
            value=json.dumps(record, default=str),
        )
    producer.flush()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_dir = f"/app/data/risk_{run_ts}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/enriched_flights.json"
    with open(out_path, "w") as f_out:
        for record in enriched:
            f_out.write(json.dumps(record, default=str) + "\n")

    print(f"\nEnriched {len(enriched)} flights using weather data for "
          f"{len(airports)} unique airports. "
          f"Published {len(enriched)} messages to Kafka topic enriched-flights.")
    print(f"  Output: {out_path}")

if __name__ == "__main__":
    main()