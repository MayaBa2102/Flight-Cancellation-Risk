from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta

import time

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

AIRPORT_COORDS = {
    "JFK": (40.6413, -73.7781),  "LAX": (33.9425, -118.4081),
    "ORD": (41.9742, -87.9073),  "ATL": (33.6407,  -84.4277),
    "DFW": (32.8998,  -97.0403), "DEN": (39.8561, -104.6737),
    "SFO": (37.6213, -122.3790), "SEA": (47.4502, -122.3088),
    "MIA": (25.7959,  -80.2870), "BOS": (42.3656,  -71.0096),
    "LHR": (51.4700,   -0.4543), "CDG": (49.0097,    2.5479),
    "AMS": (52.3086,    4.7639), "FRA": (50.0379,    8.5622),
    "MAD": (40.4983,   -3.5676), "BCN": (41.2971,    2.0785),
    "MUC": (48.3537,   11.7750), "FCO": (41.8003,   12.2389),
    "ZRH": (47.4647,    8.5492), "VIE": (48.1103,   16.5697),
    "DXB": (25.2532,   55.3657), "SIN": ( 1.3644,  103.9915),
    "HKG": (22.3080,  113.9185), "NRT": (35.7720,  140.3929),
    "SYD": (-33.9461, 151.1772), "GRU": (-23.4356,  -46.4731),
    "YYZ": (43.6777,  -79.6248), "MEX": (19.4363,   -99.0721),
    "TLV": (32.0055,   34.8854), "IST": (41.2753,   28.7519),
    "LAS": (36.0840, -115.1537), "PHX": (33.4373, -112.0078),
    "MCO": (28.4294,  -81.3089), "EWR": (40.6895,  -74.1745),
    "IAH": (29.9902,  -95.3368), "SLC": (40.7884, -111.9778),
    "MSP": (44.8848,  -93.2223), "DTW": (42.2124,  -83.3534),
    "PHL": (39.8719,  -75.2411), "CLT": (35.2140,  -80.9431),
}

ICAO_TO_IATA = {
    "KJFK": "JFK", "KLAX": "LAX", "KATL": "ATL", "KORD": "ORD",
    "KDFW": "DFW", "KDEN": "DEN", "KSFO": "SFO", "KMIA": "MIA",
    "EGLL": "LHR", "LFPG": "CDG", "EHAM": "AMS", "EDDF": "FRA",
    "LEMD": "MAD", "LEBL": "BCN", "EDDM": "MUC", "LIRF": "FCO",
    "OMDB": "DXB", "WSSS": "SIN", "VHHH": "HKG", "RJAA": "NRT",
}

def make_producer():
    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "client.id": "fetch-schedule",
    })

def fetch_departures(api_key: str, icao: str, flight_date: str) -> list[dict]:
    # we use a 12-hour window (06:00–18:00 UTC) to stay within free tier limits
    from_dt = f"{flight_date}T06:00"
    to_dt   = f"{flight_date}T18:00"
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{from_dt}/{to_dt}"

    headers = {
        "x-rapidapi-host": "aerodatabox.p.rapidapi.com",
        "x-rapidapi-key":  api_key,
    }
    params = {
        "direction":       "Departure",
        "withLeg":         "true",
        "withCodeshared":  "false",
        "withCargo":       "false",
        "withPrivate":     "false",
        "withLocation":    "false",
    }

    response = requests.get(url, headers=headers, params=params, timeout=15)

    if response.status_code == 204:
        return []

    response.raise_for_status()
    return response.json().get("departures", [])

def parse_flight(raw: dict, dep_icao: str, flight_date: str) -> dict | None:
    if raw.get("isCargo"):
        return None

    arr = raw.get("arrival", {}).get("airport", {})
    arr_iata = arr.get("iata", "").upper().strip()
    if not arr_iata or arr_iata not in AIRPORT_COORDS:
        return None

    dep_iata = ICAO_TO_IATA.get(dep_icao.upper(), "")
    if not dep_iata or dep_iata not in AIRPORT_COORDS:
        return None

    flight_number = raw.get("number", "").replace(" ", "").upper().strip()
    if not flight_number:
        return None

    dep_time = raw.get("departure", {}).get("scheduledTime", {}).get("utc", "")
    arr_time = raw.get("arrival",   {}).get("scheduledTime", {}).get("utc", "")

    dep_lat, dep_lon = AIRPORT_COORDS[dep_iata]
    arr_lat, arr_lon = AIRPORT_COORDS[arr_iata]

    return {
        "flight_iata":   flight_number,
        "airline":       raw.get("airline", {}).get("name", "Unknown"),
        "dep_iata":      dep_iata,
        "dep_lat":       dep_lat,
        "dep_lon":       dep_lon,
        "arr_iata":      arr_iata,
        "arr_lat":       arr_lat,
        "arr_lon":       arr_lon,
        "scheduled_dep": dep_time,
        "scheduled_arr": arr_time,
        "fetched_date":  flight_date,
    }

def main():
    api_key     = os.environ["AERODATABOX_API_KEY"]
    days_ahead  = int(os.environ.get("SCHEDULE_DAYS_AHEAD", "1"))
    airports    = [a.strip() for a in os.environ.get("AERODATABOX_AIRPORTS", "KJFK,EGLL,EDDF,OMDB,KATL").split(",")]
    today       = date.today()
    dates       = [str(today + timedelta(days=i)) for i in range(days_ahead + 1)]

    producer     = make_producer()
    all_flights  = []
    api_calls    = 0

    for flight_date in dates:
        for icao in airports:
            print(f"  Fetching {icao} departures on {flight_date}...")
            raw_list = fetch_departures(api_key, icao, flight_date)
            api_calls += 1

            parsed = [parse_flight(r, icao, flight_date) for r in raw_list]
            valid  = [f for f in parsed if f is not None]
            print(f"    → {len(raw_list)} returned, {len(valid)} usable")
            all_flights.extend(valid)
            time.sleep(1)

    if not all_flights:
        print("No flights fetched. Check your API key and airport codes.")
        sys.exit(1)

    seen = set()
    unique_flights = []
    for f in all_flights:
        key = (f["flight_iata"], f["scheduled_dep"])
        if key not in seen:
            seen.add(key)
            unique_flights.append(f)

    for flight in unique_flights:
        producer.produce(
            topic="raw-flights",
            key=flight["flight_iata"],
            value=json.dumps(flight),
        )
    producer.flush()

    out_dir = f"/app/data/schedule_{today}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/raw_flights.json"
    with open(out_path, "w") as f:
        for flight in unique_flights:
            f.write(json.dumps(flight) + "\n")

    print(f"\nFetched {len(unique_flights)} flights from {len(airports)} airports "
          f"across {len(dates)} dates ({api_calls} API calls used).")
    print(f"  Published {len(unique_flights)} messages to Kafka topic raw-flights.")
    print(f"  Output: {out_path}")

if __name__ == "__main__":
    main()