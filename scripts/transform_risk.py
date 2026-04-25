from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from glob import glob

def score_risk(wind_speed_kmh: float, rain_mm_h: float) -> tuple[str, str]:
    reasons = []
    if wind_speed_kmh > 50:
        reasons.append(f"wind {wind_speed_kmh:.1f} km/h")
    if rain_mm_h > 5:
        reasons.append(f"rain {rain_mm_h:.1f} mm/h")

    if reasons:
        return "high", "; ".join(reasons)
    if wind_speed_kmh > 30:
        return "medium", f"wind {wind_speed_kmh:.1f} km/h"
    return "low", ""

RISK_ORDER = {"low": 0, "medium": 1, "high": 2}

def assess_flight(flight: dict) -> dict:
    dep_level, dep_reason = score_risk(flight["dep_wind_kmh"], flight["dep_rain_mm_h"])
    arr_level, arr_reason = score_risk(flight["arr_wind_kmh"], flight["arr_rain_mm_h"])

    if RISK_ORDER[arr_level] > RISK_ORDER[dep_level]:
        risk_level = arr_level
        risk_reason = arr_reason
        risk_airport = "ARR"
    elif dep_reason:
        risk_level = dep_level
        risk_reason = dep_reason
        risk_airport = "DEP"
    else:
        risk_level = dep_level
        risk_reason = ""
        risk_airport = "DEP"

    return {**flight, "risk_level": risk_level, "risk_reason": risk_reason, "risk_airport": risk_airport}

def find_latest_run_dir() -> str:
    dirs = sorted(glob("/app/data/risk_*"))
    if not dirs:
        raise FileNotFoundError(
            "No risk_* directory found under /app/data/. "
            "Run enrich_weather.py first."
        )
    return dirs[-1]

def main():
    run_dir = find_latest_run_dir()
    in_path = f"{run_dir}/enriched_flights.json"

    if not os.path.exists(in_path):
        print(f"File not found: {in_path}")
        sys.exit(1)

    with open(in_path) as f:
        flights = [json.loads(line) for line in f if line.strip()]

    assessed = [assess_flight(f) for f in flights]

    out_path = f"{run_dir}/risk.json"
    with open(out_path, "w") as f_out:
        for record in assessed:
            f_out.write(json.dumps(record, default=str) + "\n")

    counts = {"high": 0, "medium": 0, "low": 0}
    for r in assessed:
        counts[r["risk_level"]] += 1

    print(f"\nScored {len(assessed)} flights: "
          f"{counts['high']} HIGH, {counts['medium']} MEDIUM, {counts['low']} LOW.")
    print(f"  Output: {out_path}")

if __name__ == "__main__":
    main()