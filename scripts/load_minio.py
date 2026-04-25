import json
import os
import sys
from datetime import datetime, timezone
from glob import glob

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )

def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"  Created bucket: {bucket}")

def partition_prefix(dt: datetime) -> str:
    return (f"year={dt.year}/month={dt.month:02d}/"
            f"day={dt.day:02d}/hour={dt.hour:02d}/")

def find_latest_run_dir() -> str:
    dirs = sorted(glob("/app/data/risk_*"))
    if not dirs:
        raise FileNotFoundError("No risk_* directory found. Run the pipeline first.")
    return dirs[-1]

def main():
    run_dir = find_latest_run_dir()
    run_ts_str = os.path.basename(run_dir).replace("risk_", "")
    run_dt = datetime.strptime(run_ts_str, "%Y%m%d_%H%M").replace(tzinfo=timezone.utc)
    prefix = partition_prefix(run_dt)
    ts_tag = run_ts_str.replace("_", "")

    files = {
        "enriched_flights.json": f"enriched-flights/{prefix}enriched_{ts_tag}.json",
        "risk.json":             f"risk-assessments/{prefix}risk_{ts_tag}.json",
    }

    from datetime import date
    schedule_path = f"/app/data/schedule_{date.today()}/raw_flights.json"
    if os.path.exists(schedule_path):
        sched_prefix = f"year={run_dt.year}/month={run_dt.month:02d}/day={run_dt.day:02d}/"
        files[schedule_path] = f"raw-schedules/{sched_prefix}schedules_{run_dt.strftime('%Y%m%d')}.json"

    bucket = os.environ["MINIO_BUCKET"]
    s3 = get_s3_client()
    ensure_bucket(s3, bucket)

    uploaded = 0
    for local_name, s3_key in files.items():
        local_path = local_name if os.path.isabs(local_name) else f"{run_dir}/{local_name}"
        if not os.path.exists(local_path):
            print(f"Skipping missing file: {local_path}")
            continue
        s3.upload_file(local_path, bucket, s3_key)
        print(f"  → s3://{bucket}/{s3_key}")
        uploaded += 1

    if uploaded == 0:
        print("No files were uploaded.")
        sys.exit(1)

    print(f"\nUploaded {uploaded} files to s3://{bucket}/")

if __name__ == "__main__":
    main()