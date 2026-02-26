"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted"

@bruin"""

import io
import json
import os
from datetime import date, datetime, timezone

import pandas as pd
import requests


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Generate all year-month pairs within the date range
    months = []
    current = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    while current <= end_month:
        months.append((current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    extracted_at = datetime.now(timezone.utc)
    dfs = []

    for taxi_type in taxi_types:
        for year, month in months:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = base_url + filename
            print(f"Fetching {url}")
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_parquet(io.BytesIO(response.content))
            # Normalize taxi-type-specific datetime column names so staging
            # gets a consistent schema regardless of which types are loaded
            df = df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            })
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)
