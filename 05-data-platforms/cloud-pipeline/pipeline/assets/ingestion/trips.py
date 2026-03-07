"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: int64
  - name: pickup_datetime
    type: string
  - name: dropoff_datetime
    type: string
  - name: passenger_count
    type: int64
  - name: trip_distance
    type: float64
  - name: rate_code_id
    type: int64
  - name: store_and_fwd_flag
    type: string
  - name: pu_location_id
    type: int64
  - name: do_location_id
    type: int64
  - name: payment_type
    type: int64
  - name: fare_amount
    type: float64
  - name: extra
    type: float64
  - name: mta_tax
    type: float64
  - name: tip_amount
    type: float64
  - name: tolls_amount
    type: float64
  - name: improvement_surcharge
    type: float64
  - name: total_amount
    type: float64
  - name: congestion_surcharge
    type: float64
  - name: airport_fee
    type: float64
  - name: taxi_type
    type: string
  - name: extracted_at
    type: string

@bruin"""

import os
import json
from datetime import datetime
import pandas as pd
import requests
from io import BytesIO

def materialize():
    start_date_str = os.getenv('BRUIN_START_DATE')
    end_date_str = os.getenv('BRUIN_END_DATE')
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE are required")

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    bruin_vars = os.getenv('BRUIN_VARS', '{}')
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get('taxi_types', ['yellow'])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    all_dfs = []

    column_mapping = {
        'vendorid': 'vendor_id',
        'tpep_pickup_datetime': 'pickup_datetime',
        'lpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'ratecodeid': 'rate_code_id',
        'pulocationid': 'pu_location_id',
        'dolocationid': 'do_location_id',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'congestion_surcharge': 'congestion_surcharge',
        'airport_fee': 'airport_fee'
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive'
    }

    current_date = start_date
    while current_date <= end_date:
        y, m = current_date.year, current_date.month

        for t_type in taxi_types:
            filename = f"{t_type}_tripdata_{y}-{m:02d}.parquet"
            url = f"{base_url}{filename}"

            try:
                print(f"Fetching: {url}")
                res = requests.get(url, headers=headers, timeout=60)
                res.raise_for_status()

                df = pd.read_parquet(BytesIO(res.content))
                
                df.columns = df.columns.str.lower()
                df = df.rename(columns=column_mapping)
                
                if 'pickup_datetime' in df.columns:
                    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
                if 'dropoff_datetime' in df.columns:
                    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

                valid_columns = [
                    'vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
                    'trip_distance', 'rate_code_id', 'store_and_fwd_flag', 'pu_location_id',
                    'do_location_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                    'congestion_surcharge', 'airport_fee'
                ]
                
                for col in valid_columns:
                    if col not in df.columns:
                        df[col] = None
                
                df = df[valid_columns].copy()
                df['taxi_type'] = t_type
                df['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                all_dfs.append(df)

            except Exception as e:
                print(f"Skipping {filename}: {e}")

        if m == 12:
            current_date = current_date.replace(year=y + 1, month=1)
        else:
            current_date = current_date.replace(month=m + 1)

    if not all_dfs:
        raise ValueError("No data found! Check if NYC Taxi server is blocking Bruin Cloud IP.")

    return pd.concat(all_dfs, ignore_index=True)