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
    description: "Yellow/green taxi bucket"
  - name: pickup_datetime
    type: string
    description: "Time when trip started (YYYY-MM-DD HH:MM:SS)"
  - name: dropoff_datetime
    type: string
    description: "Time when trip ended (YYYY-MM-DD HH:MM:SS)"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: payment_type_id
    type: integer
    description: "Lookup key for payment type"
  - name: extracted_at
    type: string
    description: "Timestamp when data was fetched (YYYY-MM-DD HH:MM:SS)"

@bruin"""

import os

# --- DISPEL DEBUFF ---
if 'TZDIR' in os.environ:
    del os.environ['TZDIR']

import io
import json
import pandas as pd
import requests
from datetime import datetime

def materialize():
    start_str = os.getenv("BRUIN_START_DATE")
    end_str = os.getenv("BRUIN_END_DATE")
    
    if not start_str or not end_str:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set by Bruin")

    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')

    vars_json = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(vars_json)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    frames = []
    
    # Mapping untuk merapikan nama kolom yang beda-beda
    column_mapping = {
        'tpep_pickup_datetime': 'pickup_datetime',
        'lpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'payment_type': 'payment_type_id'
    }

    # Logika looping bulan yang jauh lebih aman dari date_range
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        y, m = current_date.year, current_date.month

        for taxi in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{y:04d}-{m:02d}.parquet"
            try:
                print(f"Fetching: {url}")
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                
                df = pd.read_parquet(io.BytesIO(resp.content))
                df.columns = df.columns.str.lower()
                df = df.rename(columns=column_mapping)
                df["taxi_type"] = taxi
                
                frames.append(df)
            except Exception as e:
                print(f"Warning: Could not fetch {url}: {e}")
        
        # Pindah ke bulan selanjutnya
        if m == 12:
            current_date = current_date.replace(year=y + 1, month=1)
        else:
            current_date = current_date.replace(month=m + 1)

    if not frames:
        raise ValueError("No data found for the given dates!")

    # Gabungkan semua data
    result = pd.concat(frames, ignore_index=True)
    result["extracted_at"] = datetime.now()

    
    # Konversi SEMUA kolom waktu menjadi teks murni secara paksa
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Filter kolom agar HANYA mengembalikan 7 kolom yang diminta YAML
    expected_columns = [
        'taxi_type', 'pickup_datetime', 'dropoff_datetime', 
        'passenger_count', 'trip_distance', 'payment_type_id', 'extracted_at'
    ]
    
    # Beri nilai None jika ada kolom yang tidak ditemukan di Parquet
    for col in expected_columns:
        if col not in result.columns:
            result[col] = None

    return result[expected_columns].copy()