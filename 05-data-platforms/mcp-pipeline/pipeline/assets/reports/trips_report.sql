/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: pickup_datetime
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: taxi_type
    type: string
    description: "Yellow or green taxi"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Human-readable payment type"
    primary_key: true
  - name: date
    type: DATE
    description: "Pickup date"
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: "Number of trips in the bucket"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    -- 1. Agregasi Waktu (Harian)
    CAST(t.pickup_datetime AS DATE) AS trip_date,
    t.taxi_type,
    
    -- 2. Tarik Nama Pembayaran (Bukan Cuma ID)
    p.payment_type_name,
    
    -- 3. Hitung Ringkasan (Total & Rata-rata)
    COUNT(*) AS trip_count,
    SUM(t.passenger_count) AS total_passengers,
    ROUND(SUM(t.trip_distance), 2) AS total_distance,
    ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance
FROM staging.trips t
-- 4. JOIN dengan tabel lookup untuk mendapatkan namanya
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type_id = p.payment_type_id
WHERE
    t.pickup_datetime IS NOT NULL
    -- Filter Jendela Waktu
    AND CAST(t.pickup_datetime AS TIMESTAMP) >= CAST('{{ start_datetime }}' AS TIMESTAMP)
    AND CAST(t.pickup_datetime AS TIMESTAMP) < CAST('{{ end_datetime }}' AS TIMESTAMP)
GROUP BY
    1, 2, 3;
