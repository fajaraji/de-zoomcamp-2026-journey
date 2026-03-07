/* @bruin

name: staging.trips
type: bq.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1

@bruin */


SELECT
    t.vendor_id,
    CAST(t.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(t.dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.rate_code_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    p.payment_type_name, 
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    NULL AS airport_fee, 
    t.taxi_type,
    CAST(t.extracted_at AS TIMESTAMP) AS extracted_at
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE 
    t.vendor_id IS NOT NULL 
    AND t.pickup_datetime IS NOT NULL
    AND t.fare_amount >= 0
    AND CAST(t.pickup_datetime AS TIMESTAMP) >= CAST('{{ start_datetime }}' AS TIMESTAMP)
    AND CAST(t.pickup_datetime AS TIMESTAMP) < CAST('{{ end_datetime }}' AS TIMESTAMP)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.vendor_id, t.pickup_datetime, t.dropoff_datetime, t.pu_location_id, t.do_location_id
    ORDER BY t.extracted_at DESC
) = 1;