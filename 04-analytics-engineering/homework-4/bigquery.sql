-- ========================================
-- 1. SETUP: CREATE NATIVE TABLES FROM GCS 
-- ========================================

-- GREEN TAXI
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.ext_green_tmp`
OPTIONS (format = 'CSV', uris = ['gs://fajar-de-zoomcamp-hw4-2026/green_tripdata_*.csv.gz'], compression = 'GZIP', skip_leading_rows = 1);
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.green_tripdata` AS SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.ext_green_tmp`;
DROP TABLE `de-zoomcamp-2026-01.zoomcamp.ext_green_tmp`;

-- YELLOW TAXI
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.ext_yellow_tmp`
OPTIONS (format = 'CSV', uris = ['gs://fajar-de-zoomcamp-hw4-2026/yellow_tripdata_*.csv.gz'], compression = 'GZIP', skip_leading_rows = 1);
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata` AS SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.ext_yellow_tmp`;
DROP TABLE `de-zoomcamp-2026-01.zoomcamp.ext_yellow_tmp`;

-- FHV 2019
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.ext_fhv_tmp`
OPTIONS (format = 'CSV', uris = ['gs://fajar-de-zoomcamp-hw4-2026/fhv_tripdata_2019-*.csv.gz'], compression = 'GZIP', skip_leading_rows = 1);
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.fhv_tripdata` AS SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.ext_fhv_tmp`;
DROP TABLE `de-zoomcamp-2026-01.zoomcamp.ext_fhv_tmp`;

-- =======================================================
-- 2. HOMEWORK ANSWERS
-- =======================================================

-- Question 3: Count records in fct_monthly_zone_revenue
SELECT count(*) FROM `de-zoomcamp-2026-01.dbt_fajipamungkas.fct_monthly_zone_revenue`;

-- Question 4: Best Performing Zone for Green Taxis in 2020
SELECT pickup_zone, SUM(revenue_monthly_total_amount) AS total_revenue
FROM `de-zoomcamp-2026-01.dbt_fajipamungkas.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;

-- Question 5: Green Taxi Trip Counts in October 2019
SELECT SUM(total_monthly_trips) AS total_trips
FROM `de-zoomcamp-2026-01.dbt_fajipamungkas.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND CAST(revenue_month AS STRING) LIKE '2019-10-%';

-- Question 6: Count records in stg_fhv_tripdata
SELECT count(*) FROM `de-zoomcamp-2026-01.dbt_fajipamungkas.stg_fhv_tripdata`;