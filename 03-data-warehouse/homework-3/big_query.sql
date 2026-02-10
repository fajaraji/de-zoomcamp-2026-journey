-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://fajar-de-zoomcamp-hw3-2026/yellow_tripdata_2024-*.parquet']
);

-- Check external yellow data
SELECT COUNT(*) FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned` AS
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`;

-- Count the distinct number of PULocationIDs for external table
SELECT DISTINCT(PULocationID) FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`;

-- Count the distinct number of PULocationIDs for non partitioned table
SELECT DISTINCT(PULocationID) FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Retrieve the PULocationID from the non partitioned table
SELECT PULocationID FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Retrieve the PULocationID, DOLocationID from the non partitioned table
SELECT PULocationID, DOLocationID FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Check records that have a fare_amount of 0
SELECT COUNT(*) FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`
WHERE fare_amount = 0;

-- Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`;

SELECT DISTINCT(VendorID) as trips
FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID) as trips
FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- 
SELECT COUNT(*) FROM `de-zoomcamp-2026-01.zoomcamp.yellow_tripdata_2024_non_partitioned`;