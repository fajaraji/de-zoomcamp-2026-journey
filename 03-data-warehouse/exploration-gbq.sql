-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-fajar/green_tripdata_2019-*.csv', 'gs://kestra-fajar/green_tripdata_2020-*.csv'],
  skip_leading_rows = 1
);

-- Check yellow trip data
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata` limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_non_partitioned` AS
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_partitioned`
PARTITION BY 
  DATE(tpep_pickup_datetime) AS 
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata`;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'external_yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata`;

-- Query scans 123 MB
SELECT count(*) as trips
FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 116 MB
SELECT count(*) as trips
FROM `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-31'
  AND VendorID=1;