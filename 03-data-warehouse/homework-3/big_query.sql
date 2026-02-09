-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-01.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'CSV',
  uris = 'gs://fajar-de-zoomcamp-hw3-2026/yellow_tripdata_2024-*.parquet',
  skip_leading_rows = 1
);

