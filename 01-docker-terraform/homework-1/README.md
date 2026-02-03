Here's the link for all of the questions :
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/01-docker-terraform/homework.md

Below are the additional solution (SQL or shell commands and not code): 
# Question 1
docker run -it --entrypoint=bash python:3.13
pip --version

# Question 3 
uv sync
docker build -t taxi_ingest:uv .
docker run -it \
  --network=homework-1_default \
  taxi_ingest:uv \
  --host=pgdatabase

SELECT COUNT(1) AS total
FROM green_taxi_data
WHERE trip_distance <= 1
    AND lpep_pickup_datetime >= '2025-11-01 00:00:00'
    AND lpep_pickup_datetime < '2025-12-01 00:00:00'

# Question 4
SELECT 
    lpep_pickup_datetime::DATE AS pickup_day, 
    MAX(trip_distance) AS max_distance 
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY 1
ORDER BY 2  DESC
LIMIT 5

# Question 5
SELECT 
    z."Zone", 
    SUM(g.total_amount) AS totalamount
FROM zones AS z
JOIN green_taxi_data AS g
ON z."LocationID" = g."PULocationID"
WHERE g.lpep_pickup_datetime::DATE = '2025-11-18'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5

# Question 6
SELECT 
    z_do."Zone" AS dropoff_zone, 
    MAX(g.tip_amount) AS max_tip
FROM green_taxi_data AS g
JOIN zones AS z_pu
    ON z_pu."LocationID" = g."PULocationID"
JOIN zones AS z_do 
    ON z_do."LocationID" = g."DOLocationID"
WHERE z_pu."Zone" = 'East Harlem North'
    AND g.lpep_pickup_datetime >= '2025-11-01 00:00:00'
    AND g.lpep_pickup_datetime < '2025-12-01 00:00:00'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5

# Question 7
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/01-docker-terraform/terraform/terraform/terraform_with_variables/main.tf
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/01-docker-terraform/terraform/terraform/terraform_with_variables/variables.tf
terraform init
terraform plan
terraform apply
terraform destroy