-- Question 3 (Yellow Taxi 2020)
SELECT COUNT(*) 
FROM `project_id_kamu.dataset_kamu.yellow_tripdata`
WHERE filename LIKE '%2020%';

-- Question 4 (Green Taxi 2020)
SELECT COUNT(*) 
FROM `project_id_kamu.dataset_kamu.green_tripdata`
WHERE filename LIKE '%2020%';

-- Question 5 (Yellow Taxi Maret 2021)
SELECT COUNT(*) 
FROM `project_id_kamu.dataset_kamu.yellow_tripdata`
WHERE filename LIKE '%2021-03%';