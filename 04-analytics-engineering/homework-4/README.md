# Module 4 Homework: Analytics Engineering with dbt

This repository contains the completed homework for Module 4 of the Data Engineering Zoomcamp, focusing on Analytics Engineering using dbt and Google BigQuery.

All the SQL queries used to create the native tables from Google Cloud Storage and to answer the questions below can be found in the `bigquery.sql` file.

---

## Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)

If you run `dbt run --select int_trips_unioned`, what models will be built?

[ ] `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
[ ] Any model with upstream and downstream dependencies to `int_trips_unioned`
[x] `int_trips_unioned` only
[ ] `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

## Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:
```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data. What happens when you run dbt test --select fct_trips?

[ ] dbt will skip the test because the model didn't change
[x] dbt will fail the test, returning a non-zero exit code
[ ] dbt will pass the test with a warning about the new value
[ ] dbt will update the configuration to include the new value

## Question 3. Counting Records in fct_monthly_zone_revenue
After running your dbt project, query the fct_monthly_zone_revenue model. What is the count of records in the fct_monthly_zone_revenue model?

[ ] 12,998
[ ] 14,120
[x] 12,184
[ ] 15,421

## Question 4. Best Performing Zone for Green Taxis (2020)
Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020. Which zone had the highest revenue?

[x] East Harlem North
[ ] Morningside Heights
[ ] East Harlem South
[ ] Washington Heights South

## Question 5. Green Taxi Trip Counts (October 2019)
Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

[ ] 500,234
[ ] 350,891
[x] 384,624
[ ] 421,509

## Question 6. Build a Staging Model for FHV Data
Create a staging model stg_fhv_tripdata with these requirements:
- Filter out records where dispatching_base_num IS NULL
- Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)

What is the count of records in stg_fhv_tripdata?

[ ] 42,084,899
[x] 43,244,693
[ ] 22,998,722
[ ] 44,112,187