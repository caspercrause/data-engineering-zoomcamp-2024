# Week 3 Homework
## _The following big query statements were created and executed_

### SETUP:
 - Create an external table using the Green Taxi Trip Records Data for 2022.
 - Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).


```SQL
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `project-ast-292014.ny_taxi.green_taxi_data_2020_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-casper-2024/green_taxi_2020.parquet']
);

```

#### Question 1: What is count of records for the 2022 Green Taxi Data?
```SQL
SELECT COUNT(1) 
FROM `project-ast-292014.ny_taxi.green_taxi_data_2020`;
```

#### Question2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

#### What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```
-- Create internal table
CREATE OR REPLACE TABLE `project-ast-292014.ny_taxi.green_taxi_data_2020_internal`
AS 
SELECT * 
FROM `project-ast-292014.ny_taxi.green_taxi_data_2020_external`;
```
Then after that run:

```SQL
SELECT DISTINCT(PULocationID) 
FROM 
`project-ast-292014.ny_taxi.green_taxi_data_2020_external`;
```

and then

```SQL
SELECT DISTINCT(PULocationID) 
FROM `project-ast-292014.ny_taxi.green_taxi_data_2020_internal`;
```

#### Question 3: How many records have a fare_amount of 0?

```SQL
SELECT COUNT(1) 
FROM `project-ast-292014.ny_taxi.green_taxi_data_2020_external`
WHERE fare_amount = 0;
```
#### Question 4:
```
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `project-ast-292014.ny_taxi.green_taxi_data_2020_external_partitioned`
PARTITION BY
  lpep_pickup_datetime
  CLUSTER BY PUlocationID AS
SELECT * FROM `project-ast-292014.ny_taxi.green_taxi_data_2020_external`;
```
#### Question 5: Write a query to retrieve the distinct PULocationID between _lpep_pickup_datetime_  06/01/2022 and 06/30/2022 (inclusive)

```
SELECT DISTINCT(PULocationID) 
FROM 
`project-ast-292014.ny_taxi.green_taxi_data_2020_internal`
WHERE 
lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) 
FROM 
`project-ast-292014.ny_taxi.green_taxi_data_2020_external_partitioned`
WHERE 
lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
```