# Week 3 Homework
## _The following big query statements were created and executed_

### SETUP:
 - Create an external table using the Green Taxi Trip Records Data for 2022.
 - Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
 - It was very unlcear but you had to get the parquet data from a website into your CGP bucket. I have included a sample mage pipeline that ingests and exports the data as a preliminary step to the questions

#### The Data Loader block:
```
import pandas as pd
import numpy as np

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

URLS = [
    f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{x}.parquet'
      if x < 10 
      else f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{x}.parquet'
      
      for x in range(1, 13)
    ]

@data_loader
def load_data(*args, **kwargs):

    taxi_dtypes = {
                        'VendorID': pd.Int64Dtype(),
                        'passenger_count': pd.Int64Dtype(),
                        'trip_distance': float,
                        'lpep_pickup_datetime': np.datetime64(),
                        'lpep_dropoff_datetime': np.datetime64(),
                        'RatecodeID':pd.Int64Dtype(),
                        'store_and_fwd_flag':str,
                        'PULocationID':pd.Int64Dtype(),
                        'DOLocationID':pd.Int64Dtype(),
                        'payment_type': pd.Int64Dtype(),
                        'fare_amount': float,
                        'extra':float,
                        'mta_tax':float,
                        'tip_amount':float,
                        'tolls_amount':float,
                        'ehail_fee':float,
                        'improvement_surcharge':float,
                        'total_amount':float,
                        'trip_type':float,
                        'congestion_surcharge':float
                    }

    # Define your additional arguments for pd.read_csv
    additional_args = {
                    'use_nullable_dtypes': taxi_dtypes,
                    }


    dfs = list(map(lambda x: pd.read_parquet(x, **additional_args), URLS))

    # Combine the list of DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for col in parse_dates:
        combined_df[col] = combined_df[col].dt.date
        #combined_df[col] = pd.to_datetime(combined_df[col])
    # Selecting date columns
    #res = combined_df.select_dtypes(include=[np.datetime64])
    #print(res)
    return combined_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```

#### The Data Exporter block:
```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-zoomcamp-casper-2024'
    object_key = 'green_taxi_2020.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )

```


I had a really hard time with bigquery not being able to recognise my data as datetime so I gave up and converted my datetime columns to dates instead.


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