# Week 4 Homework
## _To ingest the required data I developed the following pipeline_:

The script takes about 10 minutes to run and uploaded the following to `BigQuery`: 
 - `107,906,519` records to the `yellow` taxi data table 
 - `8,035,161` records to the `green` taxi data table 
 - `56,731,172` records to the `For Hire Vehilces` data table

A link to that script may be found [here](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/taxi_data_ingestion.py)

## Question 1: What happens when we execute dbt build --vars '{'is_test_run':'true'}'
_Since we only define these vars in our staging the answer should be that it is applied to staging models only and not all models_

## Question2: What is the code that our CI job will run?

_CI job makes sure that development and production environments do not break with a bad merge so it tests the new changes proposed by the incoming pull request and only if it has passed all of the tests can you merge the pull request_

[![CI Test](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/%20testing-ci.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/%20testing-ci.png)

_If all goes well this is what you should see:_

[![CI Passed](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/passed-ci.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/passed-ci.png)

## Question3: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

_After running code inside of dbt about `22.8` million rows were processed_

## Question 4: What is the service that had the most rides during the month of July 2019?

_To answer this I made a quick looker data studio dashboard report and counted the amount of records by month:_

[![CI Passed](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/looker-data-studio.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/looker-data-studio.png)