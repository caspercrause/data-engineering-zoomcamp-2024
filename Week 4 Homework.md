# Week 4 Homework
## _To ingest the required data I developed the following pipeline_:

The script takes about 10 minutes to run and uploaded the following to `BigQuery`: 
 - `107,906,519` records to the `yellow` taxi data table 
 - `8,035,161` records to the `green` taxi data table 
 - `56,731,172` records to the `For Hire Vehilces` data table

A link to that script may be found [here](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/taxi_data_ingestion.py)

## Question 1: What happens when we execute dbt build --vars '{'is_test_run':'true'}'
Since we only define these vars in our staging the answer should be that it is applied to stagig models only and not all models 

## Question2: What is the code that our CI job will run?

CI job makes sure tha development and production enironments do not break with a bad merge so it checks tests the new changes proposed by the incoming pull request and only if it has passed all of the test can you then merge the pull request

[![CI Test](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/%20testing-ci.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/%20testing-ci.png)

If all goes well:

[![CI Passed](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/passed-ci.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/passed-ci.png)

## Question3: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

After running code inside of dbt about `22.8` million rows were processed

## Question 4: What is the service that had the most rides during the month of July 2019?

To answer this I made a quick looker data studio dashboard report and counted the amount of records by month:

[![CI Passed](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/looker-data-studio.png)](https://github.com/caspercrause/data-engineering-zoomcamp-2024/blob/master/04-analytics-engineering/looker-data-studio.png)