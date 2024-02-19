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