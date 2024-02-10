# Week 2 Homework
## _The following is an example of the code deployed in mage_

### Data Loader:
```python
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):

    URLS = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz']



    taxi_dtypes = {
                        'VendorID': pd.Int64Dtype(),
                        'passenger_count': pd.Int64Dtype(),
                        'trip_distance': float,
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

        # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


    # Define your additional arguments for pd.read_csv
    additional_args = {'sep': ',', 
                    'compression': 'gzip',
                    'dtype': taxi_dtypes,
                    'parse_dates': parse_dates
                    }


    dfs = list(map(lambda x: pd.read_csv(x, **additional_args), URLS))

    # Combine the list of DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    # Specify your data loading logic here

    return combined_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```

### The transformer block:

```python
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # remove data where passanger count is 0
    num_rows = len(data[data[['trip_distance', 'passenger_count']].isin([0, 0]).any(axis=1)])
    print(f'*** Removed {num_rows} from raw data containing false data\n***Rows containing trips where the passenger count or trip distance is 0')
    
    # remove rows where either trip distance or passenger count is 0
    data = data[~data[['trip_distance', 'passenger_count']].isin([0, 0]).any(axis=1)]
    #Remove rows with NaN values in passenger_count or trip_distance
    data = data.dropna(subset=['passenger_count'])
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data.lpep_pickup_datetime.dt.date

    mycols = {
        'VendorID' : 'vendor_id',
        'PULocationID' : 'pu_location_id',
        'DOLocationID' : 'do_location_id',
        'RatecodeID' : 'rate_code_id'
    }
    data.rename(columns=mycols, inplace=True)
    return data
    #return data[data.passenger_count > 0]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # Get existing values dynamically
    existing_vendor_ids = output['vendor_id'].unique()
    print(existing_vendor_ids)
    # Your specified conditions
    assert all(output['vendor_id'].isin(existing_vendor_ids)), "Assertion Error: vendor_id should be one of the existing values"
    assert all(output['passenger_count'] > 0), "Assertion Error: passenger_count should be greater than 0"
    assert all(output['trip_distance'] > 0), "Assertion Error: trip_distance should be greater than 0"
```

### The data exporter block

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os
# Tell pyarrow where our credentials are located:
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/oAuth.json'
bucket_name = 'mage-zoomcamp-casper-2024'
project_id  = 'project-ast-292014'
table_name  = 'green_taxi_data'

# From here pyarrow will handle the partitioning
root_path = f'{bucket_name}/{table_name}'

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, **kwargs):
    # For pyarrow you need to define what is called a pyarrow table
    table = pa.Table.from_pandas(data)
    # We need a filesystem object
    gcs = pa.fs.GcsFileSystem() # Will authorize using our envirnment variable automatically

    # Write data
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'], # Must be a list object
        filesystem = gcs
    )

```
