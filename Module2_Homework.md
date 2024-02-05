# Week 2 Homework

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green`

## Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`

Code used to generate answer

```python
def load_data_from_remote(year, month):
    """
    Preparing data loading
    """
    # Format month with leading zero
    formatted_month = f"{month:02d}"

    # Construct the URL based on the specified year and formatted month
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year:04d}-{formatted_month}.csv.gz'

    # Download the file content
    response = requests.get(url)
    file_content = BytesIO(response.content)

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'trip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float,
    }

    parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']

    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_content, compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

    return df

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    years = [2020]  # making a list to anticipate for future multiple years selection
    months = range(10, 13)  # last quarter months 10, 11 and 12, for all months in a year use range(1, 13)

    data = pd.DataFrame()

    for year in years:
        for month in months:
            df = load_data_from_remote(year, month)
            data = pd.concat([data, df])

    return data.reset_index(drop=True)
```

- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _or_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0

Code used to generate answer

```python
import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

@transformer
def transform(data, *args, **kwargs):

    data.columns = [camel_to_snake(col) for col in data.columns]

    print(f"Preprocessing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")
    print(f"Preprocessing: rows with zero trip distance: {data['trip_distance'].isin([0]).sum()}")
    # df.loc[(df[['a', 'b']] != 0).all(axis=1)]
    return data[(data['passenger_count'] != 0) | (data['trip_distance'] > 0)] # return data dropping zero passenger and zero distance trips


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() > 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() > 0, 'There are rides zero trip distance'
    assert 'vendor_id' in output.columns, 'vendor_id does not exists'
```

- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.

Code used to get answer

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
```

- Write your data as Parquet files to a bucket in GCP, partitioned by `lpep_pickup_date`. Use the `pyarrow` library!

Code used to generate answer

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/earnest-cosmos-412021-838dc43df880.json"

bucket_name = 'earnest-cosmos-412021-bucket'
project_id = 'earnest-cosmos-412021'

table_name = "green_taxi_data"

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```

- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

- 266,855 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is equal to 0 _or_ the trip distance is equal to zero, how many rows are left?

- 139,370 rows

```sql
SELECT *
FROM mage.green_taxi
WHERE passenger_count > 0 AND trip_distance > 0
```

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

- data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

- 1 or 2

```sql
SELECT DISTINCT green_taxi.vendor_id
FROM mage.green_taxi;
```

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

- 4

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

- 96

[Mage code](https://github.com/imonem/Zoomcamp2024/tree/master/green_taxi_etl)
