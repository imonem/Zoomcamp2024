from io import BytesIO
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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

# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
