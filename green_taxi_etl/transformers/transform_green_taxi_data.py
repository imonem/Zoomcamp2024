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

