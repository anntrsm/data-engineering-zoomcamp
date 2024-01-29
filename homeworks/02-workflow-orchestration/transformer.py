import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id
    data.rename(columns={"VendorID": "vendor_id",
                         "RatecodeID": "ratecode_id",
                         "PULocationID": "pulocation_id",
                         "DOLocationID": "dolocation_id"
                        }, inplace=True)

    #Add three assertions:
        #vendor_id is one of the existing values in the column (currently)
        #passenger_count is greater than 0
        #trip_distance is greater than 0
    filtered_data = data[(data.passenger_count > 0) & (data.trip_distance > 0)]
    filtered_data['vendor_id'].notnull()

    return filtered_data


@test
def test_output(output, *args) -> None:
    assert output ['passenger_count'].isin([0]).sum() == 0
    assert output ['trip_distance'].isin([0]).sum() == 0
