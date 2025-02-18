import io
import os
import requests
import pandas as pd
from google.cloud import storage
"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/python-runner-gcp.json"

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dezoomcamp_hw4")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    for i in range(12):

        # sets the month part of the file_name string
        month = '0' + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # define all the datatypes
        # this was necessary as I was getting field datatype errors when reading in GBQ
        # this enforces the fields coming in to be a certain datatype
        taxi_dtypes = {
            'VendorID': pd.Int64Dtype(),
            'passenger_count': pd.Int64Dtype(),
            'trip_distance': float,
            'RatecodeID': pd.Int64Dtype(),
            'store_and_fwd_flag': str,
            'PULocationID': pd.Int64Dtype(),
            'PUlocationID': pd.Int64Dtype(),
            'DOLocationID': pd.Int64Dtype(),
            'DOlocationID': pd.Int64Dtype(),
            'payment_type': pd.Int64Dtype(),
            'fare_amount': float,
            'extra': float,
            'mta_tax': float,
            'tip_amount': float,
            'tolls_amount': float,
            'improvement_surcharge': float,
            'total_amount': float,
            'congestion_surcharge': float,
            'Affiliated_base_number': str,
            'SR_Flag': pd.Int64Dtype(),
            'dispatching_base_num': str,
            'trip_type': pd.Int64Dtype()
        }

        # native date parsing
        # depending on which file type comes through I do the date parsing by field name
        if (service == 'green'):
            parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        elif (service == 'yellow'):
            parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        else:
            parse_dates = ['pickup_datetime', 'dropOff_datetime']

        # Read the data into an iterator
        # splitting the file into chunks helped in the memory management
        df_iter = pd.read_csv(file_name \
                              , sep=',' \
                              , compression='gzip' \
                              , dtype=taxi_dtypes \
                              , parse_dates=parse_dates
                              , iterator=True \
                              , chunksize=100000 \
                              , low_memory=False)

        # iterate over the chunks and append them to an array
        data = []
        for batch in df_iter:
            data.append(batch)

        # bring all the chunks of data together
        df = pd.concat(data)

        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

        # output the file as parquet file
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# -----------------------------------------------------------------------
# MAIN PROCESSING SECTION
web_to_gcs('2019', 'fhv')