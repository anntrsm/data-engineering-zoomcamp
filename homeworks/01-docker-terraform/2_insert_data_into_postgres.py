import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    dbname = params.dbname
    tablename = params.tablename
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'result_data.csv.gz'
    else:
        csv_name = 'result_data.csv'

    os.system (f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    df = pd.read_csv(csv_name, nrows=100)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    if tablename == 'green_zone_taxi_data':
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tablename, con=engine, if_exists='replace')  # table creation

    while True:
        try:
            t_start = time()

            df = next(df_iter)
            if tablename == 'green_zone_taxi_data':
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=tablename, con=engine, if_exists='append')

            t_end = time()
            print('inserted chunk, took %.3f seconds' % (t_end - t_start))  # %.3f - 3 points after comma
        except StopIteration:
            print('All data were successfully inserted into database.')
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Insert CSV data')

    parser.add_argument('--user', help='username for DB')
    parser.add_argument('--password', help='password for DB')
    parser.add_argument('--host', help='host for DB')
    parser.add_argument('--port', help='port for DB')
    parser.add_argument('--dbname', help='name of database')
    parser.add_argument('--tablename', help='name of table in db')
    parser.add_argument('--url', help='file url')

    args = parser.parse_args()

    main(args)