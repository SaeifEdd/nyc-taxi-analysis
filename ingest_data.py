import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(args):

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db_name = args.db_name
    table_name = args.table_name
    url = args.url
    #download csv file
    csv_gz_name = "output.gz"
    csv_name = "output.csv"
    os.system(f'wget {url} -O {csv_gz_name}')
    os.system(f'gunzip -c {csv_gz_name} > {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    #generate schema
    #print(pd.io.sql.get_schema(df, name='yello_taxi_data', con=engine))
    # splitting into chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    #create the table:
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print("chunk inserted ... in %0.3f" % (t_end - t_start) )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest csv data to postgres db")
    parser.add_argument("--user", help="user name in postgres db")
    parser.add_argument("--password", help="password for postgres db")
    parser.add_argument("--host", help="host machine for postgres db")
    parser.add_argument("--port", help="port number for postgres db")
    parser.add_argument("--db_name", help="db name of postgres db")
    parser.add_argument("--table_name", help="table name for data")
    parser.add_argument("--url", help="url where to download csv file")
    args = parser.parse_args()
    main(args)