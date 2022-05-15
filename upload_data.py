import argparse
from time import time
from unicodedata import name 
import pandas as pd 
from sqlalchemy import create_engine
import os 

def main(params):
     user=params.user
     password=params.password
     host=params.host
     #port=params.port
     db=params.db
     table_name=params.table_name
     
     csv_name="C:/Users/samuel/Desktop/docker_sql/yellow_tripdata_2020-06.csv"
     #os.system(f"wget {url} -O {csv_name}")
     engine=create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")
     df_iter=pd.read_csv(csv_name, iterator=True,chunksize=100000)
     df=next(df_iter)
     df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
     df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
     #print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))
     df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
     df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
     df.head(n=0).to_sql(name=table_name,con=engine, if_exists="replace")
     df.to_sql(name=table_name, con=engine, if_exists="append")
     while True:
         t_start=time()
         df=next(df_iter)
         df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
         df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
         df.to_sql(name=table_name, con=engine, if_exists="append")
         t_end=time()

     

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='ingest CSV data .')
    parser.add_argument('--user',help='username for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='hostfor postgres')
    #parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name  for postgres')
    parser.add_argument('--table_name',help='table-name for postgres')
    
    args = parser.parse_args()
    main(args)


