import pandas as pd

mydf = pd.read_csv('taxi+_zone_lookup.csv')

mydf.columns = mydf.columns.str.lower()
from sqlalchemy import create_engine
user = 'root'
password = 'root'
host = 'localhost'
port = '5432'
db = 'ny_taxi'


engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

mydf.to_sql(name='zones', con=engine, if_exists='replace') # insert new rows if the table exists