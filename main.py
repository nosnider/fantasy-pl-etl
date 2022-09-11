from datetime import datetime
import requests
import snowflake.connector
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
import numpy as np
import json
from prefect import flow, task
from dotenv import load_dotenv
import os
import time
import pandas as pd

# @task
# def get_creds():
#     load_dotenv('.env') 

#     SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
#     SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
#     SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')

#     return (SNOWFLAKE_USER,SNOWFLAKE_PASSWORD,SNOWFLAKE_ACCOUNT)

def get_snowflake_engine():
    print('getting snowflake engine...')

    # load creds - this probably isnt the right way to do this...
    # TODO: research proper way to source env vars
    load_dotenv('.env') 

    engine = create_engine(
        URL(
        account = os.environ.get('SNOWFLAKE_ACCOUNT'),
        user = os.environ.get('SNOWFLAKE_USER'),
        password = os.environ.get('SNOWFLAKE_PASSWORD'),
        database='FANTASY_PL_ETL',
        schema='RAW'
        )
    )
    return engine



# @task
def call_api():
    print('querying fantasy pl api...')
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()
    return data

@flow
def main_flow():
    api_result = call_api()
    engine = get_snowflake_engine()
    print(engine)


def prep_data(data):
    loaded_at = int(time.time())

    gameweeks = pd.DataFrame(json_data['events'])
    gameweeks['loaded_at']

    


# LOAD DATA FROM API
json_data = call_api()
loaded_at = int(time.time()) 
gameweeks = pd.DataFrame(json_data['events'])
gameweeks['loaded_at'] = loaded_at
# https://stackoverflow.com/questions/69772464/snowflake-connector-sql-compilation-error-invalid-identifier-from-pandas-datafra
gameweeks.columns = map(lambda x: str(x).upper(), gameweeks.columns)

# define variables we will pass into sql write function
table_name = 'testing1'
if_exists = 'append'

# get snowflake engine
engine = get_snowflake_engine()

#connect and write data
with engine.connect() as con:
    gameweeks.to_sql(
        name=table_name.lower()
        , con=engine
        , if_exists=if_exists
        , method=pd_writer
        , index=False)

if __name__ == "main":
    main_flow()