from datetime import datetime
import requests
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import json
from prefect import flow, task
from dotenv import load_dotenv
import os
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


json_data = call_api()
loaded_at = datetime.now()
gameweeks = pd.DataFrame(json_data['events'])
gameweeks['loaded_at'] = loaded_at

engine = get_snowflake_engine()
connection = engine.connect()
gameweeks.to_sql('gameweek_hi', con = engine, index=False, if_exists='append')
connection.close()
engine.dispose()

print('wrote to snowflake lol')

if __name__ == "main":
    main_flow()