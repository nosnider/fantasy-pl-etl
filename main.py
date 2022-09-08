import requests
import snowflake.connector
import json
from prefect import flow, task
from dotenv import load_dotenv
import os

# @task
# def get_creds():
#     load_dotenv('.env') 

#     SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
#     SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
#     SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')

#     return (SNOWFLAKE_USER,SNOWFLAKE_PASSWORD,SNOWFLAKE_ACCOUNT)

def get_snowflake_connection():
    print('getting snowflake cursor...')

    # load creds - this probably isnt the right way to do this...
    # TODO: research proper way to source env vars
    load_dotenv('.env') 

    ctx = snowflake.connector.connect(
    account = os.environ.get('SNOWFLAKE_ACCOUNT'),
    user = os.environ.get('SNOWFLAKE_USER'),
    password = os.environ.get('SNOWFLAKE_PASSWORD'),
    database='FANTASY_PL_ETL',
    schema='RAW'
    )
    return ctx



@task
def call_api():
    print('querying fantasy pl api...')
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()
    return data

@flow
def main_flow():

    api_result = call_api()
    print(api_result)

    cursor = get_snowflake_cursor()

main_flow()