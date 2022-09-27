import requests
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
import os
import time
import pandas as pd


def get_snowflake_engine():
    print('getting snowflake engine...')

    # load creds - this probably isn't the right way to do this...
    # TODO: research proper way to source env vars

    engine = create_engine(
        URL(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            database='FANTASY_PL_ETL',
            schema='RAW'
        )
    )
    return engine


# @task
def extract():
    # returns: data -> json respnose with the following keys:
    # 'events' - 1 row per gameweek
    # 'teams' - 1 row per team
    # 'elements' - players
    # 'element_types' - player type
    print('querying fantasy pl api...')
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    print(f'response status code: {response.status_code}')
    data = response.json()
    data['loaded_at'] = time.time()

    return data


def transform(data: dict, json_table_name: str):
    # transform(data): function that receives data from prem league api and generates pandas dataframes
    # input:   data -> json data from prem league api
    # returns: df -> dataframe with current time, columns enriched

    df = pd.DataFrame(data[json_table_name])
    df['loaded_at'] = data['loaded_at']
    # https://stackoverflow.com/questions/69772464/snowflake-connector-sql-compilation-error-invalid-identifier-from-pandas-datafra
    df.columns = map(lambda x: str(x).upper(), df.columns)

    return df, json_table_name


def load(df, engine, table_name: str):
    with engine.connect() as con:
        status = df.to_sql(
            name=table_name.lower()
            , con=engine
            , if_exists='replace'
            , method=pd_writer
            , index=False)


if __name__ == "__main__":
    data = extract()
    engine = get_snowflake_engine()

    for table in ['events', 'teams', 'elements', 'element_types']:
        data_transformed = transform(data, table)
        load(df=data_transformed[0], engine=engine, table_name=data_transformed[1])
