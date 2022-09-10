from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from snowflake.sqlalchemy import URL
import os


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


table_name = 'testing'
if_exists = 'append'
df = pd.DataFrame(data=[['Stephen','Oslo'],['Jane','Stockholm']],columns=['Name','City'])

engine = get_snowflake_engine()

with engine.connect() as con:
        df.to_sql(
            name=table_name.lower()
            , con=engine
            , if_exists=if_exists
            , method=pd_writer
            , index=False)

