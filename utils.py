import os

from pandas import DataFrame, read_sql
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def pull_from_snowflake(query: str) -> DataFrame:
    """
    A helper function to execute a SQL query on the data warehouse and return the relevant rows
    """
    snowflake_url = URL(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )
    with create_engine(snowflake_url).connect() as connection:
        query_result = read_sql(query, con=connection)
    return query_result
