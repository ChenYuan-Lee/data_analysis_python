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


def save_as_csv(df: DataFrame, file_name: str) -> None:
    assert file_name[-4:] == ".csv"
    df.to_csv(f"csv_files/{file_name}", index_label=False)


def retrieve_and_save_data(query: str, file_name: str) -> None:
    df = pull_from_snowflake(query)
    save_as_csv(df=df, file_name=file_name)
