from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
import prefect
import dask.dataframe as dd
from dask_snowflake import read_snowflake
from prefect.tasks.secrets import PrefectSecret
import pandas as pd
from sqlalchemy import create_engine
import datetime



@task()
def query_snowflake(table_name, creds):
    logger = prefect.context.get('logger')
    username = creds["USERNAME"]
    password = creds["PASSWORD"]
    account = creds["ACCOUNT"]
    warehouse = creds["WAREHOUSE"]
    role = creds["ROLE"]
    db = "ECONOMY_DATA_ATLAS"
    schema = "ECONOMY"

    example_query = f"""
        select *
        from {db}.{schema}.{table_name};
    """

    ddf = read_snowflake(
        query=example_query,
        connection_kwargs={
            "user": username,
            "password": password,
            "account": account,
            "warehouse": warehouse
        },
    ).repartition(partition_size='100MB')

    return ddf


@task()
def write_to_pg(df, to_table, pg_creds):
    username = pg_creds["USERNAME"]
    password = pg_creds["PASSWORD"]
    host = pg_creds["HOST"]

    connstring = f"postgresql+psycopg2://{username}:{password}@{host}:5432/postgres?sslmode=require"

    df.to_sql(name=to_table, uri=connstring, chunksize=10000, if_exists="replace")

    logger = prefect.context.get('logger')
    logger.info(pg_creds)
    logger.info(df.dtypes)
    sum_value = df.count().compute()
    logger.info(f"Counted {sum_value} rows")
    return sum_value


with Flow("Dask Loader") as flow:
    table_name = Parameter('table_name', default='DATASETS')
    to_table = Parameter('to_table', default='dask_target6')
    creds = PrefectSecret('SNOWFLAKE_CREDS')
    pg_creds = PrefectSecret('PG_CREDS')
    df = query_snowflake(table_name, creds)
    write = write_to_pg(df, to_table, pg_creds)


if __name__ == '__main__':
    flow.run()

# cocerxkubecr.azurecr.io/dask_loader:latest
flow.storage = Docker(
    image_name="dask_loader",
    image_tag='latest',
    registry_url='cocerxkubecr.azurecr.io',
    dockerfile='Dockerfile'
)


flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalExecutor()
