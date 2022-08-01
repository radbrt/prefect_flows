from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
import prefect
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
    db = "DBTHOUSE"
    schema = "DEVELOP"

    example_query = f"""
        select *
        from {table_name};
    """

    constring = f"snowflake://{username}:{password}@{account}/?warehouse={warehouse}&role={role}"
    con = create_engine(constring)

    df = pd.read_sql(table_name, con=con)

    return df


@task()
def write_to_pg(df, to_table, pg_creds):
    username = pg_creds["USERNAME"]
    password = pg_creds["PASSWORD"]
    host = pg_creds["HOST"]

    connstring = f"postgresql+psycopg2://{username}:{password}@{host}:5432/postgres?sslmode=require"
    con = create_engine(connstring)
    df.to_sql(name=to_table, con=con)

    logger = prefect.context.get('logger')
    logger.info(df.dtypes)
    sum_value = df.count().compute()
    logger.info(f"Counted {sum_value} rows")
    return sum_value


with Flow("Pandas Loader") as flow:
    table_name = Parameter('table_name', default='ECONOMY_DATA_ATLAS.ECONOMY.DATASETS')
    to_table = Parameter('to_table', default='dask_target6')
    creds = PrefectSecret('SNOWFLAKE_CREDS')
    pg_creds = PrefectSecret('PG_CREDS')
    df = query_snowflake(table_name, creds)
    write = write_to_pg(df, to_table, pg_creds)


if __name__ == '__main__':
    flow.run()

# cocerxkubecr.azurecr.io/dask_loader:latest
flow.storage = Docker(
    image_name="pandas_loader",
    image_tag='latest',
    registry_url='cocerxkubecr.azurecr.io',
    dockerfile='Dockerfile'
)


flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalExecutor()
