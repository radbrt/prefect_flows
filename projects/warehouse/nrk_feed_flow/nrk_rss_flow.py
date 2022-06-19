from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
# from prefect.tasks.dbt import dbt
# from prefect.tasks.secrets import PrefectSecret
from prefect.client.secrets import Secret
from prefect.storage import Docker
import prefect
import json
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import feedparser
from google.oauth2 import service_account
import datetime
from prefect.schedules import IntervalSchedule

@task()
def save_frontpage(df, creds):
    df.to_gbq("radwarehouse.staging.nrk_frontpage", "radwarehouse", if_exists='append', credentials=creds)


@task
def get_credentials(gcp_key):
    credentials = service_account.Credentials.from_service_account_info(gcp_key)
    return credentials


@task
def read_rss(URL):
    nrk_rss = feedparser.parse(URL)
    rss_entries_string = json.dumps(nrk_rss['entries'])
    rss_entries_json = json.loads(rss_entries_string)
    df = pd.DataFrame(rss_entries_json, dtype='str')
    df['loaded_at'] = datetime.datetime.utcnow()

    return df


# @task()
# def run_snapshot(model_name):
#     runjob = dbt.DbtCloudRunJob(cause="prefect run",
#                                 account_id=PrefectSecret('DBT-ACCOUNT-ID'),
#                                 token=PrefectSecret('DBT-KEY'),
#                                 job_id=PrefectSecret('NRK-JOB-ID'),
#                                 wait_for_job_run_completion=True
#                                 )



with Flow("nrk_feed_flow", storage=dockerstore, schedule=nrk_schedule) as flow:
    gcp_key = Secret.get("GCP-KEY")
    URL = 'https://www.nrk.no/nyheter/siste.rss'

    creds = get_credentials(gcp_key)
    rss_df = read_rss(URL)
    save_frontpage(rss_df, creds)


flow.storage = Docker(
    image_name='nrk_feed_flow',
    image_tag='latest',
    registry_url='cocerxkubecr.azurecr.io',
    dockerfile='Dockerfile'
)

flow.schedule = IntervalSchedule(
    start_date=datetime.datetime.utcnow(),
    interval=datetime.timedelta(minutes=60)
)

flow.run_config = KubernetesRun(labels=["aks", "cerxkube"])
flow.executor = LocalDaskExecutor()
