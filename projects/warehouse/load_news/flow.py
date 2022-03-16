from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.tasks.secrets import PrefectSecret
from prefect.storage import Docker
import prefect
import json
import pandas as pd
import feedparser
from google.oauth2 import service_account
import datetime

@task
def save_frontpage():
    gcp_key = PrefectSecret("GCP-KEY").run()

    URL = 'https://www.aftenposten.no/rss'
    nrk_rss = feedparser.parse(URL)
    rss_entries_string = json.dumps(nrk_rss['entries'])
    rss_entries_json = json.loads(rss_entries_string)

    logger = prefect.context.get('logger')

    credentials = service_account.Credentials.from_service_account_info(gcp_key)
    df = pd.DataFrame(rss_entries_json, dtype='str')
    df['loaded_at'] = datetime.datetime.utcnow()

    df.to_gbq("radwarehouse.staging.aftenposten_frontpage", "radwarehouse", if_exists='append', credentials=credentials)


dockerstore = Docker(
    image_name='aftenposten_feed_flow',
    registry_url='radbrt.azurecr.io',
    dockerfile='Dockerfile'
)

with Flow("Aftenposten feed flow", storage=dockerstore) as flow:
    save_frontpage()

flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalDaskExecutor()