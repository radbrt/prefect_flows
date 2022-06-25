import pymongo
import time
import json
import requests
from datetime import datetime, timedelta
from prefect.storage import Docker
from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.schedules import CronSchedule
import base64
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from prefect.tasks.secrets import PrefectSecret



def get_secret(secret_name):
    KVUri = f"https://co-cerx-aks-kv.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    kv_secret = client.get_secret(secret_name).value
    return kv_secret


@task
def save_ads(ads_list):
    secret = get_secret("mongodb-connectstring")
    client = pymongo.MongoClient(secret)
    jobsdb = client.jobs

    errors = []
    insert_attempts = 0
    for ad in ads_list:
        try:
            time.sleep(0.01)
            jobsdb.insert_one(ad)
            insert_attempts += 1
        except Exception as e:
            errors.append({"ad": ad, "error": e})
    return(insert_attempts, errors)


@task
def initiate_harvest(endpoint, token, headers):


    last_run = datetime.today() - timedelta(days=1, minutes=3)
    start_isotime = last_run.isoformat(timespec='seconds')
    endtime = "*"

    args = f"size=100&published=%5B{start_isotime}%2C{endtime}%5D"

    curpage = 0
    r = requests.get(f"{ENDPOINT}?{args}&page={curpage}", headers=HEADERS)

    ads = json.loads(r.text)
    maxpage = ads['totalPages']
    save_ads.run(ads)


    url_calls = [requests.Request('GET', f"{ENDPOINT}?{args}&page={curpage}", headers=HEADERS) for curpage in range(2, maxpage+1)]
    return url_calls


@task
def additional_page(req):
    prepped_req = req.prepare()
    s = requests.Session()
    response = s.send(prepped_req)
    if response.ok:
        ads = json.loads(response.text)
        save_ads.run(ads)


@task
def insert_ads(adsarray, db_table):
    errors = []
    insert_attempts = 0
    for ad in adsarray:
        try:
            time.sleep(0.01)
            db_table.insert_one(ad)
            insert_attempts += 1
        except Exception as e:
            errors.append({"ad": ad, "error": e})
    return(insert_attempts, errors)


with Flow("Harvest ads") as flow:

    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'
    TOKEN = PrefectSecret('NAV_TOKEN')
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
    
    additional_jobs = initiate_harvest(ENDPOINT, TOKEN, HEADERS)
    additional_results = additional_page.map(additional_jobs)

flow.run_config = KubernetesRun(labels='aks')

flow.executor = LocalExecutor()

flow.storage = Docker(
    registry_url='cocerxkubecr.azurecr.io',
    image_name='save_enhetsregisteret',
    image_tag='latest',
    dockerfile='Dockerfile'
)

# M H DOM M DOW
flow.schedule = CronSchedule('30 02 * * *', start_date=datetime.datetime.now())

if __name__ == '__main__':
    flow.run()
