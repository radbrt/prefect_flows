from asyncio.log import logger
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
import prefect
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from prefect.tasks.secrets import PrefectSecret
from prefect.backend import kv_store
from time import sleep
import random


def get_secret(secret_name):
    KVUri = f"https://co-cerx-aks-kv.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    kv_secret = client.get_secret(secret_name).value
    return kv_secret


@task
def save_ads(ads_list):
    logger = prefect.context.get("logger")
    secret = get_secret("mongodb-connectstring")
    client = pymongo.MongoClient(secret)
    jobsdb = client.jobs
    adscollection = jobsdb.ads

    errors = []
    insert_attempts = 0
    for ad in ads_list:
        try:
            time.sleep(0.01)
            adscollection.insert_one(ad)
            insert_attempts += 1
        except Exception as e:
            errors.append({"ad": ad, "error": e})
            logger.info(f"Failed to save ad. Error: {e}")
    return(insert_attempts, errors)


@task
def initiate_harvest(endpoint, token):
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    logger = prefect.context.get('logger')

    last_run = kv_store.get_key_value('last_ads_run')

    logger.info(f"Last run at {last_run}")

    endtime = "*"

    args = f"size=100&published=%5B{last_run}%2C{endtime}%5D"

    curpage = 0
    full_url = f"{endpoint}?{args}&page={curpage}"
    r = requests.get(full_url, headers=HEADERS)

    print(r.status_code)
    ads = json.loads(r.text)
    maxpage = ads['totalPages']
    save_ads.run(ads)

    url_calls = [requests.Request('GET', f"{endpoint}?{args}&page={curpage}", headers=HEADERS) for curpage in range(1, maxpage+1)]
    return url_calls


@task
def additional_page(req):
    sleep(random.random()*10)
    logger = prefect.context.get('logger')
    
    prepped_req = req.prepare()
    s = requests.Session()
    response = s.send(prepped_req)
    if response.ok:
        ads = json.loads(response.text)
        save_ads.run(ads)
    else:
        logger.info(f"Save ad not successful: {response.status_code}")


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


@task
def update_hw(start_time, x):
    logger = prefect.context.get('logger')
    logger.info(f"Updating last ads run to {start_time}")
    kv_store.set_key_value('last_ads_run', start_time)

    updated_value = kv_store.get_key_value('last_ads_run')
    logger.info(f"Last ads run updated to {updated_value}")

@task
def get_timestamp():
    return datetime.today().isoformat(timespec='seconds')


with Flow("Harvest ads") as flow:

    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'
    TOKEN = PrefectSecret('NAV_TOKEN')
    start_time = get_timestamp()

    additional_jobs = initiate_harvest(ENDPOINT, TOKEN)
    additional_results = additional_page.map(additional_jobs)
    update_hw(start_time, additional_results)



flow.run_config = KubernetesRun(labels=['aks'])

flow.executor = LocalExecutor()

flow.storage = Docker(
    registry_url='cocerxkubecr.azurecr.io',
    image_name='harvest_ads',
    image_tag='latest',
    dockerfile='Dockerfile'
)

# M H DOM M DOW
flow.schedule = CronSchedule('30 02 * * *', start_date=datetime.now())

if __name__ == '__main__':
    flow.run()
