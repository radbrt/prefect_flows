from pathlib import Path
from this import s

import requests
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from prefect.storage import Docker
from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.schedules import CronSchedule
import datetime



def get_secret(secret_name):
    keyVaultName = 'co-cerx-aks-kv'
    KVUri = f"https://{keyVaultName}.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    kv_secret = client.get_secret(secret_name).value
    return kv_secret


def save_file_to_storage(prefix, url):
    filename = f"{prefix}/{str(datetime.date.today())}.json"

    credential = DefaultAzureCredential()
    service = BlobServiceClient(account_url=f"https://radlake.blob.core.windows.net/", credential=credential)

    container_client = service.get_container_client("enhetsregisteret")
    blob = BlobClient(account_url=f"https://radlake.blob.core.windows.net/", container_name="enhetsregisteret", blob_name="testfile.txt", credential=credential)
    blob_client = container_client.get_blob_client(filename)
    r = requests.get(url).raw.read()
    blob_client.upload_blob(r, blob_type="BlockBlob")

    return filename


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=5))
def process_single_file(url):
    filename = save_file_to_storage(url['entity'], url['url'])


with Flow("Save Enhetsregisteret") as flow:

    urls = Parameter("urls", default=[
        {'entity': 'foretak',
        'url': "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"},
        {'entity': 'virksomheter',
        'url': "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned"}
    ])

    process_single_file.map(urls)


flow.run_config = KubernetesRun(labels='aks')

flow.executor = LocalExecutor()

flow.storage = Docker(
    registry_url='cocerxkubecr.azurecr.io',
    image_name='save_enhetsregisteret',
    image_tag='latest',
    dockerfile='Dockerfile'
)

# M H DOM M DOW
flow.schedule = CronSchedule('15 04 * * 6', start_date=datetime.datetime.now())

if __name__ == '__main__':
    flow.run()
