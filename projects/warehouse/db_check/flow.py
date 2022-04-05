from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.tasks.secrets import PrefectSecret
from prefect.storage import Docker
from prefect.artifacts import create_markdown
from prefect.backend.artifacts import create_markdown_artifact
import prefect

import random

import datetime

@task
def check_db_quality():
    create_markdown_artifact(
        f"""
        # Sample Artifact
        
        The randomly generated value is {random.random()}

        Generated at {datetime.datetime.utcnow().isoformat()}
        """)

dockerstore = Docker(
    image_name='db_check',
    registry_url='radbrt.azurecr.io',
    dockerfile='Dockerfile'
)

with Flow("DWH Quality check", storage=dockerstore) as flow:
    check_db_quality()

flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalDaskExecutor()