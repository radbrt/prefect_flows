from prefect import task, Flow
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
import prefect
import datetime
from prefect.schedules import IntervalSchedule


@task()
def hello_task():
    logger = prefect.context.get('logger')
    logger.info("Hello ")


with Flow("{{ cookiecutter.flow_name }}") as flow:
    hello_task()


flow.storage = Docker(
    image_name="{{ cookiecutter.flow_slug }}",
    image_tag='latest',
    registry_url='cocerxkubecr.azurecr.io',
    dockerfile='Dockerfile'
)

flow.schedule = IntervalSchedule(
    start_date=datetime.datetime.utcnow(),
    interval=datetime.timedelta(minutes=60)
)

flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalExecutor()
