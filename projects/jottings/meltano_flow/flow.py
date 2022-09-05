import os
from prefect import Flow, task
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.shell import ShellTask
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker


@task
def run_el():
    s = ShellTask(command="meltano elt tap-sftp target-jsonl", 
    env={
        "TAP_SFTP_PASSWORD": PrefectSecret("TAP_SFTP_PASSWORD"),
        # "MELTANO_DATABASE_URI": PrefectSecret("MELTANO_DATABASE_URI"),
        }, 
    helper_script="cd /el", 
    shell="bash", 
    return_all=True, 
    log_stderr=True, 
    stream_output=True)
    s.run()


with Flow("Render Meltano") as flow:
    run_el()

flow.storage = Docker(
    registry_url="cocerxkubecr.azurecr.io/empty_meltano",
    image_tag="latest",
    dockerfile='Dockerfile'
)
flow.run_config = KubernetesRun(labels=["aks"])
flow.executor = LocalExecutor()

if __name__ == '__main__':
    flow.run()