import os
from prefect import Flow, task
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.shell import ShellTask
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
import prefect

@task
def run_el():
    logger = prefect.context.get("logger")


    tap_sftp_config = PrefectSecret("TAP_SFTP_CONFIG").run()

    logger.info(tap_sftp_config.keys())
    logger.info(tap_sftp_config["TAP_SFTP_HOST"])

    s = ShellTask(
        command="meltano elt tap-sftp target-jsonl",
        # command="echo $TAP_SFTP_HOST",
        helper_script="cd /el", 
        shell="bash", 
        return_all=True, 
        log_stderr=True, 
        stream_output=True
    )
    s.run(env={
            "TAP_SFTP_PASSWORD": tap_sftp_config["TAP_SFTP_PASSWORD"],
            "TAP_SFTP_USERNAME": tap_sftp_config["TAP_SFTP_USERNAME"],
            "TAP_SFTP_HOST": tap_sftp_config["TAP_SFTP_HOST"],
            # "MELTANO_DATABASE_URI": PrefectSecret("MELTANO_DATABASE_URI").run(),
            })


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