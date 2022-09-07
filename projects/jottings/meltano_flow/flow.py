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
    meltano_database_uri = PrefectSecret("MELTANO_DATABASE_URI").run()
    target_snowflake_config = PrefectSecret("TARGET_SNOWFLAKE_CONFIG").run()
    logger.info(tap_sftp_config.keys())
    logger.info(tap_sftp_config["TAP_SFTP_HOST"])

    s = ShellTask(
        command="meltano elt tap-sftp target-snowflake",
        helper_script="cd /el", 
        shell="bash", 
        return_all=True, 
        log_stderr=True, 
        stream_output=True
    )
    s.run(env={
            "TAP_SFTP_PASSWORD": tap_sftp_config["TAP_SFTP_PASSWORD"],
            "TAP_SFTP_USERNAME": tap_sftp_config["TAP_SFTP_USERNAME"]
            "MELTANO_DATABASE_URI": meltano_database_uri,
            "TARGET_SNOWFLAKE_PASSWORD": target_snowflake_config["password"],
            "TARGET_SNOWFLAKE_USERNAME": target_snowflake_config["user"],
            "TARGET_SNOWFLAKE_ACCOUNT": target_snowflake_config["account"],
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