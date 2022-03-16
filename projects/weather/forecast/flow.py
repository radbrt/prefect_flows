import datetime
import requests
import prefect
from prefect import task, Flow, Parameter, case
from prefect.tasks.secrets import PrefectSecret
from prefect.artifacts import create_link
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun

# TASK DEFINITIONS
# Parameter Task takes a value at runtime
city = Parameter(name="City", default="Oslo")

# PrefectSecret Task stored in Hashicorp Vault, populated at runtime
api_key = PrefectSecret("WEATHER_API_KEY")

# Extraction Task pulls 5-day, 3-hour forcast for the provided City
@task(max_retries=2, retry_delay=datetime.timedelta(seconds=5))
def pull_forecast(city, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/forecast?"
    url = base_url + "appid=" + api_key + "&q=" + city
    link_artifact = create_link(url)
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return data

# Analysis Task determines whether there will be rain in forecast data
@task(tags=["database", "analysis"])
def is_raining_this_week(data):
    rain = [
        forecast["rain"].get("3h", 0) for forecast in data["list"] if "rain" in forecast
    ]
    return True if sum([s >= 1 for s in rain]) >= 1 else False


# FLOW DEFINITION
with Flow("Rain Flow") as flow:
    logger = prefect.context.get('logger')
    # Set up Task dependencies
    forecast = pull_forecast(city=city, api_key=api_key)
    rain = is_raining_this_week(forecast)
    with case(rain, True):
        logger.info("RAIN in the forecast for this week!")
    with case(rain, False):
        logger.info("NO RAIN in the forecast for this week!")


    # Storage for code pushed to Docker Registry, image pulled at runtime
flow.storage = Docker(
    registry_url="radbrt.azurecr.io",
    image_name="prefect_rain",
    image_tag='latest'
)

# Flow Run configuration for env vars and label matching
flow.run_config = KubernetesRun(labels=["aks"])