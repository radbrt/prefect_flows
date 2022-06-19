from prefect import Flow, Task, Parameter, task
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
from prefect.schedules import Schedule
from prefect.schedules.clocks import Clock
from datetime import datetime, timedelta

def get_times():
    for i in range(1, 100):
        o = datetime.fromordinal(738390)
        n = o + timedelta(days=i)
        yield Clock(n)

@task(log_stdout=True)
def t_one(v1, v2):
    print("TOne", v1, v2)

@task(log_stdout=True)
def t_two(v1, v2):
    print("TTwo", v1, v2)

@task(log_stdout=True)
def t_par(x):
    print("X is ", x)

with Flow("Some flow", executor=LocalDaskExecutor()) as flow:

    p = Parameter("x", default="Some Param")


    t1 = t_one("A", "B")
    t2 = t_two("1", "2")
   
    t2.set_upstream(t1)
    t3 = t_par(p)
    t3.set_upstream(t1)

flow.run_config = KubernetesRun()

flow.storage = Docker(
    image_name='radbrt/demo_dag',
    image_tag='latest',
    dockerfile='Dockerfile'
)

flow.schedule = Schedule(get_times())