FROM prefecthq/prefect:latest-python3.10

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install libpq-dev -y

ADD . /

RUN pip install --upgrade pip

RUN pip install -r requirements.txt