FROM prefecthq/prefect:latest-python3.9

RUN apt update
RUN apt upgrade -y

ADD . /

RUN pip install -r requirements.txt