FROM prefecthq/prefect:1.2.4-python3.10

RUN apt update && 	apt install -y python3-dev libpq-dev && 	apt-get install -y build-essential git

RUN pip install --upgrade pip wheel

RUN pip install meltano

WORKDIR /el
COPY meltano.yml .

RUN meltano install

WORKDIR /

