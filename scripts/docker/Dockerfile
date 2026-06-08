FROM apache/airflow:2.8.1-python3.11

ARG PYPI_PROD
ARG PYPI_DEV
ARG CONSUL_TOKEN

ENV CONSUL_TOKEN=${CONSUL_TOKEN}

USER root
RUN apt-get update -y && \
    apt-get install -y build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash appuser
RUN usermod -aG root appuser

RUN mkdir -p /var/opt/brightmoney-central-servies-agents
RUN chown -R appuser:root /var/opt/brightmoney-central-servies-agents

USER appuser

COPY . /var/opt/brightmoney-central-servies-agents

WORKDIR /var/opt/brightmoney-central-servies-agents

RUN pip3 install virtualenv
RUN virtualenv venv
ENV OLD_PATH=$PATH
ENV PATH="/var/opt/brightmoney-central-servies-agents/venv/bin:$PATH"
ENV PIP_USER=false

RUN pip install --upgrade pip==23.0.1

RUN pip install --extra-index-url=${PYPI_PROD} --extra-index-url=${PYPI_DEV} -r /var/opt/brightmoney-central-servies-agents/requirements.txt

RUN python3 /var/opt/brightmoney-central-servies-agents/scripts/docker/create_env.py

ENV PATH=$OLD_PATH
USER airflow
