FROM python:3.12.0-slim-bookworm

RUN apt-get update -y
RUN apt install -y gcc librdkafka-dev

# Create filesystem tree structure
RUN mkdir /opt/phenomena-agent
RUN mkdir /opt/phenomena-agent/bin
RUN mkdir /opt/phenomena-agent/config
RUN mkdir /opt/phenomena-agent/secrets
RUN mkdir /var/log/phenomena-agent

WORKDIR /opt/phenomena-agent

ADD ./requirements.txt .
RUN pip3 install -r requirements.txt

ADD src /opt/phenomena-agent/bin

ENTRYPOINT [ "python3", "/opt/phenomena-agent/bin/main.py" ]