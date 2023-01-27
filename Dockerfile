FROM python:3.11.1-slim-bullseye

RUN apt-get update -qq
RUN apt-get install -qq gcc

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt