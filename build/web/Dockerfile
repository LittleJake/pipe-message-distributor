# FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
FROM python:3.8-slim-buster
MAINTAINER LittleJake https://littlejake.net

WORKDIR /app
COPY . /app

RUN python3 -m pip install -r requirements.server.txt

ENV FLASK_APP=main.py

CMD ["flask", "run", "--host", "0.0.0.0", "--port", "6080"]

EXPOSE 6000