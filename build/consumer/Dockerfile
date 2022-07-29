FROM python:3.8-slim-buster
MAINTAINER LittleJake https://littlejake.net

WORKDIR /app
COPY . /app

RUN python3 -m pip install -r requirements.consumer.txt

CMD ["python3", "consumer.py"]
