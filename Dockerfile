FROM python:3.6

MAINTAINER Bryce McDonnell <bryce@bridgetownint.com>

RUN mkdir /app
WORKDIR /app

ADD . /app

RUN apt-get update && pip install -r requirements.txt

CMD ["python", "consume_to_pg.py"]
