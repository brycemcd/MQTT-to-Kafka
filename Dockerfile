FROM python:3.6

MAINTAINER Bryce McDonnell <bryce@bridgetownint.com>

RUN mkdir /app
WORKDIR /app

# add requirements for pip, but mount the app via volumes
ADD requirements.txt /app

RUN apt-get update && pip install --upgrade pip && pip install -r requirements.txt

CMD ["python", "consume_to_pg.py"]
