FROM python:3.8-slim
RUN sed -i 's/htt[p|ps]:\/\/archive.ubuntu.com\/ubuntu\//mirror:\/\/mirrors.ubuntu.com\/mirrors.txt/g' /etc/apt/sources.list

RUN apt-get update \
    && apt-get install build-essential -y \
    && pip install psycopg2-binary sqlalchemy faker

WORKDIR /app

COPY . .


