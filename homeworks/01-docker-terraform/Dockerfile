FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY 2_insert_data_into_postgres.py insert_data_into_postgres.py

ENTRYPOINT [ "python", "insert_data_into_postgres.py"]