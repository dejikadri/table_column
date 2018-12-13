import os
from os.path import join, dirname
from dotenv import load_dotenv

import psycopg2
env_file_path = join(os.path.dirname(os.path.dirname(__file__)),'.env')
load_dotenv(env_file_path)


user = os.getenv('REDSHIFT_USER')
passwd = os.getenv('REDSHIFT_PASS')
host = os.getenv('REDSHIFT_HOST')
dbname = os.getenv('DBNAME')


def get_connection():
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            port=5439,
            password=passwd,
            dbname=dbname
        )
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def close_connection(connection):
    if connection:
        connection.close()

