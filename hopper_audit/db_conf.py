import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

user = os.getenv('REDSHIFT_USER')
passwd = os.getenv('REDSHIFT_PASS')
host = os.getenv('REDSHIFT_HOST')
db_name = os.getenv('DB_NAME')

def get_connection():
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            port=5439,
            password=passwd,
            dbname=db_name
        )

        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def close_connection(connection):
    if connection:
        connection.close()

