import psycopg2


def get_connection():
    try:
        connection = psycopg2.connect(
            # host="abacosdw.cef4fivrgm1a.us-east-2.redshift.amazonaws.com",
            # user='admin',
            # port=5439,
            # password='Milmi24hind',
            # dbname='abacosdw',
            #

            host="localhost",
            user='andelastc',
            port=5432,
            password='',
            dbname='djdb'


        )
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def close_connection(connection, cursor):
    if connection:
        cursor.close()
        connection.close()

