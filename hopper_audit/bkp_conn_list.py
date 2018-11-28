import psycopg2


# DB_USER = "admin",
# DB_PASSWORD = "Milmi24hind"
# HOST = "abacosdw.cef4fivrgm1a.us-east-2.redshift.amazonaws.com"
# # HOST = "hopper.ckzjiwngbej4.us-east-1.redshift.amazonaws.com:5439/bi"
# PORT = "5439"
# DATABASE = "abacosdw"

try:
    connection = psycopg2.connect(
        host="abacosdw.cef4fivrgm1a.us-east-2.redshift.amazonaws.com",
        user='admin',
        port=5439,
        password='Milmi24hind',
        dbname='abacosdw')
    cursor = connection.cursor()  # create a cursor for executing queries

    # Print PostgreSQL Connection properties
    # print(connection.get_dsn_parameters(), "\n")

    # Print PostgreSQL version
    cursor.execute("select distinct(tablename) from pg_table_def where schemaname = 'public';")
    # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    records = cursor.fetchall()
    print("List of tables - ", records, records[0], "\n")
    for r in records:
        print(r[0])
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
    print('Finished')
