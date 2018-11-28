from db import db_conf as db


def get_table_list():
    """

    :return:
    """
    connection = db.get_connection()
    cursor = connection.cursor()

    lst_tablenames = []
    cursor.execute("select distinct(tablename) from pg_table_def where schemaname = 'public';")
    # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    table_names = cursor.fetchall()
    for table in table_names:
        lst_tablenames.append(table[0])

    db.close_connection()
    return lst_tablenames


print(get_table_list())