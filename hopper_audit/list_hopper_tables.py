from db import db_conf as db


def get_table_list():
    """
    This method retrieves the list of all tables names in the
    database

    :return a python list of table names:
    """
    connection = db.get_connection()
    cursor = connection.cursor()

    lst_table_names = []
    sql_statement = "SELECT distinct(table_name) FROM information_schema.tables WHERE table_schema='public'"
    cursor.execute(sql_statement)
    table_names = cursor.fetchall()
    for table in table_names:
        lst_table_names.append(table[0])

    db.close_connection(connection, cursor)
    return lst_table_names


def get_table_columns(table_name):
    """
    this methods takes in a redshift table name and returns a
    list of the columns in that table
    :param table_name:
    :return: a python list of column names of the
    table that was passed in as the method parameter
    """
    connection = db.get_connection()
    cursor = connection.cursor()
    lst_column_names = []
    sql_statement = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' \
                        AND table_name = '{table_name}'"
    cursor.execute(sql_statement)
    column_names = cursor.fetchall()
    for column in column_names:
        lst_column_names.append(column[0])

    db.close_connection(connection, cursor)
    return lst_column_names


def table_column_dict():
    """
    This function loops through a list of tables and returns the below
    :return: a python dictionary with table names as key and a list of columns as values
    """

    tables = get_table_list()
    dict_table_column = dict()
    for table in tables:
        lst_columns = get_table_columns(table)
        dict_table_column[table] = lst_columns
    return dict_table_column


def check_if_column_empty(table, column):
    """
    This function checks if table column is empty
    :param table:
    :param column:
    :return:
    """

    connection = db.get_connection()
    cursor = connection.cursor()
    sql_statement = f'select {column} from {table} order by {column} desc limit 1'
    cursor.execute(sql_statement)
    column_value = cursor.fetchone()

    if column_value:
        return 1
    else:
        return 0


# print(table_column_dict())
