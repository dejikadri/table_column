from db import db_conf as db


def get_table_column_dict(connection):
    """
    This function loops through a list of tables and returns the below
    :return: a python dictionary with table names as key and a list of empty columns as values
    """
    cursor = connection.cursor()

    dict_table_column = dict()

    sql_statement = "SELECT distinct(table_name) FROM information_schema.tables WHERE table_schema='public'"
    cursor.execute(sql_statement)
    table_names = cursor.fetchall()
    for table in table_names:

        # build a list of each tables columns
        lst_column_names = []
        sql_statement = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' \
                                AND table_name = '{table[0]}'"
        cursor.execute(sql_statement)
        column_names = cursor.fetchall()
        for column in column_names:
            lst_column_names.append(column[0])

        # build a dictionary with tabke name as the key and the list of column names as the value
        dict_table_column[table[0]] = lst_column_names  # lst_columns

        table_columns = dict_table_column

        # loop through table column dictionary and check for empty columns and
        # build a dictionary of table name as key  and list of  empty columns as values
        dict_table_with_empty_column = dict()
        for table_key, columns in table_columns.items():
            for column in columns:
                res = check_if_column_empty(table_key, column, connection)
                if res == 0:
                    dict_table_with_empty_column[table] = column
                    # print('Column', column, '--->', table, 'Is empty')
                else:
                    pass

    # lst_less_table_names = [x for x in lst_table_names if 'book_' in x]
    # return lst_less_table_names

    # return ['facebook_page_insights_day']
    return dict_table_with_empty_column


def check_if_column_empty(table, column, connection):
    """
    This function checks if table column is empty and returns 0 (column empty) or 1 (column not empty)
    :param table:
    :param column:
    :param connection:
    :return: integer 0 or 1
    """
    # connection = db.get_connection()
    cursor = connection.cursor()
    # sql_statement = f'select {column} from {table} where date_of_data > \'2018-12-01\' order by {column} desc limit 1'
    # sql_statement = f'select {column} from {table} where date_of_data > \'2018-12-01\' and {column} <> \'\'  limit 1'
    # sql_statement = f'select {column} from {table} where date_of_data > \'2018-12-01\' and {column} <> 0  limit 1'
    # sql_statement = f'select {column} from {table} where  {column} is not Null limit 1'
    sql_statement = f'select {column} from {table} where  cast({column} as varchar) <> \'\' limit 1'

    cursor.execute(sql_statement)
    column_value = cursor.fetchone()

    if column_value:
        return 1
    else:
        return 0

    cursor.close()

