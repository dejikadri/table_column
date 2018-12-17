def get_table_column_dict(connection, table_list=None):
    """
    This function loops through a list of tables and returns the below
    :return: a python dictionary with table names as key and a list of empty columns as values
    """
    cursor = connection.cursor()

    dict_table_column = dict()

    # Get list of all tables
    if table_list:
        table_names = [(table_list,) for table_list in table_list]
    else:
        table_names = get_table_list(connection)

    for table in table_names:

        # build a list of each tables columns
        lst_column_names = []
        sql_statement = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' \
                                    AND table_name = '{table[0]}'"
        cursor.execute(sql_statement)
        column_names = cursor.fetchall()
        for column in column_names:
            lst_column_names.append((column[0], column[1]))

        # build a dictionary with table names as the keys and the list of column names as the values
        dict_table_column[table[0]] = lst_column_names
    cursor.close()
    dict_table_with_empty_column = build_table_column_dict(dict_table_column, connection)

    return dict_table_with_empty_column


def build_table_column_dict(table_columns_dict, connection):
    # loop through table column dictionary and check for empty columns and
    # build a dictionary of table name as key  and list of  empty columns as values
    dict_table_with_empty_column = dict()
    lst_of_columns = []
    for table_key, columns in table_columns_dict.items():
        for column in columns:
            # check if the column is empty
            empty_status = check_if_column_empty(table_key, column, connection)

            # build dictionary of table name as keys and
            # list of empty columns as values
            if empty_status == 0:
                lst_of_columns.append(column[0])
                dict_table_with_empty_column[table_key] = lst_of_columns

    return dict_table_with_empty_column


def check_if_column_empty(table, column, connection):
    """
    This function checks if table column is empty and returns 0 (column empty) or 1 (column not empty)
    :param table:
    :param column:
    :param connection:
    :return: integer 0 or 1
    """

    cursor = connection.cursor()
    if column[1] == 'boolean':
        sql_statement = f'select \"{column[0]}\" from {table} where  \"{column[0]}\" is not Null limit 1'

    elif column[1] in ['SMALLINT', 'INTEGER', 'BIGINT', 'NUMERIC', 'REAL', 'DOUBLE PRECISION']:
        sql_statement = f'select \"{column[0]}\" from {table} where \"{column[0]}\" <> 0 or \
                        \"{column[0]}\" is not Null limit 1'

    else:
        sql_statement = f'select \"{column[0]}\" from {table} where  \"{column[0]}\" :: varchar <> \'\' or \
                        \"{column[0]}\" :: varchar is not Null  order by \"{column[0]}\" desc limit 1'

    cursor.execute(sql_statement)
    column_value = cursor.fetchone()
    if column_value:
        if column_value[0] == '':
            return 0
        elif column_value[0] == 0:
            return 0
        else:
            return 1
    else:
        return 0

    cursor.close()


def get_table_list(connection):
    """
        This function gets a  list of tables in the bi database
        :return: a python list of tuples of  table names
        """
    cursor = connection.cursor()

    # Get list of all tables
    sql_statement = "SELECT distinct(table_name) FROM information_schema.tables   \
                        WHERE table_schema='public'  and (table_name like 'instagram_%' or  \
                        table_name like '%facebook_%') and table_type='BASE TABLE'"

    cursor.execute(sql_statement)
    table_names = cursor.fetchall()

    return table_names


def get_tables_without_dateofdata(connection):
    """
    This function returns a set of tables that do not
    contain the field date_of_data
    :param connection:
    :return: set of table names
    """
    set_tables_without_dateofdata = set()

    tables = get_table_column_dictionary(connection)
    for tb, cl in tables.items():
        if 'date_of_data' not in cl:
            set_tables_without_dateofdata.add(tb)

    return set_tables_without_dateofdata


def get_table_column_dictionary(connection, table_list=None):
    """
    This function loops through a list of tables and returns the below
    :return: a python dictionary with table names as key and a list of empty columns as values
    """
    cursor = connection.cursor()

    dict_table_column = dict()

    # Get list of all tables
    if table_list:
        table_names = [(table_list,) for table_list in table_list]
    else:
        table_names = get_table_list(connection)

    for table in table_names:

        # build a list of each tables columns
        lst_column_names = []
        sql_statement = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' \
                                    AND table_name = '{table[0]}'"
        cursor.execute(sql_statement)
        column_names = cursor.fetchall()
        for column in column_names:
            lst_column_names.append(column[0])

        # build a dictionary with table names as the keys and the list of column names as the values
        dict_table_column[table[0]] = lst_column_names
    return dict_table_column
