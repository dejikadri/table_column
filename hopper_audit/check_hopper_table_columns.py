import list_hopper_tables as ht

table_columns = ht.table_column_dict()

dict_table_with_empty_column = dict()
for table, columns in table_columns.items():
    for column in columns:
        res = ht.check_if_column_empty(table, column)
        if res == 0:
            dict_table_with_empty_column[table] = column
            print('Column', column, '--->', table, 'Is empty')
        else:
            pass

print(dict_table_with_empty_column)


