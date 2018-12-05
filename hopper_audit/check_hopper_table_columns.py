import list_hopper_tables as ht

table_columns = ht.table_column_dict()
print(type(table_columns))

for table, columns in table_columns.items():
    for column in columns:
        res = ht.check_if_column_empty(table, column)
        if res == 0:
            print('Column', column, '--->', table, 'Is empty')
        else:
            pass


