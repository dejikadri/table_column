import time
import db_conf as db

import list_hopper_tables as ht

connection = db.get_connection()

start_time = time.time()

dict_empty_columns = ht.get_table_column_dict(connection, 'facebook_page_metadata_lifetime')

end_time = int(round(time.time() * 1000))
end_time = time.time()

print(dict_empty_columns)
print("Time: ", round((end_time - start_time) * 1000), "Milliseconds")

db.close_connection(connection)
