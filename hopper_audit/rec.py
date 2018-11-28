import records

db = records.Database('postgres://...')
rows = db.query('select * from active_users')

