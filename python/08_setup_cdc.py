import duckdb

con = duckdb.connect("netflix_dw.db")

with open("sql/cdc_setup.sql") as f:
    con.execute(f.read())

print("✅ CDC tracker table created")

con.close()
