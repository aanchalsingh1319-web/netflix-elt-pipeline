import duckdb

con = duckdb.connect("netflix_dw.db")

with open("sql/analytics_setup.sql") as f:
    con.execute(f.read())

print("✅ analytics_netflix_current table created")

con.close()
