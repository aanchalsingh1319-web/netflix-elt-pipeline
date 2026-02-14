import duckdb

con = duckdb.connect("netflix_dw.db")

with open("sql/snapshot.sql") as f:
    con.execute(f.read())

print("✅ New snapshot table created (simulated)")

con.close()
