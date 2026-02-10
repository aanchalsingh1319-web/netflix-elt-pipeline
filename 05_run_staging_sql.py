import duckdb

con = duckdb.connect("netflix_dw.db")

with open("sql/staging.sql", "r") as f:
    staging_sql = f.read()

con.execute(staging_sql)

count = con.execute("SELECT COUNT(*) FROM staging_netflix_titles").fetchone()[0]
print("✅ STAGING table created")
print("Rows in staging_netflix_titles:", count)

con.close()
