import duckdb

con = duckdb.connect("netflix_dw.db")

tables = con.execute("""
SHOW TABLES
""").fetchdf()

print("Tables in DuckDB:")
print(tables)

con.close()
