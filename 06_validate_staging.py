import duckdb

con = duckdb.connect("netflix_dw.db")

# Get column names
cols = con.execute("""
PRAGMA table_info('staging_netflix_titles')
""").fetchdf()

print("Columns in staging:")
print(cols[["name", "type"]])

con.close()
