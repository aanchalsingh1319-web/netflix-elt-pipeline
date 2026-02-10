import duckdb

con = duckdb.connect("netflix_dw.db")

result = con.execute("""
SELECT show_id, title, type, ingestion_ts
FROM raw_netflix_titles
LIMIT 5
""").fetchdf()

print(result)

con.close()
