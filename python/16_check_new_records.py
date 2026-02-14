import duckdb

con = duckdb.connect("netflix_dw.db")

df = con.execute("""
SELECT show_id, title, rating
FROM analytics_netflix_current
WHERE show_id = 's_new_001'
""").fetchdf()

print(df)

con.close()
