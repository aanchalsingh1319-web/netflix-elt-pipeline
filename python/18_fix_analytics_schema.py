import duckdb

con = duckdb.connect("netflix_dw.db")

# Recreate analytics table without rn column
con.execute("""
CREATE OR REPLACE TABLE analytics_netflix_current_clean AS
SELECT
    show_id, type, title, director, "cast", country, date_added,
    release_year, rating, duration, listed_in, description, ingestion_ts
FROM analytics_netflix_current
""")

con.execute("DROP TABLE analytics_netflix_current")

con.execute("""
ALTER TABLE analytics_netflix_current_clean
RENAME TO analytics_netflix_current
""")

print("✅ Fixed analytics_netflix_current schema (removed rn column)")

con.close()
