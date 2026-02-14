import duckdb

con = duckdb.connect("netflix_dw.db")

con.execute("""
CREATE OR REPLACE TABLE analytics_netflix_current_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY show_id ORDER BY ingestion_ts DESC) AS rn
    FROM analytics_netflix_current
)
WHERE rn = 1
""")

con.execute("""
DROP TABLE analytics_netflix_current
""")

con.execute("""
ALTER TABLE analytics_netflix_current_dedup
RENAME TO analytics_netflix_current
""")

print("✅ Deduplicated analytics_netflix_current based on show_id")

con.close()
