import duckdb

con = duckdb.connect("netflix_dw.db")

# Simulate an update: change rating for one show
con.execute("""
UPDATE staging_netflix_titles
SET rating = 'PG-13'
WHERE show_id = (SELECT show_id FROM staging_netflix_titles LIMIT 1)
""")

# Simulate an insert: add a new fake record
con.execute("""
INSERT INTO staging_netflix_titles (
    show_id, type, title, director, "cast", country, date_added,
    release_year, rating, duration, listed_in, description, ingestion_ts
)
SELECT
    's_new_001', 'Movie', 'My New Netflix Movie', 'Test Director',
    'Test Cast', 'India', '2026-02-11',
    2026, 'PG', '120 min', 'Drama', 'Test description', now()
""")

print("✅ Simulated one update and one insert in staging")

con.close()
