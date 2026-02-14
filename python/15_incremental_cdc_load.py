import duckdb

con = duckdb.connect("netflix_dw.db")

# Count rows before
before_count = con.execute("SELECT COUNT(*) FROM analytics_netflix_current").fetchone()[0]

# 1. Insert new records
con.execute("""
INSERT INTO analytics_netflix_current (
    show_id, type, title, director, "cast", country, date_added,
    release_year, rating, duration, listed_in, description, ingestion_ts
)
SELECT
    s.show_id, s.type, s.title, s.director, s."cast", s.country, s.date_added,
    s.release_year, s.rating, s.duration, s.listed_in, s.description, s.ingestion_ts
FROM staging_netflix_titles s
LEFT JOIN cdc_netflix_tracker t
  ON s.show_id = t.show_id
WHERE t.show_id IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM analytics_netflix_current a
      WHERE a.show_id = s.show_id
  )
""")

# Count rows after inserts
after_insert_count = con.execute("SELECT COUNT(*) FROM analytics_netflix_current").fetchone()[0]
new_inserts = after_insert_count - before_count

# 2. Update changed records
con.execute("""
UPDATE analytics_netflix_current a
SET
    type = s.type,
    title = s.title,
    director = s.director,
    "cast" = s."cast",
    country = s.country,
    date_added = s.date_added,
    release_year = s.release_year,
    rating = s.rating,
    duration = s.duration,
    listed_in = s.listed_in,
    description = s.description,
    ingestion_ts = s.ingestion_ts
FROM staging_netflix_titles s
JOIN cdc_netflix_tracker t
  ON s.show_id = t.show_id
WHERE a.show_id = s.show_id
  AND hash(
        s.title || s.type || coalesce(s.director, '') ||
        coalesce(s.country, '') || coalesce(s.rating, '')
      ) <> t.record_hash
""")

# We can estimate updated rows by counting mismatches beforehand (optional),
# but for now we'll just log that updates were attempted.
updated_estimate = con.execute("""
SELECT COUNT(*)
FROM staging_netflix_titles s
JOIN cdc_netflix_tracker t
  ON s.show_id = t.show_id
WHERE hash(
        s.title || s.type || coalesce(s.director, '') ||
        coalesce(s.country, '') || coalesce(s.rating, '')
      ) <> t.record_hash
""").fetchone()[0]

# 3. Refresh CDC tracker for new + changed records
con.execute("""
DELETE FROM cdc_netflix_tracker
WHERE show_id IN (
    SELECT s.show_id
    FROM staging_netflix_titles s
    LEFT JOIN cdc_netflix_tracker t
      ON s.show_id = t.show_id
    WHERE t.show_id IS NULL
       OR hash(
            s.title || s.type || coalesce(s.director, '') ||
            coalesce(s.country, '') || coalesce(s.rating, '')
          ) <> t.record_hash
)
""")

con.execute("""
INSERT INTO cdc_netflix_tracker (show_id, record_hash, last_seen_ts)
SELECT
    show_id,
    hash(
        title || type || coalesce(director, '') ||
        coalesce(country, '') || coalesce(rating, '')
    ) AS record_hash,
    ingestion_ts
FROM staging_netflix_titles
WHERE show_id IN (
    SELECT s.show_id
    FROM staging_netflix_titles s
    LEFT JOIN cdc_netflix_tracker t
      ON s.show_id = t.show_id
    WHERE t.show_id IS NULL
       OR hash(
            s.title || s.type || coalesce(s.director, '') ||
            coalesce(s.country, '') || coalesce(s.rating, '')
          ) <> t.record_hash
)
""")

print(f"✅ CDC Incremental Load Complete: Inserted={new_inserts}, Updated≈{updated_estimate}")

con.close()
