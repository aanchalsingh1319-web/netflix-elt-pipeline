import duckdb

con = duckdb.connect("netflix_dw.db")

# Check if analytics table is empty
count = con.execute("SELECT COUNT(*) FROM analytics_netflix_current").fetchone()[0]

if count == 0:
    # Initial full load
    con.execute("""
        INSERT INTO analytics_netflix_current
        SELECT * FROM staging_netflix_titles
    """)

    # Initialize CDC tracker with hashes
    con.execute("""
        INSERT INTO cdc_netflix_tracker (show_id, record_hash, last_seen_ts)
        SELECT
            show_id,
            hash(
                title || type || coalesce(director, '') || coalesce(country, '') || coalesce(rating, '')
            ) AS record_hash,
            ingestion_ts
        FROM staging_netflix_titles
    """)

    print("✅ Initial load completed: analytics table and CDC tracker initialized")
else:
    print("ℹ️ analytics_netflix_current already has data, skipping initial load")

con.close()
