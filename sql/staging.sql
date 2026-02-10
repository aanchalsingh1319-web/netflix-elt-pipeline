CREATE TABLE IF NOT EXISTS staging_netflix_titles AS
SELECT
    show_id,
    type,
    TRIM(title) AS title,
    director,
    "cast",
    country,
    date_added,
    release_year,
    rating,
    duration,
    listed_in,
    description,
    ingestion_ts
FROM raw_netflix_titles;
