CREATE TABLE IF NOT EXISTS cdc_netflix_tracker AS
SELECT
    show_id,
    hash(title || type || coalesce(director, '') || coalesce(country, '') || coalesce(rating, '')) AS record_hash,
    ingestion_ts AS last_seen_ts
FROM staging_netflix_titles
WHERE 1 = 0;
