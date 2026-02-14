CREATE TABLE IF NOT EXISTS analytics_netflix_current AS
SELECT *
FROM staging_netflix_titles
WHERE 1 = 0;
