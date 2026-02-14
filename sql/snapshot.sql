CREATE OR REPLACE TEMP TABLE staging_netflix_new_snapshot AS
SELECT * FROM staging_netflix_titles;
