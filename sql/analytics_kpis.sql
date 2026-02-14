-- 1) Movies vs TV Shows
CREATE OR REPLACE VIEW kpi_movies_vs_tv AS
SELECT
  type,
  COUNT(*) AS total_titles
FROM analytics_netflix_current
GROUP BY type;

-- 2) Content growth by release year
CREATE OR REPLACE VIEW kpi_titles_by_year AS
SELECT
  release_year,
  COUNT(*) AS total_titles
FROM analytics_netflix_current
GROUP BY release_year
ORDER BY release_year;

-- 3) Top countries by number of titles
CREATE OR REPLACE VIEW kpi_titles_by_country AS
SELECT
  TRIM(country) AS country,
  COUNT(*) AS total_titles
FROM analytics_netflix_current
WHERE country IS NOT NULL
GROUP BY TRIM(country)
ORDER BY total_titles DESC
LIMIT 10;

-- 4) Ratings distribution
CREATE OR REPLACE VIEW kpi_titles_by_rating AS
SELECT
  rating,
  COUNT(*) AS total_titles
FROM analytics_netflix_current
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY total_titles DESC;

-- 5) Top genres (listed_in contains comma-separated genres)
CREATE OR REPLACE VIEW kpi_top_genres AS
SELECT
  TRIM(genre) AS genre,
  COUNT(*) AS total_titles
FROM (
  SELECT
    UNNEST(STRING_SPLIT(listed_in, ',')) AS genre
  FROM analytics_netflix_current
  WHERE listed_in IS NOT NULL
)
GROUP BY TRIM(genre)
ORDER BY total_titles DESC
LIMIT 10;

-- 6) Top genres over time (by release year)
CREATE OR REPLACE VIEW kpi_genres_over_time AS
SELECT
  release_year,
  TRIM(genre) AS genre,
  COUNT(*) AS total_titles
FROM (
  SELECT
    release_year,
    UNNEST(STRING_SPLIT(listed_in, ',')) AS genre
  FROM analytics_netflix_current
  WHERE listed_in IS NOT NULL
    AND release_year IS NOT NULL
)
GROUP BY release_year, TRIM(genre)
ORDER BY release_year, total_titles DESC;

-- 7) Top countries by recent content growth (last 5 years)
CREATE OR REPLACE VIEW kpi_country_recent_growth AS
SELECT
  TRIM(country) AS country,
  COUNT(*) AS total_titles_recent
FROM analytics_netflix_current
WHERE country IS NOT NULL
  AND release_year >= (SELECT MAX(release_year) - 5 FROM analytics_netflix_current)
GROUP BY TRIM(country)
ORDER BY total_titles_recent DESC
LIMIT 10;
