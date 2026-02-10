import duckdb

DB_PATH = "netflix_dw.db"
RAW_PARQUET_PATH = "data/raw/netflix_titles_raw.parquet"

# Connect to DuckDB (this creates the DB file if it doesn't exist)
con = duckdb.connect(DB_PATH)

# Create RAW table from Parquet
con.execute("""
CREATE TABLE IF NOT EXISTS raw_netflix_titles AS
SELECT * FROM read_parquet(?)
""", [RAW_PARQUET_PATH])

# Simple validation
row_count = con.execute("SELECT COUNT(*) FROM raw_netflix_titles").fetchone()[0]
print("✅ Loaded RAW table into DuckDB")
print("Rows in raw_netflix_titles:", row_count)

con.close()
