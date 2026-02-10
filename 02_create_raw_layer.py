import pandas as pd
from datetime import datetime
import os

# Paths
SOURCE_PATH = "data/raw/netflix_titles.csv"
RAW_PARQUET_PATH = "data/raw/netflix_titles_raw.parquet"

# Read CSV with proper encoding
df = pd.read_csv(SOURCE_PATH, encoding="latin-1")

# Add ingestion timestamp (metadata)
df["ingestion_ts"] = datetime.utcnow()

# Ensure raw folder exists (it already does, but good practice)
os.makedirs("data/raw", exist_ok=True)

# Write to Parquet (RAW layer)
df.to_parquet(RAW_PARQUET_PATH, index=False)

print("✅ RAW layer created at:", RAW_PARQUET_PATH)
print("Rows:", len(df))