import duckdb
from datetime import datetime, timedelta

con = duckdb.connect("netflix_dw.db")

alerts = []

# 1. Pipeline row count check
raw_count = con.execute("SELECT COUNT(*) FROM raw_netflix_titles").fetchone()[0]
stg_count = con.execute("SELECT COUNT(*) FROM staging_netflix_titles").fetchone()[0]

if raw_count != stg_count:
    alerts.append(f"Row count mismatch: RAW={raw_count}, STAGING={stg_count}")

# 2. Null check on critical columns
null_titles = con.execute("""
SELECT COUNT(*) FROM staging_netflix_titles WHERE title IS NULL
""").fetchone()[0]

if null_titles > 0:
    alerts.append(f"Null titles detected: {null_titles}")

# 3. Freshness check (last ingestion within 1 day)
last_ingestion = con.execute("""
SELECT MAX(ingestion_ts) FROM raw_netflix_titles
""").fetchone()[0]

if last_ingestion is None or last_ingestion < datetime.utcnow() - timedelta(days=1):
    alerts.append("Data freshness issue: No recent ingestion in last 24 hours")	

# 4. Print results

run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

if alerts:
    print(f"[{run_ts}] 🚨 DATA PIPELINE ALERTS:")
    for a in alerts:
        print(f"[{run_ts}] - {a}")
else:
    print(f"[{run_ts}] ✅ All monitoring checks passed")
