# Netflix ELT Pipeline (DuckDB + Python + SQL)


## Why This Project?
This project demonstrates how to design and implement a production-style ELT data pipeline locally using modern data engineering patterns (raw/staging layers, warehouse-based transformations, data quality monitoring, and automation-ready scheduling).


## Overview
This project implements an end-to-end ELT (Extract, Load, Transform) data pipeline using Netflix content data. The pipeline ingests raw CSV data, stores it in a Parquet-based raw layer, loads it into an analytical warehouse (DuckDB), and transforms it into cleaned, analytics-ready tables using SQL. The project also includes data quality monitoring, logging, and automation-ready scheduling.

## Architecture
Source (CSV) → RAW Layer (Parquet) → Warehouse (DuckDB) → STAGING (SQL) → Analytics (KPIs)  
Monitoring & Logging → Automated health checks with timestamped logs

## Tech Stack
- Python (pandas, duckdb, pyarrow)
- DuckDB (local analytical warehouse)
- SQL (ELT transformations)
- Parquet (raw data lake format)
- Linux / Cron (job scheduling simulation)

## Pipeline Flow
1. **Ingestion (Python)**
   - Read Netflix CSV with encoding handling
   - Add ingestion metadata (timestamp)
   - Persist raw data to Parquet

2. **Warehouse Load (DuckDB)**
   - Load Parquet data into `raw_netflix_titles` table
   - Validate row counts

3. **Staging Layer (SQL)**
   - Clean raw data
   - Drop junk columns
   - Standardize fields and trim text
   - Create `staging_netflix_titles`

4. **Monitoring & Data Quality**
   - Row count validation (RAW vs STAGING)
   - Null checks on critical columns
   - Freshness checks on ingestion timestamps
   - Timestamped logging
   - Automation-ready via cron

## Example Data Quality Checks
- Row count consistency between raw and staging layers  
- Null checks on title and key fields  
- Freshness checks to detect stale ingestions  

## Project Structure

netflix-elt-pipeline/
│
├── data/
│ └── raw/
│ ├── netflix_titles.csv
│ └── netflix_titles_raw.parquet
│
├── python/
│ ├── 01_inspect_data.py
│ ├── 02_create_raw_layer.py
│ ├── 03_load_raw_to_duckdb.py
│ ├── 05_run_staging_sql.py
│ └── 07_monitor_pipeline.py
│
├── sql/
│ └── staging.sql
│
├── logs/
│ └── monitor.log
│
├── netflix_dw.db
├── requirements.txt
└── README.md


## 🔁 Incremental Loads (CDC Simulation)

The pipeline supports CDC-style incremental updates for the analytics layer:
- New records are inserted using primary-key detection (`show_id`)
- Changed records are detected via hash-based comparisons and updated
- Unchanged records are skipped
- Jobs are idempotent and safe to re-run without creating duplicates
- Schema drift is handled using explicit column mapping

## How to Run
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Inspect data
python python/01_inspect_data.py

# Create RAW layer
python python/02_create_raw_layer.py

# Load RAW into DuckDB
python python/03_load_raw_to_duckdb.py

# Create STAGING layer
python python/05_run_staging_sql.py

# Run monitoring checks
python python/07_monitor_pipeline.py

### **BUISNESS QUESTIONS ENABLED**

 - Movies vs TV Shows distribution

 - Content growth by release year

 - Country-wise content distribution

 - Rating distribution

 - Genre analysis


#### **Future Enhancements**

- Add analytics layer (fact/dimension modeling)

- Integrate BI dashboards

- Add schema change detection

- Migrate pipeline to cloud warehouse (Snowflake/BigQuery)





