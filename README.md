# Netflix ELT Pipeline (DuckDB + Python + SQL+ BigQuery)


## Why This Project?
This project demonstrates how to design and implement a production-style ELT data pipeline locally using modern data engineering patterns (raw/staging layers, warehouse-based transformations, data quality monitoring, and automation-ready scheduling).


## Overview
This project implements an end-to-end ELT (Extract, Load, Transform) data pipeline using Netflix content data. The pipeline ingests raw CSV data, stores it in a Parquet-based raw layer, loads it into an analytical warehouse (DuckDB), and transforms it into cleaned, analytics-ready tables using SQL. The project also includes data quality monitoring, logging, and automation-ready scheduling.

## Architecture
Source (CSV) → RAW Layer (Parquet) → Warehouse (DuckDB) → STAGING (SQL) → Analytics (KPIs) → Looker (Explores & Dashboards)
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


## Incremental Loads (CDC Simulation)

The pipeline supports CDC-style incremental updates for the analytics layer:
- New records are inserted using primary-key detection (`show_id`)
- Changed records are detected via hash-based comparisons and updated
- Unchanged records are skipped
- Jobs are idempotent and safe to re-run without creating duplicates
- Schema drift is handled using explicit column mapping

## Analytics Layer (KPIs)

Built an analytics layer on top of the incremental CDC table with KPI views:
- Movies vs TV Shows distribution  
- Titles by release year (growth trend)  
- Top countries by content volume  
- Ratings distribution  
- Top genres (derived from listed_in)

# lookml/netflix.model.lkml
connection: "bigquery_connection"

explore: analytics_netflix_current {
  label: "Netflix Content Explore"
}
# lookml/analytics_netflix_current.view.lkml
view: analytics_netflix_current {
  sql_table_name: analytics_netflix_current ;;

  dimension: show_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.show_id ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension: release_year {
    type: number
    sql: ${TABLE}.release_year ;;
  }

  measure: total_titles {
    type: count
  }
}

Designed LookML models for analytics tables to support Looker Explores and dashboards.

  ## BI Dashboard (Looker Studio)

The analytics layer is designed to integrate with Looker for business intelligence use cases.  
In production, Looker would connect to the cloud warehouse (BigQuery/Snowflake) hosting the analytics tables and KPI views, enabling dashboards for:

- Content mix (Movies vs TV Shows)  
- Content growth trends by year  
- Regional content distribution  
- Genre popularity  
- Recent country-wise growth  

For hands-on validation, KPI datasets are exported from DuckDB and loaded into BigQuery, where Looker Studio dashboards are built on top of the analytics outputs & business-friendly layout.
[View Dashboard](https://lookerstudio.google.com/s/j5aGDRbywzs)

## Sample Insights

- International content and dramas dominate the Netflix catalog, highlighting a strong global content strategy.
- Genre trends over time show increasing diversification in recent years, with strong growth in international movies and documentaries.
- Recent content growth is led by the United States, with notable momentum in India and South Korea, reflecting regional market expansion.

## Advanced Analytics

- Genre popularity trends over time (multi-valued field explosion + time-based aggregation)
- Top countries contributing to recent content growth (last 5 years)
  
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












