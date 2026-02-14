import duckdb

con = duckdb.connect("netflix_dw.db")

with open("sql/analytics_kpis.sql") as f:
    con.execute(f.read())

print("✅ Analytics KPI views created")

# Quick preview of KPIs
for view in [
    "kpi_movies_vs_tv",
    "kpi_titles_by_year",
    "kpi_titles_by_country",
    "kpi_titles_by_rating",
    "kpi_top_genres",
    "kpi_genres_over_time",
    "kpi_country_recent_growth"
]:
    print(f"\n--- {view} ---")
    print(con.execute(f"SELECT * FROM {view} LIMIT 5").fetchdf())

con.close()
