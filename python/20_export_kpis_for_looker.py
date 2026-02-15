import duckdb
import os

os.makedirs("bi_exports", exist_ok=True)

con = duckdb.connect("netflix_dw.db")

views = {
    "movies_vs_tv": "kpi_movies_vs_tv",
    "titles_by_year": "kpi_titles_by_year",
    "titles_by_country": "kpi_titles_by_country",
    "top_genres": "kpi_top_genres",
    "country_recent_growth": "kpi_country_recent_growth"
}

for name, view in views.items():
    df = con.execute(f"SELECT * FROM {view}").fetchdf()
    path = f"bi_exports/{name}.csv"
    df.to_csv(path, index=False)
    print(f"✅ Exported {view} → {path}")

con.close()
