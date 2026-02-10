import pandas as pd

# Read the raw CSV (Handled Encoding)
df = pd.read_csv("data/raw/netflix_titles.csv", encoding="latin-1")

# Print basic info
print("Shape (rows, columns):", df.shape)
print("\nColumns:")
print(df.columns.tolist())

print("\nFirst 5 rows:")
print(df.head())
