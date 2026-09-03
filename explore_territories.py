import pandas as pd

territories = pd.read_csv(
    "Data/AdventureWorks_Territories.csv",
    encoding="latin1"
)

print("Number of rows:", len(territories))
print("Number of columns:", len(territories.columns))

print("\nColumns:")
print(territories.columns.tolist())

print("\nFirst 5 rows:")
print(territories.head())

print("\nData types:")
print(territories.dtypes)