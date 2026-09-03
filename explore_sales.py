import pandas as pd

sales = pd.read_csv(
    "Data/AdventureWorks_Sales_2017.csv",
    encoding="latin1"
)

print("Number of rows:", len(sales))
print("Number of columns:", len(sales.columns))

print("\nColumns:")
print(sales.columns.tolist())

print("\nFirst 5 rows:")
print(sales.head())

print("\nData types:")
print(sales.dtypes)