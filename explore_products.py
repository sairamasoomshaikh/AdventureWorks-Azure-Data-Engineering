import pandas as pd

products = pd.read_csv(
    "Data/AdventureWorks_Products.csv",
    encoding="latin1"
)

print("Number of rows:", len(products))
print("Number of columns:", len(products.columns))

print("\nColumns:")
print(products.columns.tolist())

print("\nFirst 5 rows:")
print(products.head())

print("\nData types:")
print(products.dtypes)