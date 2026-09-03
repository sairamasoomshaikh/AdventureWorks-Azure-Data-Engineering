import pandas as pd

# Load the customers dataset
customers = pd.read_csv(
    "Data/AdventureWorks_Customers.csv",
    encoding="latin1"
)

print("Number of rows:", len(customers))
print("Number of columns:", len(customers.columns))

print("\nColumns:")
print(customers.columns.tolist())

print("\nFirst 5 rows:")
print(customers.head())