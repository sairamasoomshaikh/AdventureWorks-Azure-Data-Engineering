from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# Start Spark
spark = (
    SparkSession.builder
    .appName("SalesTransformation")
    .master("local[*]")
    .getOrCreate()
)

# Load sales data
sales = spark.read.csv(
    "Data/AdventureWorks_Sales_2017.csv",
    header=True,
    inferSchema=True
)
# Load products data
products = spark.read.csv(
    "Data/AdventureWorks_Products.csv",
    header=True,
    inferSchema=True
)
# Load customers data
customers = spark.read.csv(
    "Data/AdventureWorks_Customers.csv",
    header=True,
    inferSchema=True
)

# Load territories data
territories = spark.read.csv(
    "Data/AdventureWorks_Territories.csv",
    header=True,
    inferSchema=True
)
# Quick verification
print("\nDataset counts:")
print("Sales:", sales.count())
print("Products:", products.count())
print("Customers:", customers.count())
print("Territories:", territories.count())
# Convert string dates into proper date columns
sales = sales.withColumn(
    "OrderDate",
    to_date(col("OrderDate"), "M/d/yyyy")
)

sales = sales.withColumn(
    "StockDate",
    to_date(col("StockDate"), "M/d/yyyy")
)

# -----------------------------
# Data Quality Checks
# -----------------------------

print("\nData Quality Checks:")

# 1. Check for missing OrderNumber
null_orders = sales.filter(
    col("OrderNumber").isNull()
).count()

print("Null OrderNumbers:", null_orders)

# 2. Check for missing ProductKey
null_products = sales.filter(
    col("ProductKey").isNull()
).count()

print("Null ProductKeys:", null_products)

# 3. Check for missing CustomerKey
null_customers = sales.filter(
    col("CustomerKey").isNull()
).count()

print("Null CustomerKeys:", null_customers)

# 4. Check for invalid quantities
invalid_quantity = sales.filter(
    col("OrderQuantity") <= 0
).count()

print("Invalid OrderQuantities:", invalid_quantity)



# Stop Spark
spark.stop()