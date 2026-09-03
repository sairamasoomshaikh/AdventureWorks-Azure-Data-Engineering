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

# Join sales with products
sales_products = sales.join(
    products,
    sales.ProductKey == products.ProductKey,
    "left"
)
print("\nSales + Products:")
print("Rows:", sales_products.count())

sales_products.select(
    sales.OrderNumber,
    sales.ProductKey,
    products.ProductName,
    products.ProductPrice,
    products.ProductCost
).show(5)
# Join sales + products with customers
sales_products_customers = sales_products.join(
    customers,
    sales_products.CustomerKey == customers.CustomerKey,
    "left"
)

print("\nSales + Products + Customers:")
print("Rows:", sales_products_customers.count())

sales_products_customers.select(
    sales.OrderNumber,
    sales.ProductKey,
    sales.CustomerKey,
    products.ProductName,
    customers.FirstName,
    customers.LastName,
    customers.EmailAddress
).show(5)
# Join with territories
sales_enriched = sales_products_customers.join(
    territories,
    sales_products_customers.TerritoryKey == territories.SalesTerritoryKey,
    "left"
)

print("\nFinal Enriched Sales:")
print("Rows:", sales_enriched.count())

sales_enriched.select(
    sales.OrderNumber,
    sales.ProductKey,
    sales.CustomerKey,
    sales.TerritoryKey,
    products.ProductName,
    customers.FirstName,
    customers.LastName,
    territories.Region,
    territories.Country,
    territories.Continent
).show(5)
# Add business calculations
sales_enriched = sales_enriched.withColumn(
    "SalesAmount",
    col("ProductPrice") * col("OrderQuantity")
)

sales_enriched = sales_enriched.withColumn(
    "CostAmount",
    col("ProductCost") * col("OrderQuantity")
)

sales_enriched = sales_enriched.withColumn(
    "ProfitAmount",
    col("SalesAmount") - col("CostAmount")
)

sales_enriched = sales_enriched.withColumn(
    "ProfitMargin",
    col("ProfitAmount") / col("SalesAmount")
)

print("\nBusiness Calculations:")
sales_enriched.select(
    "OrderNumber",
    "ProductName",
    "OrderQuantity",
    "ProductPrice",
    "ProductCost",
    "SalesAmount",
    "CostAmount",
    "ProfitAmount",
    "ProfitMargin"
).show(5)

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