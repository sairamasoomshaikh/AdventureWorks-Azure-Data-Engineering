from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, round

# ============================================================
# 1. START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("AaysAzureDataEngineering")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 2. LOAD RAW DATA
# ============================================================

sales = spark.read.csv(
    "Data/AdventureWorks_Sales_2017.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    "Data/AdventureWorks_Products.csv",
    header=True,
    inferSchema=True
)

customers = spark.read.csv(
    "Data/AdventureWorks_Customers.csv",
    header=True,
    inferSchema=True
)

territories = spark.read.csv(
    "Data/AdventureWorks_Territories.csv",
    header=True,
    inferSchema=True
)

print("\n========== DATASET COUNTS ==========")
print("Sales:", sales.count())
print("Products:", products.count())
print("Customers:", customers.count())
print("Territories:", territories.count())

# ============================================================
# 3. TRANSFORM DATES
# ============================================================

sales = sales.withColumn(
    "OrderDate",
    to_date(col("OrderDate"), "M/d/yyyy")
)

sales = sales.withColumn(
    "StockDate",
    to_date(col("StockDate"), "M/d/yyyy")
)

# ============================================================
# 4. JOIN SALES + PRODUCTS
# ============================================================

sales_products = sales.join(
    products,
    sales.ProductKey == products.ProductKey,
    "left"
)

# ============================================================
# 5. JOIN CUSTOMERS
# ============================================================

sales_products_customers = sales_products.join(
    customers,
    sales_products.CustomerKey == customers.CustomerKey,
    "left"
)

# ============================================================
# 6. JOIN TERRITORIES
# ============================================================

sales_enriched = sales_products_customers.join(
    territories,
    sales_products_customers.TerritoryKey
    == territories.SalesTerritoryKey,
    "left"
)

# ============================================================
# 7. SELECT FINAL COLUMNS
# ============================================================

sales_enriched = sales_enriched.select(
    sales.OrderDate,
    sales.StockDate,
    sales.OrderNumber,
    sales.OrderLineItem,
    sales.ProductKey,
    sales.CustomerKey,
    sales.TerritoryKey,

    products.ProductSKU,
    products.ProductName,
    products.ProductSubcategoryKey,
    products.ProductColor,
    products.ProductSize,
    products.ProductStyle,
    products.ProductCost,
    products.ProductPrice,

    customers.FirstName,
    customers.LastName,
    customers.EmailAddress,
    customers.AnnualIncome,
    customers.TotalChildren,
    customers.EducationLevel,
    customers.Occupation,
    customers.HomeOwner,

    territories.Region,
    territories.Country,
    territories.Continent,

    sales.OrderQuantity
)

# ============================================================
# 8. BUSINESS CALCULATIONS
# ============================================================

sales_enriched = sales_enriched.withColumn(
    "SalesAmount",
    round(col("ProductPrice") * col("OrderQuantity"), 2)
)

sales_enriched = sales_enriched.withColumn(
    "CostAmount",
    round(col("ProductCost") * col("OrderQuantity"), 2)
)

sales_enriched = sales_enriched.withColumn(
    "ProfitAmount",
    round(col("SalesAmount") - col("CostAmount"), 2)
)

sales_enriched = sales_enriched.withColumn(
    "ProfitMargin",
    round(
        col("ProfitAmount") / col("SalesAmount"),
        4
    )
)

# ============================================================
# 9. DATA QUALITY CHECKS
# ============================================================

print("\n========== DATA QUALITY CHECKS ==========")

null_orders = sales_enriched.filter(
    col("OrderNumber").isNull()
).count()

null_products = sales_enriched.filter(
    col("ProductName").isNull()
).count()

null_customers = sales_enriched.filter(
    col("FirstName").isNull()
).count()

null_territories = sales_enriched.filter(
    col("Region").isNull()
).count()

invalid_quantity = sales_enriched.filter(
    col("OrderQuantity") <= 0
).count()

invalid_sales = sales_enriched.filter(
    col("SalesAmount") <= 0
).count()

invalid_profit_margin = sales_enriched.filter(
    col("SalesAmount") == 0
).count()

print("Null OrderNumbers:", null_orders)
print("Unmatched Products:", null_products)
print("Unmatched Customers:", null_customers)
print("Unmatched Territories:", null_territories)
print("Invalid OrderQuantities:", invalid_quantity)
print("Invalid SalesAmounts:", invalid_sales)
print("Zero SalesAmount records:", invalid_profit_margin)

# ============================================================
# 10. DISPLAY FINAL DATA
# ============================================================

print("\n========== FINAL ENRICHED DATA ==========")

print("Final row count:", sales_enriched.count())
print("Final column count:", len(sales_enriched.columns))

sales_enriched.select(
    "OrderDate",
    "OrderNumber",
    "ProductName",
    "OrderQuantity",
    "ProductPrice",
    "SalesAmount",
    "CostAmount",
    "ProfitAmount",
    "ProfitMargin",
    "FirstName",
    "LastName",
    "Region",
    "Country"
).show(10, truncate=False)

# ============================================================
# 11. WRITE TRANSFORMED DATA TO PARQUET
# ============================================================

output_path = "output/sales_enriched"

sales_enriched.write \
    .mode("overwrite") \
    .parquet(output_path)

print("\n========== ETL COMPLETE ==========")
print("Output written to:", output_path)

# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()