from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


# Project directories
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "Data"
BRONZE_DIR = PROJECT_DIR / "output" / "bronze"
SILVER_DIR = PROJECT_DIR / "output" / "silver"


def create_spark_session():
    """Create a local Spark session."""

    return (
        SparkSession.builder
        .appName("AdventureWorksSilverTransformation")
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark):
    """Load Bronze sales and raw dimension data."""

    sales = spark.read.parquet(
        str(BRONZE_DIR / "sales")
    )

    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(DATA_DIR / "AdventureWorks_Products.csv"))
    )

    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "ISO-8859-1")
        .csv(str(DATA_DIR / "AdventureWorks_Customers.csv"))
    )

    territories = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(DATA_DIR / "AdventureWorks_Territories.csv"))
    )

    return sales, products, customers, territories


def transform_sales(sales, products, customers, territories):
    """Clean and enrich Bronze sales data."""

    # Parse date columns
    sales = (
        sales
        .withColumn("OrderDate", to_date(col("OrderDate"), "M/d/yyyy"))
        .withColumn("StockDate", to_date(col("StockDate"), "M/d/yyyy"))
    )

    # Rename dimension columns to avoid ambiguity
    products = products.select(
        "ProductKey",
        "ProductSubcategoryKey",
        "ProductSKU",
        "ProductName",
        "ModelName",
        "ProductColor",
        "ProductSize",
        "ProductStyle",
        "ProductCost",
        "ProductPrice",
    )

    customers = customers.select(
        "CustomerKey",
        "FirstName",
        "LastName",
        "Gender",
        "EmailAddress",
        "AnnualIncome",
        "EducationLevel",
        "Occupation",
        "HomeOwner",
    )

    territories = territories.select(
        col("SalesTerritoryKey").alias("TerritoryKey"),
        "Region",
        "Country",
        "Continent",
    )

    # Enrich sales with product information
    silver = sales.join(
        products,
        on="ProductKey",
        how="left",
    )

    # Enrich with customer information
    silver = silver.join(
        customers,
        on="CustomerKey",
        how="left",
    )

    # Enrich with territory information
    silver = silver.join(
        territories,
        on="TerritoryKey",
        how="left",
    )

    # Create row-level business measures
    silver = (
        silver
        .withColumn(
            "SalesAmount",
            col("OrderQuantity") * col("ProductPrice")
        )
        .withColumn(
            "CostAmount",
            col("OrderQuantity") * col("ProductCost")
        )
        .withColumn(
            "ProfitAmount",
            col("SalesAmount") - col("CostAmount")
        )
    )

    return silver


def run_quality_checks(silver):
    """Run basic Silver data-quality checks."""

    print("\nSilver data-quality checks:")

    null_orders = silver.filter(
        col("OrderNumber").isNull()
    ).count()

    invalid_quantity = silver.filter(
        col("OrderQuantity") <= 0
    ).count()

    unmatched_products = silver.filter(
        col("ProductName").isNull()
    ).count()

    unmatched_customers = silver.filter(
        col("FirstName").isNull()
    ).count()

    unmatched_territories = silver.filter(
        col("Region").isNull()
    ).count()

    print(f"Null OrderNumber records: {null_orders:,}")
    print(f"Invalid quantity records: {invalid_quantity:,}")
    print(f"Unmatched products: {unmatched_products:,}")
    print(f"Unmatched customers: {unmatched_customers:,}")
    print(f"Unmatched territories: {unmatched_territories:,}")


def write_silver(silver):
    """Write the Silver dataset as Parquet."""

    output_path = SILVER_DIR / "sales_enriched"

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    (
        silver.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"\nSilver data written to: {output_path}")


def main():
    """Run Silver transformation."""

    print("Starting Silver transformation...")

    spark = create_spark_session()

    try:
        sales, products, customers, territories = load_data(spark)

        print(f"Bronze sales records loaded: {sales.count():,}")

        silver = transform_sales(
            sales,
            products,
            customers,
            territories,
        )

        print(f"Silver records produced: {silver.count():,}")

        run_quality_checks(silver)

        print("\nSilver schema:")
        silver.printSchema()

        write_silver(silver)

        print("\nSilver transformation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()