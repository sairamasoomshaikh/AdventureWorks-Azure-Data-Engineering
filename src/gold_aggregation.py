from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    month,
    sum,
    year,
)


PROJECT_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_DIR / "output" / "silver"
GOLD_DIR = PROJECT_DIR / "output" / "gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("AdventureWorksGoldAggregation")
        .master("local[*]")
        .getOrCreate()
    )


def load_silver(spark):
    return spark.read.parquet(
        str(SILVER_DIR / "sales_enriched")
    )


def create_monthly_sales(silver):
    return (
        silver
        .withColumn("Year", year(col("OrderDate")))
        .withColumn("Month", month(col("OrderDate")))
        .groupBy("Year", "Month")
        .agg(
            sum("OrderQuantity").alias("TotalQuantity"),
            sum("SalesAmount").alias("TotalSales"),
            sum("CostAmount").alias("TotalCost"),
            sum("ProfitAmount").alias("TotalProfit"),
        )
        .orderBy("Year", "Month")
    )


def create_product_returns(silver):
    return (
        silver
        .groupBy(
            "ProductKey",
            "ProductName",
        )
        .agg(
            sum("OrderQuantity").alias("TotalQuantity"),
            sum("SalesAmount").alias("TotalSales"),
            sum("ProfitAmount").alias("TotalProfit"),
        )
        .orderBy(col("TotalSales").desc())
    )


def write_gold(df, name):
    output_path = GOLD_DIR / name

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Gold data written to: {output_path}")


def main():
    print("Starting Gold aggregation...")

    spark = create_spark_session()

    try:
        silver = load_silver(spark)

        print(f"Silver records loaded: {silver.count():,}")

        monthly_sales = create_monthly_sales(silver)
        product_returns = create_product_returns(silver)

        print(f"Monthly sales records: {monthly_sales.count():,}")
        print(f"Product summary records: {product_returns.count():,}")

        write_gold(monthly_sales, "monthly_sales")
        write_gold(product_returns, "product_returns")

        print("\nGold aggregation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()