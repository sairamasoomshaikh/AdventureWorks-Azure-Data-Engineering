-- Create schema
CREATE SCHEMA IF NOT EXISTS adventureworks;
GO

-- Gold Monthly Sales
CREATE EXTERNAL TABLE adventureworks.monthly_sales
(
    Year INT,
    Month INT,
    TotalQuantity BIGINT,
    TotalSales DOUBLE,
    TotalCost DOUBLE,
    TotalProfit DOUBLE
)
WITH
(
    LOCATION = 'gold/monthly_sales/',
    DATA_SOURCE = [AdventureWorksData],
    FILE_FORMAT = [ParquetFormat]
);
GO

-- Gold Product Summary
CREATE EXTERNAL TABLE adventureworks.product_summary
(
    ProductKey INT,
    ProductName VARCHAR(255),
    TotalQuantity BIGINT,
    TotalSales DOUBLE,
    TotalProfit DOUBLE
)
WITH
(
    LOCATION = 'gold/product_returns/',
    DATA_SOURCE = [AdventureWorksData],
    FILE_FORMAT = [ParquetFormat]
);
GO