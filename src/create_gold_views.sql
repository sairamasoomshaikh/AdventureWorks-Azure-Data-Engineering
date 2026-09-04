CREATE OR ALTER VIEW adventureworks.vw_monthly_sales
AS
SELECT
    Year,
    Month,
    TotalQuantity,
    TotalSales,
    TotalCost,
    TotalProfit
FROM adventureworks.monthly_sales;
GO


CREATE OR ALTER VIEW adventureworks.vw_product_performance
AS
SELECT
    ProductKey,
    ProductName,
    TotalQuantity,
    TotalSales,
    TotalProfit
FROM adventureworks.product_summary;
GO