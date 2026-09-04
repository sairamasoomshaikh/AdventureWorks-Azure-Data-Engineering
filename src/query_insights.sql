-- Monthly sales
SELECT
    Year,
    Month,
    TotalQuantity,
    TotalSales,
    TotalCost,
    TotalProfit
FROM adventureworks.vw_monthly_sales
ORDER BY Year, Month;


-- Top products by sales
SELECT TOP 10
    ProductKey,
    ProductName,
    TotalQuantity,
    TotalSales,
    TotalProfit
FROM adventureworks.vw_product_performance
ORDER BY TotalSales DESC;


-- Top products by profit
SELECT TOP 10
    ProductKey,
    ProductName,
    TotalQuantity,
    TotalSales,
    TotalProfit
FROM adventureworks.vw_product_performance
ORDER BY TotalProfit DESC;