# AdventureWorks Azure Data Engineering Pipeline

End-to-end Azure data engineering project based on the AdventureWorks sales dataset.

```text
AdventureWorks CSV
       ↓
Azure Data Factory
       ↓
ADLS Gen2
       ↓
Bronze
       ↓
Azure Databricks / PySpark
       ↓
Silver
       ↓
Gold
       ↙       ↘
Azure Synapse   Power BI
Serverless SQL  Dashboard
Project Overview

This project demonstrates an end-to-end data engineering workflow using Python, PySpark, SQL, Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Azure Synapse Analytics, Parquet, and Power BI.

The pipeline ingests AdventureWorks sales data, stores it in a Bronze layer, transforms and validates the data using PySpark, produces analytical Gold datasets, exposes the data through Synapse Serverless SQL, and provides a Power BI reporting layer.

The project follows a Bronze/Silver/Gold data lake architecture.

Architecture
AdventureWorks CSV
       |
       v
Azure Data Factory
       |
       v
Azure Data Lake Storage Gen2
       |
       v
Bronze Layer
       |
       v
Azure Databricks / PySpark
       |
       v
Silver Layer
       |
       v
Gold Layer
       |
       +-------------------------+
       |                         |
       v                         v
Azure Synapse              Power BI
Serverless SQL             Dashboard
       |
       v
SQL Analytics
Technologies Used
Python
SQL
PySpark
Pandas
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
Azure Synapse Analytics
Power BI
Parquet
Git
GitHub
Data Pipeline
1. Source Data

The project uses the AdventureWorks dataset containing information about:

Sales orders
Customers
Products
Territories

The sales data covers:

2015
2016
2017

The repository also contains supporting customer, product, and territory datasets used during the transformation process.

2. Bronze Layer

The Bronze layer stores the ingested sales data with minimal transformation.

Azure Data Factory is used as the ingestion and orchestration component to copy the source sales data into Azure Data Lake Storage Gen2.

The Bronze layer preserves the source data in Parquet format for downstream processing.

3. Silver Layer

Azure Databricks and PySpark are used to transform the Bronze data into an enriched analytical dataset.

Transformations include:

Parsing OrderDate and StockDate
Joining sales with product information
Joining sales with customer information
Joining sales with territory information
Calculating sales amounts
Calculating cost amounts
Calculating profit
Calculating profit margin
Performing data-quality validation
4. Gold Layer

The Gold layer contains analytical datasets and business-focused outputs used for reporting and analysis.

The project includes Gold-level metrics such as:

Monthly sales performance
Product return/performance analysis
Sales and profit metrics
Business-focused analytical views

These datasets are designed to make downstream SQL analysis and reporting easier.

Azure Data Lake Storage Structure
aays-data/
├── source/
│   └── sales/
├── bronze/
│   └── sales/
├── silver/
│   └── sales/
└── gold/
    ├── monthly_sales/
    └── product_returns/
Azure Data Factory

Azure Data Factory is used as the ingestion and orchestration component.

The project includes the following pipeline components:

PL_Aays_Sales_ETL
Copy_Sales_To_Bronze
DS_Source_Sales_CSV
DS_Bronze_Sales_Parquet

The pipeline demonstrates moving source sales data into the Bronze layer of ADLS Gen2.

Azure Databricks

Azure Databricks is used for PySpark-based data processing.

The Databricks transformation workflow:

Reads Bronze Parquet data
Parses and transforms the sales data
Joins sales with supporting datasets
Calculates business metrics
Performs data-quality checks
Writes transformed data to downstream layers

The implementation uses parameterized storage configuration rather than embedding Azure credentials directly in the notebook.

Azure credentials and secrets are not committed to the repository.

Data Quality

The PySpark pipeline performs data-quality validation during processing.

Checks include:

Missing OrderNumber
Missing ProductKey
Missing CustomerKey
Unmatched product records
Unmatched customer records
Unmatched territory records
Invalid order quantities
Invalid sales amounts
Zero sales amounts

The local dataset contains:

56,046 sales records

The pipeline was used to validate the sales data before producing the transformed output.

Azure Synapse Analytics

Azure Synapse Serverless SQL is used as the analytical SQL layer.

The Gold Parquet datasets can be queried using OPENROWSET.

Example:

SELECT TOP 10
    *
FROM OPENROWSET(
    BULK 'https://aaysdataeng2026.dfs.core.windows.net/aays-data/gold/monthly_sales/*.parquet',
    FORMAT = 'PARQUET'
) AS [result];

The repository includes SQL scripts for:

Creating schemas
Creating external tables
Creating serving-layer views
Creating Gold views
Data-quality checks
Business and analytical queries
SQL Analytics

The project includes SQL queries for analyzing transformed sales data.

Examples include:

Sales by Year
SELECT
    OrderYear,
    COUNT(*) AS TotalSalesRecords,
    SUM(OrderQuantity) AS TotalQuantity
FROM silver_sales
GROUP BY OrderYear
ORDER BY OrderYear;
Top Products
SELECT
    ProductKey,
    SUM(OrderQuantity) AS TotalQuantity,
    COUNT(DISTINCT OrderNumber) AS TotalOrders
FROM silver_sales
GROUP BY ProductKey
ORDER BY TotalQuantity DESC;
Sales by Territory
SELECT
    TerritoryKey,
    COUNT(DISTINCT OrderNumber) AS TotalOrders,
    SUM(OrderQuantity) AS TotalQuantity
FROM silver_sales
GROUP BY TerritoryKey
ORDER BY TotalQuantity DESC;

Additional business-focused queries are available in:

query_insights.sql
Data Warehouse Concepts

The project also demonstrates analytical data warehouse concepts through SQL schemas, external tables, and serving-layer views.

The SQL layer is designed to support analytical reporting from the transformed data stored in ADLS Gen2.

Power BI Dashboard

The project includes:

Azure_PowerBI_Dashboard.pbix

The Power BI report represents the reporting and visualization layer of the data engineering pipeline.

The dashboard is based on the analytical data produced by the pipeline.

Project Structure
AdventureWorks-Azure-Data-Engineering/
│
├── Data/
│   ├── AdventureWorks_Sales_2015.csv
│   ├── AdventureWorks_Sales_2016.csv
│   ├── AdventureWorks_Sales_2017.csv
│   ├── AdventureWorks_Customers.csv
│   ├── AdventureWorks_Products.csv
│   └── AdventureWorks_Territories.csv
│
├── output/
│   └── sales_enriched/
│
├── src/
│
├── Azure_PowerBI_Dashboard.pbix
├── create_schema.sql
├── create_external_table.sql
├── create_views_servinglayer.sql
├── create_gold_views.sql
├── data_quality_checks.sql
├── query_insights.sql
├── dataset_load.json
├── data_transformations_databricks.ipynb
├── explore_data.py
├── explore_products.py
├── explore_sales.py
├── explore_territories.py
├── transform_sales.py
├── test_python.py
├── test_spark.py
├── CHANGES.md
└── README.md
Key Learning Areas

This project demonstrates practical experience with:

ETL and ELT concepts
Data ingestion
Data transformation
PySpark
SQL analytics
Data lake architecture
Bronze/Silver/Gold architecture
Data-quality validation
Azure cloud services
Azure Data Factory
Azure Databricks
Azure Synapse Serverless SQL
Analytical data processing
Git version control
GitHub
Security

Azure credentials and secrets are not stored in the repository.

The Databricks implementation uses parameterized storage configuration rather than hard-coded Azure credentials.

For production deployments, Azure identity-based authentication and services such as Azure Key Vault or managed identities should be used for secure access to Azure resources.

Cost Awareness

The Azure implementation uses Azure for Students resources.

The project uses services including:

Azure Data Lake Storage Gen2
Azure Data Factory
Azure Databricks
Azure Synapse Serverless SQL

Azure services can incur usage-based charges, so unnecessary compute and SQL queries should be avoided.

Future Improvements

Possible future improvements include:

Incremental data loading
Parameterized Azure Data Factory pipelines
Delta Lake tables
Additional data-quality monitoring
Automated pipeline scheduling
Advanced dashboard filtering
CI/CD integration
Pipeline monitoring and alerting
Author

Saira MS
Vishnu Dutt K

Azure Data Engineering portfolio project focused on Python, SQL, PySpark, Azure data services, data pipelines, data warehousing, data quality, and analytical processing.