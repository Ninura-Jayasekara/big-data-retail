CREATE EXTERNAL TABLE cleaned_retail_data (
  InvoiceNo STRING,
  StockCode STRING,
  Description STRING,
  Quantity INT,
  InvoiceDate STRING,
  UnitPrice DOUBLE,
  CustomerID STRING,
  Country STRING,
  TotalPrice DOUBLE
)
STORED AS PARQUET
LOCATION 's3://bigdata-retail-analytics-data/processed/cleaned_data.parquet';
