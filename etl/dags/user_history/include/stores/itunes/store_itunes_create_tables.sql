-- Description: Create tables for iTunes store

CREATE TABLE IF NOT EXISTS`{project_id}.{app_id}_external.store_itunes_sales` (
    date_ DATE,
    package_id STRING,
    app_version STRING,
    country_code STRING,
    currency_code STRING,
    units INT64,
    price FLOAT64, 
    developer_proceeds FLOAT64
)
PARTITION BY date_;

CREATE TABLE IF NOT EXISTS`{project_id}.{app_id}_main.store_itunes_sales` (
    date_ DATE,
    country_code STRING,
    country_name STRING,
    revenue_usd FLOAT64, 
    net_revenue_usd FLOAT64
)
PARTITION BY date_;