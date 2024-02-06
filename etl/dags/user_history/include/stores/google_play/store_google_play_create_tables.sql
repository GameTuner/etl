-- Description: Create tables for Google Play sales data

CREATE TABLE IF NOT EXISTS`{project_id}.{app_id}_external.store_google_play_sales` (
    date_ DATE,
    order_number STRING, 
    financial_status STRING,
    package_id STRING,
    currency_code STRING,
    item_price FLOAT64, 
    price FLOAT64,
    country_code STRING
)
PARTITION BY date_;

CREATE TABLE IF NOT EXISTS`{project_id}.{app_id}_main.store_google_play_sales` (
    date_ DATE,
    country_code STRING,
    country_name STRING,
    revenue_usd FLOAT64, 
    net_revenue_usd FLOAT64
)
PARTITION BY date_;