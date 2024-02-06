-- Description: This file contains the SQL to process the data from the store_google_play_sales table.

DECLARE execution_date DEFAULT DATE '{execution_date}';

DELETE FROM `{project_id}.{app_id}_main.store_google_play_sales`
WHERE date_ = execution_date;

INSERT INTO `{project_id}.{app_id}_main.store_google_play_sales`
WITH cte AS 
(
  SELECT 
    s.*, 
    s.price / c.rate AS revenue_usd,
    (s.price / c.rate) * v.net_revenue_index AS net_revenue_usd,
    v.country_name AS country_name
  FROM `{project_id}.{app_id}_external.store_google_play_sales` s
  LEFT JOIN `{project_id}.gametuner_common.currencies` c
  ON s.date_ = c.date_ AND s.currency_code = c.currency
  LEFT JOIN `{project_id}.gametuner_common.country_vat`v
  ON s.country_code = v.alpha2_code
  WHERE s.date_ = execution_date
  AND financial_status = 'Charged'
)
SELECT 
date_,
country_code,
country_name,
SUM(revenue_usd) AS revenue_usd,
SUM(net_revenue_usd) AS net_revenue_usd,
COUNT(1) AS iap_purchase_count
FROM cte
GROUP BY 1, 2, 3;

