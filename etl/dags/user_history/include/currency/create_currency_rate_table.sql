-- Description: Create currency rate table

CREATE TABLE IF NOT EXISTS `{project_id}.gametuner_common.currencies`
(
  date_ DATE NOT NULL,
  currency STRING,
  rate FLOAT64
)
PARTITION BY date_