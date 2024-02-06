-- Description: Inserts data into user_history_daily and user_history_monthly tables

DECLARE execution_date DEFAULT DATE '{execution_date}';

-- daily
CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.user_history_daily`
LIKE `{project_id}.{app_id}_main.user_history`;

DELETE FROM `{project_id}.{app_id}_main.user_history_daily` WHERE date_ = '{execution_date}';

INSERT INTO `{project_id}.{app_id}_main.user_history_daily`
SELECT * FROM `{project_id}.{app_id}_main.user_history`
WHERE date_ = execution_date AND last_login_day = execution_date;

-- monthly
CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.user_history_monthly`
LIKE `{project_id}.{app_id}_main.user_history`;

DELETE FROM `{project_id}.{app_id}_main.user_history_monthly` WHERE date_ = '{execution_date}';

INSERT INTO `{project_id}.{app_id}_main.user_history_monthly`
SELECT * FROM `{project_id}.{app_id}_main.user_history`
WHERE date_ = execution_date AND last_login_day >= execution_date - INTERVAL 30 DAY;

-- views
CREATE OR REPLACE VIEW `{project_id}.{app_id}_main.v_user_history` AS
SELECT
  *,
  DATE_TRUNC(registration_date, WEEK(MONDAY)) AS registration_week,
  DATE_TRUNC(registration_date, MONTH) AS registration_month
FROM `{project_id}.{app_id}_main.user_history`;

CREATE OR REPLACE VIEW `{project_id}.{app_id}_main.v_user_history_monthly` AS
SELECT
  *,
  DATE_TRUNC(registration_date, WEEK(MONDAY)) AS registration_week,
  DATE_TRUNC(registration_date, MONTH) AS registration_month
FROM `{project_id}.{app_id}_main.user_history_monthly`;

CREATE OR REPLACE VIEW `{project_id}.{app_id}_main.v_user_history_daily` AS
SELECT
  *,
  DATE_TRUNC(registration_date, WEEK(MONDAY)) AS registration_week,
  DATE_TRUNC(registration_date, MONTH) AS registration_month
FROM `{project_id}.{app_id}_main.user_history_daily`;