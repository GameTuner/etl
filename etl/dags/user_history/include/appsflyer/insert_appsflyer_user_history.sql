-- Description: Creates appsflyer user history table
-- Source: appsflyer raw user level data
-- Destination: appsflyer_user_history
-- Usage: Run daily

DECLARE execution_date DEFAULT DATE '{execution_date}';

-- Calculate sessions metrics
CREATE OR REPLACE TEMPORARY TABLE session_metrics_temp_af_{app_id}
AS
WITH session_data AS
(
    SELECT
        appsflyer_id,
        dt,
        LAST_VALUE(country_code) OVER (PARTITION BY appsflyer_id ORDER BY event_time) AS last_geo_country_code,
        LAST_VALUE(platform) OVER (PARTITION BY appsflyer_id ORDER BY event_time) AS last_platform,
        LAST_VALUE(app_version) OVER (PARTITION BY appsflyer_id ORDER BY event_time) AS last_app_version
    FROM `{project_id}.{app_id}_external.af_sessions`
    WHERE dt = execution_date
    AND appsflyer_id IS NOT NULL
)
SELECT
    appsflyer_id,
    MAX(last_geo_country_code) as last_geo_country_name,
    MAX(last_platform) as last_platform,
    SUM(DISTINCT IF(dt=execution_date, 1, 0)) AS sessions_count,
    MAX(last_app_version) AS app_version
FROM session_data
GROUP BY appsflyer_id;

-- Calculate countries vat for current date
CREATE OR REPLACE TEMP TABLE countries_vat_by_date_af_{app_id}
AS
SELECT *
FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY updated_at DESC) AS country_date_rank
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY numeric_code) AS country_rank
        FROM `{project_id}.gametuner_common.country_vat`
        WHERE updated_at <= execution_date
    )
    WHERE country_rank = 1
)
WHERE country_date_rank = 1;

-- Calculate metrics from purchase table
-- Workaround for missing event_revenue_usd data in af_inapps table for af_purchase
-- We are using paid_usd from purchase table joined by af_order_id.
-- If paid_usd is null, we are using event_revenue_usd from af_inapps table
CREATE OR REPLACE TEMPORARY TABLE purchase_metrics_temp_af_{app_id}
AS
WITH purchase_data AS (
SELECT
  dt AS date_,
  appsflyer_id,
  CAST(event_revenue_usd AS FLOAT64) AS event_revenue_usd,
  country_code,
  JSON_VALUE(event_value, '$.af_order_id') AS af_order,
FROM `{project_id}.{app_id}_external.af_inapps`
WHERE dt = execution_date
AND event_name = 'af_purchase'
AND appsflyer_id IS NOT NULL
), fixed_purchase_data AS
(
SELECT
  purchase_data.* EXCEPT(event_revenue_usd),
  IF(raw_purchase.paid_usd IS NULL, CAST(purchase_data.event_revenue_usd AS FLOAT64), raw_purchase.paid_usd) AS event_revenue_usd
FROM purchase_data
LEFT JOIN (
  SELECT
    params.transaction_id AS af_order,
    params.paid_usd AS paid_usd
  FROM `{project_id}.{app_id}_raw.purchase`
  WHERE date_ BETWEEN DATE_SUB(execution_date, INTERVAL 2 DAY) AND DATE_ADD(execution_date, INTERVAL 2 DAY)
) raw_purchase
USING(af_order)
), purchase_data_vat_index AS (
    SELECT
        p.*,
        IF(c.net_revenue_index IS NULL, 1, c.net_revenue_index) AS net_revenue_index
    FROM fixed_purchase_data p
    LEFT JOIN countries_vat_by_date_af_{app_id} c
    ON p.country_code = c.alpha2_code
)
SELECT
    date_ AS played_date,
    appsflyer_id,
    COUNT(*) AS cnt,
    SUM(event_revenue_usd) AS sum_revenue,
    SUM(event_revenue_usd * net_revenue_index) AS net_revenue
FROM purchase_data_vat_index
WHERE date_ = execution_date
GROUP BY date_, appsflyer_id;

--Calculate ad revenue
CREATE OR REPLACE TEMPORARY TABLE ad_revenue_metrics_temp_af_{app_id}
AS
WITH ad_data AS
(
    SELECT
        dt,
        appsflyer_id,
        event_revenue_usd
    FROM `{project_id}.{app_id}_external.af_attributed_ad_revenue`
    UNION ALL
    SELECT
        dt,
        appsflyer_id,
        event_revenue_usd
    FROM `{project_id}.{app_id}_external.af_organic_ad_revenue`
)
SELECT
  dt AS date_,
  appsflyer_id,
  SUM(CAST(event_revenue_usd AS FLOAT64)) AS ad_revenue,
  COUNT(1) AS ad_revenue_count
FROM ad_data
WHERE dt = execution_date
AND appsflyer_id IS NOT NULL
GROUP BY dt, appsflyer_id;

--Calculate daily users
CREATE OR REPLACE TEMPORARY TABLE daily_users_temp_af_{app_id}
AS
SELECT
    appsflyer_id,
    COUNT(DISTINCT IF(dt = execution_date, dt, NULL)) AS days_active_last_day,
    COUNT(DISTINCT IF(dt BETWEEN DATE_SUB(execution_date, INTERVAL 6 DAY) AND execution_date, dt, NULL)) AS days_active_last_7_days,
    COUNT(DISTINCT IF(dt BETWEEN DATE_SUB(execution_date, INTERVAL 27 DAY) AND execution_date, dt, NULL)) AS days_active_last_28_days,
FROM `{project_id}.{app_id}_external.af_sessions`
WHERE dt BETWEEN DATE_SUB(execution_date, INTERVAL 27 DAY) AND execution_date
AND appsflyer_id IS NOT NULL
GROUP BY appsflyer_id;

-- Get new users from installs table
CREATE OR REPLACE TEMPORARY TABLE registrations_users_temp_af_{app_id}
AS
WITH user_map AS
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY appsflyer_id ORDER BY first_seen_timestamp) AS row_num
    FROM `{project_id}.{app_id}_external.af_user_map`
), appsflyer_ids AS
(
    SELECT
        *
    FROM user_map
    WHERE row_num = 1
    AND appsflyer_id IS NOT NULL
), appsflyer_new AS
(
SELECT
    appsflyer_id,
    MAX(country_code) AS country_code,
    MAX(platform) AS platform,
    MIN(app_version) AS registration_application_version
FROM `{project_id}.{app_id}_external.af_installs`
WHERE dt = execution_date
AND appsflyer_id IS NOT NULL
AND DATE(event_time) = DATE(install_time)
GROUP BY appsflyer_id
)
SELECT
    n.*,
    a.media_source,
    a.campaign,
    a.ad,
    a.ad_id,
    a.ad_type,
    a.adset,
    a.adset_id,
    a.campaign_id,
    a.channel,
    a.keywords,
    a.siteid
FROM appsflyer_new n
LEFT JOIN appsflyer_ids a
ON n.appsflyer_id = a.appsflyer_id;

CREATE OR REPLACE TEMPORARY TABLE user_history_new_af_{app_id}
PARTITION BY date_
AS
SELECT
    execution_date AS date_,
    registered_users.appsflyer_id AS appsflyer_id,
    registered_users.media_source AS media_source,
    registered_users.campaign AS campaign,
    registered_users.ad AS ad,
    registered_users.ad_id AS ad_id,
    registered_users.ad_type AS ad_type,
    registered_users.adset AS adset,
    registered_users.adset_id AS adset_id,
    registered_users.campaign_id AS campaign_id,
    registered_users.channel AS channel,
    registered_users.keywords AS keywords,
    registered_users.siteid AS siteid,
    execution_date AS registration_date,

    --Registration Dimensions
    registered_users.platform AS registration_platform,
    registered_users.country_code AS registration_country,
    registered_users.registration_application_version AS registration_application_version,

    --Session Dimensions,
    registered_users.country_code as last_login_country,
    registered_users.platform as last_login_platform,
    registered_users.registration_application_version AS last_login_application_version,
    1 AS days_active_last_7_days,
    1 AS days_active_last_28_days,
    1 AS days_active,
    execution_date AS last_login_day,
    CAST(NULL AS DATE) AS previous_login_day,

    --Purchase Dimensions
    IFNULL(purchase_metrics_temp_af_{app_id}.played_date, NULL) AS first_transaction_day,
    CAST(NULL AS DATE) AS previous_transaction_day,
    IFNULL(purchase_metrics_temp_af_{app_id}.played_date, NULL) AS last_transaction_day,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NOT NULL, TRUE, FALSE) AS is_payer,
    FALSE AS is_repeated_payer,

    --Cohort Dimensions
    0 AS cohort_day,
    1 AS cohort_size,

    --Session Measures
    IFNULL(session_metrics_temp_af_{app_id}.sessions_count, 0) AS sessions_count,
    IFNULL(session_metrics_temp_af_{app_id}.sessions_count, 0) AS sessions_count_total,
    1 AS dau,
    0 AS dau_yesterday,
    0 AS dau_2days_ago,
    1 AS wau,
    1 AS mau,
    0 AS mau_lost,
    0 AS mau_reactivated,

    --Purchase Measures
    IFNULL(purchase_metrics_temp_af_{app_id}.net_revenue, 0) AS net_revenue_usd,
    IFNULL(purchase_metrics_temp_af_{app_id}.net_revenue, 0) AS net_revenue_usd_total,
    IFNULL(purchase_metrics_temp_af_{app_id}.sum_revenue, 0) AS gross_revenue_usd,
    IFNULL(purchase_metrics_temp_af_{app_id}.sum_revenue, 0) AS gross_revenue_usd_total,
    IFNULL(purchase_metrics_temp_af_{app_id}.cnt, 0) AS transactions_count,
    IFNULL(purchase_metrics_temp_af_{app_id}.cnt, 0) AS transactions_count_total,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, 0, 1) AS new_payers,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, 0, 1) AS total_payers,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, 0, 1) AS daily_payers,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, NULL, 0) AS days_since_last_purchase,

    --Ads Measures
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue, 0) AS ad_revenue_usd,
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue, 0) AS ad_revenue_usd_total,
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue_count, 0) AS ad_revenue_count,
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue_count, 0) AS ad_revenue_count_total,

FROM registrations_users_temp_af_{app_id} registered_users
LEFT JOIN session_metrics_temp_af_{app_id} ON registered_users.appsflyer_id = session_metrics_temp_af_{app_id}.appsflyer_id
LEFT JOIN purchase_metrics_temp_af_{app_id} ON registered_users.appsflyer_id = purchase_metrics_temp_af_{app_id}.appsflyer_id
LEFT JOIN ad_revenue_metrics_temp_af_{app_id} ON registered_users.appsflyer_id = ad_revenue_metrics_temp_af_{app_id}.appsflyer_id;

CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.appsflyer_user_history`
LIKE user_history_new_af_{app_id}
OPTIONS(expiration_timestamp=NULL);

CREATE OR REPLACE TEMPORARY TABLE user_history_existing_af_{app_id}
PARTITION BY date_
AS
SELECT
    execution_date AS date_,
    user_history.appsflyer_id,
    user_history.media_source,
    user_history.campaign,
    user_history.ad,
    user_history.ad_id,
    user_history.ad_type,
    user_history.adset,
    user_history.adset_id,
    user_history.campaign_id,
    user_history.channel,
    user_history.keywords,
    user_history.siteid,
    user_history.registration_date,

    --Registration Dimensions
    user_history.registration_platform,
    user_history.registration_country,
    user_history.registration_application_version,

    --Session Dimensions,
    IF(session_metrics_temp_af_{app_id}.appsflyer_id IS NOT NULL, session_metrics_temp_af_{app_id}.last_geo_country_name, user_history.last_login_country) AS last_login_country,
    IF(session_metrics_temp_af_{app_id}.appsflyer_id IS NOT NULL, session_metrics_temp_af_{app_id}.last_platform, user_history.last_login_platform) AS last_login_platform,
    IF(session_metrics_temp_af_{app_id}.appsflyer_id IS NOT NULL, session_metrics_temp_af_{app_id}.app_version, user_history.last_login_application_version) AS last_login_application_version,
    IFNULL(daily_users_temp_af_{app_id}.days_active_last_7_days, 0) AS days_active_last_7_days,
    IFNULL(daily_users_temp_af_{app_id}.days_active_last_28_days, 0) AS days_active_last_28_days,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL AND daily_users_temp_af_{app_id}.days_active_last_day>0, user_history.days_active + 1,  user_history.days_active) AS days_active,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL AND daily_users_temp_af_{app_id}.days_active_last_day>0, execution_date,  user_history.last_login_day) AS last_login_day,
    user_history.last_login_day AS previous_login_day,

    --Purchase Dimensions
    IFNULL(user_history.first_transaction_day, purchase_metrics_temp_af_{app_id}.played_date) AS first_transaction_day,
    user_history.last_transaction_day AS previous_transaction_day,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, user_history.last_transaction_day, execution_date) AS last_transaction_day,
    IF(user_history.is_payer OR purchase_metrics_temp_af_{app_id}.played_date IS NOT NULL, TRUE, FALSE) AS is_payer,
    IF(user_history.is_repeated_payer, TRUE, IF(user_history.is_payer AND purchase_metrics_temp_af_{app_id}.played_date IS NOT NULL, TRUE, FALSE)) AS is_repeated_payer,

    --Cohort Dimensions
    DATE_DIFF(execution_date, user_history.registration_date, DAY) AS cohort_day,
    1 AS cohort_size,

    --Session Measures
    IF(session_metrics_temp_af_{app_id}.appsflyer_id IS NOT NULL, session_metrics_temp_af_{app_id}.sessions_count,  0) AS sessions_count,
    IF(session_metrics_temp_af_{app_id}.appsflyer_id IS NOT NULL, IFNULL(user_history.sessions_count_total, 0) + session_metrics_temp_af_{app_id}.sessions_count, user_history.sessions_count_total) AS sessions_count_total,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL, IF(IFNULL(daily_users_temp_af_{app_id}.days_active_last_day, 0) = 0, 0, 1), 0)  AS dau,
    user_history.dau AS dau_yesterday,
    user_history.dau_yesterday AS dau_2days_ago,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL AND daily_users_temp_af_{app_id}.days_active_last_7_days>0, 1, 0) AS wau,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL AND daily_users_temp_af_{app_id}.days_active_last_28_days>0, 1, 0) AS mau,
    IF((daily_users_temp_af_{app_id}.appsflyer_id IS NULL OR daily_users_temp_af_{app_id}.days_active_last_28_days=0) AND user_history.last_login_day<DATE_SUB(execution_date, INTERVAL 27 DAY), 1, 0) AS mau_lost,
    IF(daily_users_temp_af_{app_id}.appsflyer_id IS NOT NULL AND user_history.mau_lost=1, 1, 0) AS mau_reactivated,

    --Purchase Measures
    IFNULL(purchase_metrics_temp_af_{app_id}.net_revenue, 0) AS net_revenue_usd,
    IF(purchase_metrics_temp_af_{app_id}.net_revenue IS NULL, user_history.net_revenue_usd_total, user_history.net_revenue_usd_total + purchase_metrics_temp_af_{app_id}.net_revenue) AS net_revenue_usd_total,
    IFNULL(purchase_metrics_temp_af_{app_id}.sum_revenue, 0) AS gross_revenue_usd,
    IF(purchase_metrics_temp_af_{app_id}.sum_revenue IS NULL, user_history.gross_revenue_usd_total, user_history.gross_revenue_usd_total + purchase_metrics_temp_af_{app_id}.sum_revenue) AS gross_revenue_usd_total,
    IFNULL(purchase_metrics_temp_af_{app_id}.cnt, 0) AS transactions_count,
    IF(purchase_metrics_temp_af_{app_id}.cnt IS NULL, user_history.transactions_count_total, user_history.transactions_count_total + purchase_metrics_temp_af_{app_id}.cnt) AS transactions_count_total,
    IF(IFNULL(user_history.first_transaction_day, purchase_metrics_temp_af_{app_id}.played_date) = execution_date, 1, 0) AS new_payers,
    IF(user_history.first_transaction_day IS NOT NULL OR purchase_metrics_temp_af_{app_id}.played_date IS NOT NULL, 1, 0) AS total_payers,
    IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, 0, 1) AS daily_payers,
    DATE_DIFF(execution_date, IF(purchase_metrics_temp_af_{app_id}.played_date IS NULL, user_history.last_transaction_day, execution_date), DAY) AS days_since_last_purchase,

    --Ads Measures
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue, 0) AS ad_revenue_usd,
    IF(ad_revenue_metrics_temp_af_{app_id}.ad_revenue IS NULL, user_history.ad_revenue_usd_total, user_history.ad_revenue_usd_total + ad_revenue_metrics_temp_af_{app_id}.ad_revenue) AS ad_revenue_usd_total,
    IFNULL(ad_revenue_metrics_temp_af_{app_id}.ad_revenue_count, 0) AS ad_revenue_count,
    IF(ad_revenue_metrics_temp_af_{app_id}.ad_revenue_count IS NULL, user_history.ad_revenue_count_total, user_history.ad_revenue_count_total + ad_revenue_metrics_temp_af_{app_id}.ad_revenue_count) AS ad_revenue_count_total,

FROM `{project_id}.{app_id}_main.appsflyer_user_history` AS user_history
LEFT JOIN session_metrics_temp_af_{app_id} ON user_history.appsflyer_id = session_metrics_temp_af_{app_id}.appsflyer_id
LEFT JOIN purchase_metrics_temp_af_{app_id} ON user_history.appsflyer_id = purchase_metrics_temp_af_{app_id}.appsflyer_id
LEFT JOIN daily_users_temp_af_{app_id} ON user_history.appsflyer_id = daily_users_temp_af_{app_id}.appsflyer_id
LEFT JOIN ad_revenue_metrics_temp_af_{app_id} ON user_history.appsflyer_id = ad_revenue_metrics_temp_af_{app_id}.appsflyer_id
WHERE user_history.date_ = DATE_SUB(execution_date, INTERVAL 1 DAY);

DELETE FROM `{project_id}.{app_id}_main.appsflyer_user_history` WHERE date_ = execution_date;

INSERT INTO `{project_id}.{app_id}_main.appsflyer_user_history`
SELECT * FROM user_history_existing_af_{app_id}
UNION ALL
SELECT * FROM user_history_new_af_{app_id}
WHERE appsflyer_id NOT IN (
  SELECT DISTINCT appsflyer_id
  FROM `{project_id}.{app_id}_main.appsflyer_user_history`
  WHERE date_ = DATE_SUB(execution_date, INTERVAL 1 DAY)
);
