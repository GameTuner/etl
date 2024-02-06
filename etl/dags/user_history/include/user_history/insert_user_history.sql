-- Description: Insert user history data into user_history table
-- User history table is a table that contains all the data about the users
-- from the first day of the app launch and every day after that,
-- even if the user is not active anymore.

DECLARE execution_date DEFAULT DATE '{execution_date}';
DECLARE columns_user_history ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_temp_uh ARRAY<STRUCT<column_name STRING, data_type STRING>> DEFAULT [{user_history_columns_struct}];

DECLARE temp_colums_to_add STRING;
DECLARE update_table_command STRING;
DECLARE exists_table INT64;

-- Update user history if needed
SET exists_table = (SELECT size_bytes FROM `{project_id}.{app_id}_main`.__TABLES__ WHERE table_id='user_history');

IF (exists_table IS NOT NULL)
    THEN
        SET columns_user_history = (SELECT ARRAY(SELECT (column_name, data_type) FROM `{project_id}.{app_id}_main`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'user_history'));

        SET temp_colums_to_add = (SELECT STRING_AGG(CONCAT("ADD COLUMN ", temp_uh.column_name, " ", temp_uh.data_type))
                                        FROM UNNEST(columns_temp_uh) AS temp_uh
                                        LEFT JOIN UNNEST(columns_user_history) AS uh
                                        ON temp_uh.column_name = uh.column_name
                                        WHERE uh.column_name IS NULL);

        SELECT temp_colums_to_add;

        IF temp_colums_to_add IS NOT NULL THEN
            SET update_table_command = CONCAT("ALTER TABLE `{project_id}.{app_id}_main.user_history` ");
            SET update_table_command = CONCAT(update_table_command, temp_colums_to_add);
            EXECUTE IMMEDIATE update_table_command;

            SET update_table_command = CONCAT("ALTER TABLE `{project_id}.{app_id}_main.user_history_daily` ");
            SET update_table_command = CONCAT(update_table_command, temp_colums_to_add);
            EXECUTE IMMEDIATE update_table_command;

            SET update_table_command = CONCAT("ALTER TABLE `{project_id}.{app_id}_main.user_history_monthly` ");
            SET update_table_command = CONCAT(update_table_command, temp_colums_to_add);
            EXECUTE IMMEDIATE update_table_command;
        END IF;
END IF;

-- Calculate sessions metrics
CREATE OR REPLACE TEMPORARY TABLE session_metrics_temp_{app_id}
AS
WITH session_data AS 
(
    SELECT 
        *,
        LAST_VALUE(geo_country_name) OVER (PARTITION BY unique_id ORDER BY session_start_ts) AS last_geo_country_name,
        LAST_VALUE(platform) OVER (PARTITION BY unique_id ORDER BY session_start_ts) AS last_platform,
        LAST_VALUE(ctx_device_context.build_version) OVER (PARTITION BY unique_id ORDER BY session_start_ts) AS last_app_version,
        LAST_VALUE(installation_id) OVER (PARTITION BY unique_id ORDER BY session_start_ts) AS last_installation_id,
        LAST_VALUE(user_id) OVER (PARTITION BY unique_id ORDER BY session_start_ts) AS last_user_id
    FROM `{project_id}.{app_id}_main.session`
    WHERE date_ = execution_date
)
SELECT 
    unique_id,
    MAX(last_user_id) as user_id,
    MAX(last_installation_id) as last_installation_id,
    MAX(last_geo_country_name) as last_geo_country_name,
    MAX(last_platform) as last_platform,
    COUNT(DISTINCT IF(date_=execution_date, session_id, NULL)) AS sessions_count,
    SUM(IF(date_=execution_date, IFNULL(session_length, 0), 0)) AS playtime,
    MAX(last_app_version) AS app_version
FROM session_data
GROUP BY unique_id;

CREATE OR REPLACE TEMP TABLE countries_vat_by_date_{app_id}
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
        WHERE updated_at <= "{execution_date}"
    )
    WHERE country_rank = 1
)
WHERE country_date_rank = 1;

-- Calculate metrics from purchase table
CREATE OR REPLACE TEMPORARY TABLE purchase_metrics_temp_{app_id}
AS
WITH purchase_data AS (
SELECT 
  date_,
  unique_id,
  params.paid_usd,
  geo_country
FROM `{project_id}.{app_id}_raw.purchase`
WHERE date_ = execution_date
), purchase_data_vat_index AS (
    SELECT
        p.*,
        IF(c.net_revenue_index IS NULL, 1, c.net_revenue_index) AS net_revenue_index
    FROM purchase_data p
    LEFT JOIN countries_vat_by_date_{app_id} c 
    ON p.geo_country = c.alpha2_code
)
SELECT
    date_ AS played_date,
    unique_id,
    COUNT(*) AS cnt,
    SUM(paid_usd) AS sum_revenue,
    SUM(paid_usd * net_revenue_index) AS net_revenue
FROM purchase_data_vat_index
WHERE date_ = execution_date
GROUP BY date_, unique_id;

-- Calculate daily users
-- DAU is calculated by session table
CREATE OR REPLACE TEMPORARY TABLE daily_users_temp_{app_id}
AS
SELECT
    unique_id,
    COUNT(DISTINCT IF(date_ = execution_date, date_, NULL)) AS days_active_last_day,
    COUNT(DISTINCT IF(date_ BETWEEN DATE_SUB(execution_date, INTERVAL 6 DAY) AND execution_date, date_, NULL)) AS days_active_last_7_days,
    COUNT(DISTINCT IF(date_ BETWEEN DATE_SUB(execution_date, INTERVAL 27 DAY) AND execution_date, date_, NULL)) AS days_active_last_28_days,
FROM `{project_id}.{app_id}_main.session`
WHERE date_ BETWEEN DATE_SUB(execution_date, INTERVAL 27 DAY) AND execution_date
GROUP BY unique_id;


-- Template from semantic layer
{temp_tables}

-- Get new users
CREATE OR REPLACE TEMPORARY TABLE registrations_users_temp_{app_id}
AS
SELECT
    installation_id,
    user_id,
    unique_id,
    event_fingerprint AS event_id,
    geo_country_name,
    platform,
    ctx_device_context.build_version AS registration_application_version
FROM `{project_id}.{app_id}_main.registration`
WHERE date_ = execution_date;

CREATE OR REPLACE TEMPORARY TABLE user_history_new_{app_id}
PARTITION BY date_
AS
SELECT
    execution_date AS date_,
    COALESCE(session_metrics_temp_{app_id}.last_installation_id, registered_users.installation_id) AS last_installation_id,
    COALESCE(session_metrics_temp_{app_id}.user_id, registered_users.user_id) AS user_id,
    registered_users.unique_id AS unique_id,
    execution_date AS registration_date,

    --Registration Dimensions
    registered_users.event_id AS registration_event_id,
    registered_users.platform AS registration_platform,
    registered_users.geo_country_name AS registration_country,
    registered_users.registration_application_version AS registration_application_version,

    --Session Dimensions,
    registered_users.geo_country_name as last_login_country,
    registered_users.platform as last_login_platform,
    registered_users.registration_application_version AS last_login_application_version,
    1 AS days_active_last_7_days,
    1 AS days_active_last_28_days,
    1 AS days_active,
    execution_date AS last_login_day,
    CAST(NULL AS DATE) AS previous_login_day,

    --Purchase Dimensions
    IFNULL(purchase_metrics_temp_{app_id}.played_date, NULL) AS first_transaction_day,
    CAST(NULL AS DATE) AS previous_transaction_day,
    IFNULL(purchase_metrics_temp_{app_id}.played_date, NULL) AS last_transaction_day,
    IF(purchase_metrics_temp_{app_id}.played_date IS NOT NULL, TRUE, FALSE) AS is_payer,
    FALSE AS is_repeated_payer,

    --Cohort Dimensions
    0 AS cohort_day,
    1 AS cohort_size,

    --Session Measures
    IFNULL(session_metrics_temp_{app_id}.sessions_count, 0) AS sessions_count,
    IFNULL(session_metrics_temp_{app_id}.playtime, 0) AS playtime,
    IFNULL(session_metrics_temp_{app_id}.sessions_count, 0) AS sessions_count_total,
    IFNULL(session_metrics_temp_{app_id}.playtime, 0) AS playtime_total,
    1 AS dau,
    0 AS dau_yesterday,
    0 AS dau_2days_ago,
    1 AS wau,
    1 AS mau,
    0 AS mau_lost,
    0 AS mau_reactivated,

    --Purchase Measures
    IFNULL(purchase_metrics_temp_{app_id}.net_revenue, 0) AS net_revenue_usd,
    IFNULL(purchase_metrics_temp_{app_id}.net_revenue, 0) AS net_revenue_usd_total,
    IFNULL(purchase_metrics_temp_{app_id}.sum_revenue, 0) AS gross_revenue_usd,
    IFNULL(purchase_metrics_temp_{app_id}.sum_revenue, 0) AS gross_revenue_usd_total,
    IFNULL(purchase_metrics_temp_{app_id}.cnt, 0) AS transactions_count,
    IFNULL(purchase_metrics_temp_{app_id}.cnt, 0) AS transactions_count_total,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, 0, 1) AS new_payers,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, 0, 1) AS total_payers,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, 0, 1) AS daily_payers,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, NULL, 0) AS days_since_last_purchase,

    -- Template from semantic layer
    {insert_new_select}

FROM registrations_users_temp_{app_id} registered_users
LEFT JOIN session_metrics_temp_{app_id} ON registered_users.unique_id = session_metrics_temp_{app_id}.unique_id
LEFT JOIN purchase_metrics_temp_{app_id} ON registered_users.unique_id = purchase_metrics_temp_{app_id}.unique_id
--Template from semantic layer
{insert_new_table}
;

CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.user_history`
LIKE user_history_new_{app_id}
OPTIONS(expiration_timestamp=NULL);

CREATE OR REPLACE TEMPORARY TABLE user_history_existing_{app_id}
PARTITION BY date_
AS
SELECT
    execution_date AS date_,
    COALESCE(session_metrics_temp_{app_id}.last_installation_id, user_history.last_installation_id) AS last_installation_id,
    COALESCE(session_metrics_temp_{app_id}.user_id, user_history.user_id) AS user_id,
    user_history.unique_id,
    user_history.registration_date,

    --Registration Dimensions
    user_history.registration_event_id,
    user_history.registration_platform,
    user_history.registration_country,
    user_history.registration_application_version,

    --Session Dimensions,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, session_metrics_temp_{app_id}.last_geo_country_name, user_history.last_login_country) AS last_login_country,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, session_metrics_temp_{app_id}.last_platform, user_history.last_login_platform) AS last_login_platform,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, session_metrics_temp_{app_id}.app_version, user_history.last_login_application_version) AS last_login_application_version,
    IFNULL(daily_users_temp_{app_id}.days_active_last_7_days, 0) AS days_active_last_7_days,
    IFNULL(daily_users_temp_{app_id}.days_active_last_28_days, 0) AS days_active_last_28_days,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL AND daily_users_temp_{app_id}.days_active_last_day>0, user_history.days_active + 1,  user_history.days_active) AS days_active,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL AND daily_users_temp_{app_id}.days_active_last_day>0, execution_date,  user_history.last_login_day) AS last_login_day,
    user_history.last_login_day AS previous_login_day,

    --Purchase Dimensions
    IFNULL(user_history.first_transaction_day, purchase_metrics_temp_{app_id}.played_date) AS first_transaction_day,
    user_history.last_transaction_day AS previous_transaction_day,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, user_history.last_transaction_day, execution_date) AS last_transaction_day,
    IF(user_history.is_payer OR purchase_metrics_temp_{app_id}.played_date IS NOT NULL, TRUE, FALSE) AS is_payer,
    IF(user_history.is_repeated_payer, TRUE, IF(user_history.is_payer AND purchase_metrics_temp_{app_id}.played_date IS NOT NULL, TRUE, FALSE)) AS is_repeated_payer,

    --Cohort Dimensions
    DATE_DIFF(execution_date, user_history.registration_date, DAY) AS cohort_day,
    1 AS cohort_size,

    --Session Measures
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, session_metrics_temp_{app_id}.sessions_count,  0) AS sessions_count,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, session_metrics_temp_{app_id}.playtime,  0) AS playtime,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, IFNULL(user_history.sessions_count_total, 0) + session_metrics_temp_{app_id}.sessions_count, user_history.sessions_count_total) AS sessions_count_total,
    IF(session_metrics_temp_{app_id}.unique_id IS NOT NULL, IFNULL(user_history.playtime_total, 0) + session_metrics_temp_{app_id}.playtime, user_history.playtime_total) AS playtime_total,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL, IF(IFNULL(daily_users_temp_{app_id}.days_active_last_day, 0) = 0, 0, 1), 0)  AS dau,
    user_history.dau AS dau_yesterday,
    user_history.dau_yesterday AS dau_2days_ago,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL AND daily_users_temp_{app_id}.days_active_last_7_days>0, 1, 0) AS wau,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL AND daily_users_temp_{app_id}.days_active_last_28_days>0, 1, 0) AS mau,
    IF((daily_users_temp_{app_id}.unique_id IS NULL OR daily_users_temp_{app_id}.days_active_last_28_days=0) AND user_history.last_login_day<DATE_SUB(execution_date, INTERVAL 27 DAY), 1, 0) AS mau_lost,
    IF(daily_users_temp_{app_id}.unique_id IS NOT NULL AND user_history.mau_lost=1, 1, 0) AS mau_reactivated,

    --Purchase Measures
    IFNULL(purchase_metrics_temp_{app_id}.net_revenue, 0) AS net_revenue_usd,
    IF(purchase_metrics_temp_{app_id}.net_revenue IS NULL, user_history.net_revenue_usd_total, user_history.net_revenue_usd_total + purchase_metrics_temp_{app_id}.net_revenue) AS net_revenue_usd_total,
    IFNULL(purchase_metrics_temp_{app_id}.sum_revenue, 0) AS gross_revenue_usd,
    IF(purchase_metrics_temp_{app_id}.sum_revenue IS NULL, user_history.gross_revenue_usd_total, user_history.gross_revenue_usd_total + purchase_metrics_temp_{app_id}.sum_revenue) AS gross_revenue_usd_total,
    IFNULL(purchase_metrics_temp_{app_id}.cnt, 0) AS transactions_count,
    IF(purchase_metrics_temp_{app_id}.cnt IS NULL, user_history.transactions_count_total, user_history.transactions_count_total + purchase_metrics_temp_{app_id}.cnt) AS transactions_count_total,
    IF(IFNULL(user_history.first_transaction_day, purchase_metrics_temp_{app_id}.played_date) = execution_date, 1, 0) AS new_payers,
    IF(user_history.first_transaction_day IS NOT NULL OR purchase_metrics_temp_{app_id}.played_date IS NOT NULL, 1, 0) AS total_payers,
    IF(purchase_metrics_temp_{app_id}.played_date IS NULL, 0, 1) AS daily_payers,
    DATE_DIFF(execution_date, IF(purchase_metrics_temp_{app_id}.played_date IS NULL, user_history.last_transaction_day, execution_date), DAY) AS days_since_last_purchase,

    --Template from semantic layer
    {insert_old_select}


FROM `{project_id}.{app_id}_main.user_history` AS user_history
LEFT JOIN session_metrics_temp_{app_id} ON user_history.unique_id = session_metrics_temp_{app_id}.unique_id
LEFT JOIN purchase_metrics_temp_{app_id} ON user_history.unique_id = purchase_metrics_temp_{app_id}.unique_id
LEFT JOIN daily_users_temp_{app_id} ON user_history.unique_id = daily_users_temp_{app_id}.unique_id
--Template from semantic layer
{insert_old_table}
WHERE user_history.date_ = DATE_SUB(execution_date, INTERVAL 1 DAY);

DELETE FROM `{project_id}.{app_id}_main.user_history` WHERE date_ = '{execution_date}';

INSERT INTO `{project_id}.{app_id}_main.user_history` ({user_history_columns})
SELECT {user_history_columns} FROM user_history_existing_{app_id}
UNION ALL
SELECT {user_history_columns} FROM user_history_new_{app_id}
WHERE unique_id NOT IN (
  SELECT DISTINCT unique_id
  FROM `{project_id}.{app_id}_main.user_history`
  WHERE date_ = DATE_SUB(execution_date, INTERVAL 1 DAY)
);