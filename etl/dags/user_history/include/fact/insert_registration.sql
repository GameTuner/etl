-- Description: Inserts registration data into registration table

CREATE OR REPLACE TEMPORARY TABLE registration_temp_{app_id}
PARTITION BY date_
AS
WITH all_users_duplicates AS (
    SELECT
        ctx_events.* EXCEPT(params),
        ctx_device.params AS ctx_device_context,
        ctx_session.params AS ctx_session_context,
    FROM `{project_id}.{app_id}_raw.ctx_event_context` ctx_events
    LEFT JOIN (SELECT event_fingerprint, ctx_device_context as params FROM `{project_id}.{app_id}_raw.*` WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'ctx_') AND ctx_device_context IS NOT NULL AND date_ = '{execution_date}') ctx_device
    ON ctx_events.event_fingerprint = ctx_device.event_fingerprint
    LEFT JOIN (SELECT event_fingerprint, ctx_session_context as params  FROM `{project_id}.{app_id}_raw.*` WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'ctx_') AND ctx_session_context IS NOT NULL AND date_ = '{execution_date}') ctx_session
    ON ctx_events.event_fingerprint = ctx_session.event_fingerprint
    WHERE
      ctx_events.date_ = '{execution_date}' AND ctx_events.event_name <> 'logout'
),all_users AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY unique_id ORDER BY ctx_device_context.build_version IS NULL, user_id IS NULL, event_tstamp) AS rank_first_event
    FROM all_users_duplicates
)
SELECT
  date_,
  installation_id,
  user_id,
  unique_id,
  platform,
  geo_country,
  geo_country_name,
  event_id,
  event_fingerprint,
  event_name,
  event_tstamp as registration_tstamp,
  event_quality,
  backfill_mode,
  ctx_device_context,
  ctx_session_context
FROM all_users
WHERE rank_first_event = 1;

CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.registration`
LIKE registration_temp_{app_id}
OPTIONS(expiration_timestamp=NULL);

DELETE FROM `{project_id}.{app_id}_main.registration` WHERE date_ >= '{execution_date}';

INSERT INTO `{project_id}.{app_id}_main.registration`
SELECT * FROM registration_temp_{app_id}
WHERE unique_id NOT IN (
  SELECT DISTINCT unique_id
  FROM `{project_id}.{app_id}_main.registration`
  WHERE date_ <= '{execution_date}' AND unique_id IS NOT NULL
)
