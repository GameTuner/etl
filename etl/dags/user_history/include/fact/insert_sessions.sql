-- Description: Inserts sessions data into the session table

CREATE OR REPLACE TEMPORARY TABLE session_temp_{app_id}
PARTITION BY date_
AS
WITH processing_data AS (
SELECT
    CAST('{execution_date}' AS DATE) AS date_,
    ctx_event.installation_id,
    ctx_event.user_id,
    ctx_event.unique_id,
    ctx_event.event_tstamp,
    ctx_event.event_quality AS data_quality,
    ctx_event.event_name,
    ctx_event.geo_country,
    ctx_event.geo_country_name,
    ctx_event.platform,
    ctx_session.session_id,
    ctx_session.session_time,
    ctx_event.params.event_index,
    ctx_event.v_tracker
FROM `{project_id}.{app_id}_raw.ctx_event_context` ctx_event
LEFT JOIN (
         SELECT 
            unique_id, 
            event_fingerprint, 
            ctx_session_context.session_id AS session_id,
            ctx_session_context.session_time AS session_time
         FROM `{project_id}.{app_id}_raw.*` WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'ctx_') AND ctx_session_context IS NOT NULL AND date_ = '{execution_date}') ctx_session
ON ctx_event.event_fingerprint = ctx_session.event_fingerprint
WHERE
    ctx_event.date_ = '{execution_date}'
    AND ctx_event.event_name <> 'logout'
),
enriched_session_data AS (
    SELECT
        *,
        FIRST_VALUE(event_tstamp) OVER(PARTITION BY unique_id, session_id ORDER BY event_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_tstamp,
        LAST_VALUE(event_tstamp) OVER(PARTITION BY unique_id, session_id ORDER BY event_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_tstamp,
        LAST_VALUE(session_time) OVER(PARTITION BY unique_id, session_id ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_session_time,
        AVG(data_quality) OVER(PARTITION BY unique_id, session_id ORDER BY event_index ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_quality,
        ROW_NUMBER() OVER(PARTITION BY unique_id, session_id ORDER BY event_index ASC) AS rank_events
    FROM processing_data
) 
SELECT
    enriched_session_data.date_,
    enriched_session_data.installation_id,
    enriched_session_data.user_id,
    enriched_session_data.unique_id,
    enriched_session_data.session_id,
    enriched_session_data.geo_country,
    enriched_session_data.geo_country_name,
    enriched_session_data.platform,
    enriched_session_data.first_event_tstamp AS session_start_ts,
    enriched_session_data.last_event_tstamp AS session_end_ts,
    SAFE_CAST(enriched_session_data.last_event_session_time AS INT64) AS tick_session_length,
    TIMESTAMP_DIFF(enriched_session_data.last_event_tstamp, enriched_session_data.first_event_tstamp, SECOND) AS time_session_length,
    IF(enriched_session_data.last_event_session_time IS NOT NULL, SAFE_CAST(enriched_session_data.last_event_session_time AS INT64), TIMESTAMP_DIFF(enriched_session_data.last_event_tstamp, enriched_session_data.first_event_tstamp, SECOND)) AS session_length,
    enriched_session_data.session_quality,
    device.ctx_device_context,
FROM enriched_session_data
LEFT JOIN (
    WITH raw_data AS
    (
        SELECT
            ctx_device.unique_id,
            ctx_session.session_id AS session_id,
            ctx_device.ctx_device_context,
            ROW_NUMBER() OVER(PARTITION BY ctx_device.unique_id, ctx_session.session_id ORDER BY ctx_device.event_tstamp ASC) AS session_rank
        FROM `{project_id}.{app_id}_raw.*` ctx_device
        LEFT JOIN (
                 SELECT unique_id, event_fingerprint, ctx_session_context.session_id AS session_id
                 FROM `{project_id}.{app_id}_raw.*`
                 WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'ctx_') AND ctx_session_context IS NOT NULL AND date_ = '{execution_date}') ctx_session
        ON ctx_device.event_fingerprint = ctx_session.event_fingerprint
        WHERE NOT STARTS_WITH(ctx_device._TABLE_SUFFIX, 'ctx_') AND ctx_device.ctx_device_context IS NOT NULL AND ctx_device.date_ = '{execution_date}'
    )
    SELECT
        * EXCEPT(session_rank)
    FROM raw_data
    WHERE session_rank = 1
) device USING (unique_id, session_id)
WHERE enriched_session_data.rank_events = 1;


CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.session`
LIKE session_temp_{app_id}
OPTIONS(expiration_timestamp=NULL);

DELETE FROM `{project_id}.{app_id}_main.session` WHERE date_ = '{execution_date}';

INSERT INTO `{project_id}.{app_id}_main.session`
SELECT * FROM session_temp_{app_id};
