-- Description: Update user map table with new users from appsflyer

DECLARE execution_date DEFAULT DATE '{execution_date}';

--Fill user map
CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_external.af_user_map`
(
  appsflyer_id STRING,
  customer_user_id STRING,
  user_id STRING,
  installation_id STRING,
  media_source STRING,
  campaign STRING,
  ad STRING,
  ad_id STRING,
  ad_type STRING,
  adset STRING,
  adset_id STRING,
  campaign_id STRING,
  channel STRING,
  keywords STRING,
  siteid STRING,
  is_primary_attribution BOOL,
  is_retargeting BOOL,
  first_seen_timestamp TIMESTAMP,
  last_seen_timestamp TIMESTAMP
);

CREATE OR REPLACE TEMP TABLE appsflyer_map_{app_id}
AS
WITH raw_data AS
(
  SELECT
    appsflyer_id,
    customer_user_id,
    JSON_VALUE(custom_data, '$.user_id') AS user_id,
    JSON_VALUE(custom_data, '$.installation_id') AS installation_id,
    event_time,
    media_source,
    campaign,
    af_ad AS ad,
    af_ad_id AS ad_id,
    af_ad_type AS ad_type,
    af_adset AS adset,
    af_adset_id AS adset_id,
    af_c_id AS campaign_id,
    af_channel AS channel,
    af_keywords AS keywords,
    af_siteid AS siteid,
    CAST(is_primary_attribution AS BOOL) AS is_primary_attribution,
    CAST(is_retargeting AS BOOL) AS is_retargeting,
  FROM `{project_id}.{app_id}_external.af_inapps`
  WHERE dt = execution_date
  UNION ALL
  SELECT
    appsflyer_id,
    customer_user_id,
    JSON_VALUE(custom_data, '$.user_id') AS user_id,
    JSON_VALUE(custom_data, '$.installation_id') AS installation_id,
    event_time,
    media_source,
    campaign,
    af_ad AS ad,
    af_ad_id AS ad_id,
    af_ad_type AS ad_type,
    af_adset AS adset,
    af_adset_id AS adset_id,
    af_c_id AS campaign_id,
    af_channel AS channel,
    af_keywords AS keywords,
    af_siteid AS siteid,
    CAST(is_primary_attribution AS BOOL) AS is_primary_attribution,
    CAST(is_retargeting AS BOOL) AS is_retargeting,
  FROM `{project_id}.{app_id}_external.af_sessions`
  WHERE dt = execution_date
  UNION ALL
  SELECT
    appsflyer_id,
    customer_user_id,
    JSON_VALUE(custom_data, '$.user_id') AS user_id,
    JSON_VALUE(custom_data, '$.installation_id') AS installation_id,
    event_time,
    media_source,
    campaign,
    af_ad AS ad,
    af_ad_id AS ad_id,
    af_ad_type AS ad_type,
    af_adset AS adset,
    af_adset_id AS adset_id,
    af_c_id AS campaign_id,
    af_channel AS channel,
    af_keywords AS keywords,
    af_siteid AS siteid,
    CAST(is_primary_attribution AS BOOL) AS is_primary_attribution,
    CAST(is_retargeting AS BOOL) AS is_retargeting
  FROM `{project_id}.{app_id}_external.af_installs`
  WHERE dt = execution_date
  UNION ALL
  SELECT
    appsflyer_id,
    customer_user_id,
    JSON_VALUE(custom_data, '$.user_id') AS user_id,
    JSON_VALUE(custom_data, '$.installation_id') AS installation_id,
    event_time,
    media_source,
    campaign,
    af_ad AS ad,
    af_ad_id AS ad_id,
    af_ad_type AS ad_type,
    af_adset AS adset,
    af_adset_id AS adset_id,
    af_c_id AS campaign_id,
    af_channel AS channel,
    af_keywords AS keywords,
    af_siteid AS siteid,
    CAST(is_primary_attribution AS BOOL) AS is_primary_attribution,
    CAST(is_retargeting AS BOOL) AS is_retargeting
  FROM `{project_id}.{app_id}_external.af_inapps_retargeting`
  WHERE dt = execution_date
  UNION ALL
  SELECT
    appsflyer_id,
    customer_user_id,
    JSON_VALUE(custom_data, '$.user_id') AS user_id,
    JSON_VALUE(custom_data, '$.installation_id') AS installation_id,
    event_time,
    media_source,
    campaign,
    af_ad AS ad,
    af_ad_id AS ad_id,
    af_ad_type AS ad_type,
    af_adset AS adset,
    af_adset_id AS adset_id,
    af_c_id AS campaign_id,
    af_channel AS channel,
    af_keywords AS keywords,
    af_siteid AS siteid,
    CAST(is_primary_attribution AS BOOL) AS is_primary_attribution,
    CAST(is_retargeting AS BOOL) AS is_retargeting
  FROM `{project_id}.{app_id}_external.af_sessions_retargeting`
  WHERE dt = execution_date
), cte_data AS
(
SELECT
DISTINCT
  appsflyer_id,
  customer_user_id,
  user_id,
  installation_id,
  media_source,
  campaign,
  ad,
  ad_id,
  ad_type,
  adset,
  adset_id,
  campaign_id,
  channel,
  keywords,
  siteid,
  is_primary_attribution,
  is_retargeting,
  FIRST_VALUE(event_time)
    OVER (PARTITION BY appsflyer_id,
          customer_user_id,
          user_id,
          installation_id,
          media_source,
          campaign,
          ad,
          ad_id,
          ad_type,
          adset,
          adset_id,
          campaign_id,
          channel,
          keywords,
          siteid,
          is_primary_attribution,
          is_retargeting   ORDER BY event_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_seen_timestamp,
  LAST_VALUE(event_time)
    OVER (PARTITION BY appsflyer_id,
          appsflyer_id,
          customer_user_id,
          user_id,
          installation_id,
          media_source,
          campaign,
          ad,
          ad_id,
          ad_type,
          adset,
          adset_id,
          campaign_id,
          channel,
          keywords,
          siteid,
          is_primary_attribution,
          is_retargeting ORDER BY event_time ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seen_timestamp
FROM raw_data
)
SELECT * FROM cte_data
WHERE is_primary_attribution IS NOT NULL
  AND is_retargeting IS NOT NULL;

MERGE `{project_id}.{app_id}_external.af_user_map` existing_map
USING appsflyer_map_{app_id} new_map
ON existing_map.appsflyer_id = new_map.appsflyer_id
AND IFNULL(existing_map.customer_user_id, 'null') = IFNULL(new_map.customer_user_id, 'null')
AND IFNULL(existing_map.user_id, 'null') = IFNULL(new_map.user_id, 'null')
AND IFNULL(existing_map.installation_id, 'null') = IFNULL(new_map.installation_id, 'null')
AND IFNULL(existing_map.media_source, 'null') = IFNULL(new_map.media_source, 'null')
AND IFNULL(existing_map.campaign, 'null') = IFNULL(new_map.campaign, 'null')
AND IFNULL(existing_map.ad, 'null') = IFNULL(new_map.ad, 'null')
AND IFNULL(existing_map.ad_id, 'null') = IFNULL(new_map.ad_id, 'null')
AND IFNULL(existing_map.ad_type, 'null') = IFNULL(new_map.ad_type, 'null')
AND IFNULL(existing_map.adset, 'null') = IFNULL(new_map.adset, 'null')
AND IFNULL(existing_map.adset_id, 'null') = IFNULL(new_map.adset_id, 'null')
AND IFNULL(existing_map.campaign_id, 'null') = IFNULL(new_map.campaign_id, 'null')
AND IFNULL(existing_map.channel, 'null') = IFNULL(new_map.channel, 'null')
AND IFNULL(existing_map.keywords, 'null') = IFNULL(new_map.keywords, 'null')
AND IFNULL(existing_map.siteid, 'null') = IFNULL(new_map.siteid, 'null')
AND existing_map.is_primary_attribution = new_map.is_primary_attribution
AND existing_map.is_retargeting = new_map.is_retargeting
WHEN NOT MATCHED THEN
  INSERT(appsflyer_id,
          customer_user_id,
          user_id,
          installation_id,
          media_source,
          campaign,
          ad,
          ad_id,
          ad_type,
          adset,
          adset_id,
          campaign_id,
          channel,
          keywords,
          siteid,
          is_primary_attribution,
          is_retargeting,
          first_seen_timestamp,
          last_seen_timestamp)
  VALUES(
      new_map.appsflyer_id,
      new_map.customer_user_id,
      new_map.user_id,
      new_map.installation_id,
      new_map.media_source,
      new_map.campaign,
      new_map.ad,
      new_map.ad_id,
      new_map.ad_type,
      new_map.adset,
      new_map.adset_id,
      new_map.campaign_id,
      new_map.channel,
      new_map.keywords,
      new_map.siteid,
      new_map.is_primary_attribution,
      new_map.is_retargeting,
      new_map.first_seen_timestamp,
      new_map.last_seen_timestamp)
WHEN MATCHED AND existing_map.last_seen_timestamp < new_map.last_seen_timestamp THEN
  UPDATE
    SET existing_map.last_seen_timestamp = new_map.last_seen_timestamp;
