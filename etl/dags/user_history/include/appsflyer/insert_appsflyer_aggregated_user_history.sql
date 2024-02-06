-- Description: This script is used to insert raw appsflyer data into appsflyer_aggregated_user_history table.

DECLARE execution_date DEFAULT DATE '{execution_date}';

CREATE TEMP FUNCTION GetMediaSource(media_source STRING)
RETURNS STRING
AS (
  IF(media_source LIKE '%rtbhouse_int%', 'rtbhouse_int', LOWER(media_source))
);

CREATE OR REPLACE TEMP TABLE aggregated_history_new_{app_id}
PARTITION BY date_
AS
WITH geo_cost AS 
(
SELECT
  DATE(date_) AS date_,
  DATE(date_) AS registration_date,
  DATE_DIFF(DATE(date_), DATE(date_), DAY) AS cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(geo, '') AS country_code,
  IF(app_id = '{android_app_id}', 'android', IF(app_id = '{ios_app_id}', 'ios', '')) AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(CAST(cost AS FLOAT64)) AS cost,
  SUM(CAST(impressions AS INT64)) AS impressions,
  SUM(CAST(clicks AS INT64)) AS clicks,
  SUM(CAST(installs AS INT64)) AS installs,
  SUM(CAST(re_engagements AS INT64)) AS re_engagements,
  SUM(CAST(re_attributions AS INT64)) AS re_attributions,
FROM `{project_id}.{app_id}_external.af_cost_geo`
WHERE DATE(date_) = execution_date
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), channel_cost AS 
(
  SELECT
    DATE(date_) AS date_,
    DATE(date_) AS registration_date,
    DATE_DIFF(DATE(date_), DATE(date_), DAY) AS cohort_day,
    IFNULL(GetMediaSource(media_source), '') AS media_source,
    '' AS campaign,
    IFNULL(campaign_id, '') AS campaign_id,
    IFNULL(adset, '') AS adset,
    IFNULL(adset_id, '') AS adset_id,
    IFNULL(ad, '') AS ad,
    IFNULL(ad_id, '') AS ad_id,
    IF(app_id = '{android_app_id}', 'android', IF(app_id = '{ios_app_id}', 'ios', '')) AS platform,
    IFNULL(keywords, '') AS keywords,

    SUM(CAST(cost AS FLOAT64)) AS cost,
    SUM(CAST(impressions AS INT64)) AS impressions,
    SUM(CAST(clicks AS INT64)) AS clicks,
    SUM(CAST(installs AS INT64)) AS installs,
    SUM(CAST(re_engagements AS INT64)) AS re_engagements,
    SUM(CAST(re_attributions AS INT64)) AS re_attributions,
  FROM `{project_id}.{app_id}_external.af_cost_channel`
  WHERE media_source = 'Facebook Ads' 
    AND DATE(date_) = execution_date
  GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
), geo_cost_agg AS (
  SELECT
    date_,
    registration_date,
    cohort_day,
    media_source,
    campaign,
    campaign_id,
    adset,
    adset_id,
    ad,
    ad_id,
    platform,
    keywords,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(installs) AS installs,
    SUM(re_engagements) AS re_engagements,
    SUM(re_attributions) AS re_attributions,
  FROM geo_cost
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
), non_organic_users AS
(
  SELECT
    date_,
    registration_date,
    cohort_day,
    media_source,
    campaign,
    campaign_id,
    adset,
    adset_id,
    ad,
    ad_id,
    '' AS country_code,
    platform,
    keywords,
    IF(c.cost - IFNULL(g.cost, 0) > 0, c.cost - IFNULL(g.cost, 0), 0) AS cost,
    IF(c.impressions - IFNULL(g.impressions, 0) > 0, c.impressions - IFNULL(g.impressions, 0), 0) AS impressions,
    IF(c.clicks - IFNULL(g.clicks, 0) > 0, c.clicks - IFNULL(g.clicks, 0), 0) AS clicks,
    IF(c.installs - IFNULL(g.installs, 0) > 0, c.installs - IFNULL(g.installs, 0), 0) AS installs,
    IF(c.re_engagements - IFNULL(g.re_engagements, 0) > 0, c.re_engagements - IFNULL(g.re_engagements, 0), 0) AS re_engagements,
    IF(c.re_attributions - IFNULL(g.re_attributions, 0) > 0, c.re_attributions - IFNULL(g.re_attributions, 0), 0) AS re_attributions,
  FROM channel_cost c
  LEFT JOIN geo_cost_agg g
  USING(date_, registration_date, cohort_day, media_source, campaign, campaign_id, adset, adset_id, ad, ad_id, platform, keywords)
  WHERE 
    c.cost - IFNULL(g.cost, 0) > 0
    OR c.impressions - IFNULL(g.impressions, 0) > 0
    OR c.clicks - IFNULL(g.clicks, 0) > 0
    OR c.installs - IFNULL(g.installs, 0) > 0
    OR c.re_engagements - IFNULL(g.re_engagements, 0) > 0
    OR c.re_attributions - IFNULL(g.re_attributions, 0) > 0
  UNION ALL
  SELECT * FROM geo_cost
), organic_users AS
(
SELECT
  date_,
  date_ AS registration_date,
  cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(registration_country, '') AS country_code,
  registration_platform,
  IFNULL(keywords, '') AS keywords,

  CAST(NULL AS FLOAT64) AS cost,
  CAST(NULL AS INT64) AS impressions,
  CAST(NULL AS INT64) AS clicks,
  COUNT(*) AS installs,
  CAST(NULL AS INT64) AS re_engagements,
  CAST(NULL AS INT64) AS re_attributions,

FROM `{project_id}.{app_id}_main.appsflyer_user_history`
WHERE date_ = execution_date AND cohort_day = 0
AND GetMediaSource(media_source) = 'organic'
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), cohort_base AS
(
SELECT * FROM non_organic_users
UNION ALL
SELECT * FROM organic_users
), user_level_cohort_data AS
(
  SELECT
  date_,
  DATE(registration_date) AS registration_date,
  cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(registration_country, '') AS country_code,
  registration_platform AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(daily_payers) AS iap_revenue_unique,
  SUM(transactions_count) AS iap_revenue_count,
  SUM(transactions_count_total) AS iap_revenue_count_total,
  SUM(gross_revenue_usd) AS iap_revenue_usd,
  SUM(gross_revenue_usd_total) AS iap_revenue_usd_total,
  SUM(new_payers) AS payer_conversions,
  SUM(new_payers) AS payer_conversions_total,
  SUM(IF(ad_revenue_usd > 0, 1, 0)) AS ad_revenue_unique,
  SUM(ad_revenue_count) AS ad_revenue_count,
  SUM(ad_revenue_count_total) AS ad_revenue_count_total,
  SUM(ad_revenue_usd) AS ad_revenue_usd,
  SUM(ad_revenue_usd_total) AS ad_revenue_usd_total,
  SUM(dau) AS unique_users,
  SUM(sessions_count) AS session_count,
  SUM(sessions_count_total) AS session_count_total,

  CAST(NULL AS INT64) AS re_attribution_iap_revenue_unique,
  CAST(NULL AS INT64) AS re_attribution_iap_revenue_count,
  CAST(NULL AS INT64) AS re_attribution_iap_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_attribution_iap_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_attribution_iap_revenue_usd_total,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_unique,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_count,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_attribution_ad_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_attribution_ad_revenue_usd_total,

  CAST(NULL AS INT64) AS re_engagement_iap_revenue_unique,
  CAST(NULL AS INT64) AS re_engagement_iap_revenue_count,
  CAST(NULL AS INT64) AS re_engagement_iap_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_engagement_iap_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_engagement_iap_revenue_usd_total,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_unique,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_count,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_engagement_ad_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_engagement_ad_revenue_usd_total,

FROM `{project_id}.{app_id}_main.appsflyer_user_history`
WHERE date_ = execution_date AND cohort_day = 0
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), ad_revenue_organic AS
(
  SELECT 
  DATE(event_date) AS date_,
  DATE(conversion_date) AS registration_date,
  DATE_DIFF(DATE(event_date), DATE(conversion_date), DAY) AS cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(geo, '') AS country_code,
  IF(app_id = '{android_app_id}', 'android', IF(app_id = '{ios_app_id}', 'ios', '')) AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(IF(event_name = 'af_ad_revenue', CAST(unique_users AS INT64), 0)) AS ad_revenue_unique,
  SUM(IF(event_name = 'af_ad_revenue', CAST(event_count AS INT64), 0))AS ad_revenue_count,
  SUM(IF(event_name = 'af_ad_revenue', CAST(event_count AS INT64), 0))AS ad_revenue_count_total,
  SUM(IF(event_name = 'af_ad_revenue', CAST(revenue_usd AS FLOAT64), 0)) AS ad_revenue_usd,
  SUM(IF(event_name = 'af_ad_revenue', CAST(revenue_usd AS FLOAT64), 0)) AS ad_revenue_usd_total,
FROM `{project_id}.{app_id}_external.af_cohort_unified`
WHERE DATE(event_date) = execution_date AND DATE(conversion_date) = execution_date
AND DATE(dt) BETWEEN DATE_SUB(execution_date, INTERVAL 2 DAY)  AND DATE_ADD(execution_date, INTERVAL 2 DAY)
AND GetMediaSource(media_source) = 'organic'
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), re_att_data AS
(
  SELECT
  DATE(event_date) AS date_,
  DATE(conversion_date) AS registration_date,
  DATE_DIFF(DATE(event_date), DATE(conversion_date), DAY) AS cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(geo, '') AS country_code,
  IF(app_id = '{android_app_id}', 'android', IF(app_id = '{ios_app_id}', 'ios', '')) AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(unique_users AS INT64), 0)) AS re_attribution_iap_revenue_unique,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0)) AS re_attribution_iap_revenue_count,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0)) AS re_attribution_iap_revenue_count_total,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_iap_revenue_usd,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_iap_revenue_usd_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(unique_users AS INT64), 0)) AS re_attribution_ad_revenue_unique,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0))AS re_attribution_ad_revenue_count,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0))AS re_attribution_ad_revenue_count_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_ad_revenue_usd,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_ad_revenue_usd_total,

  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(unique_users AS INT64), 0)) AS re_engagement_iap_revenue_unique,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0)) AS re_engagement_iap_revenue_count,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0)) AS re_engagement_iap_revenue_count_total,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_iap_revenue_usd,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_iap_revenue_usd_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(unique_users AS INT64), 0)) AS re_engagement_ad_revenue_unique,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0))AS re_engagement_ad_revenue_count,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0))AS re_engagement_ad_revenue_count_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_ad_revenue_usd,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_ad_revenue_usd_total,
FROM `{project_id}.{app_id}_external.af_cohort_unified`
WHERE DATE(event_date) = execution_date AND DATE(conversion_date) = execution_date
AND DATE(dt) BETWEEN DATE_SUB(execution_date, INTERVAL 2 DAY)  AND DATE_ADD(execution_date, INTERVAL 2 DAY)
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
HAVING re_attribution_iap_revenue_unique > 0
    OR re_attribution_ad_revenue_unique > 0
    OR re_engagement_iap_revenue_unique > 0
    OR re_engagement_ad_revenue_unique > 0
), cohort_data AS
(
SELECT
  date_,
  registration_date,
  cohort_day,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country_code,
  platform,
  keywords,

  IFNULL(iap_revenue_unique, 0) AS iap_revenue_unique,
  IFNULL(iap_revenue_count, 0) AS iap_revenue_count,
  IFNULL(iap_revenue_count_total, 0) AS iap_revenue_count_total,
  IFNULL(iap_revenue_usd, 0) AS iap_revenue_usd,
  IFNULL(iap_revenue_usd_total, 0) AS iap_revenue_usd_total,
  IFNULL(payer_conversions, 0) AS payer_conversions,
  IFNULL(payer_conversions_total, 0) AS payer_conversions_total,
  IFNULL(IF(media_source = 'organic', o.ad_revenue_unique, c.ad_revenue_unique), 0) AS ad_revenue_unique,
  IFNULL(IF(media_source = 'organic', o.ad_revenue_count, c.ad_revenue_count), 0) AS ad_revenue_count,
  IFNULL(IF(media_source = 'organic', o.ad_revenue_count_total, c.ad_revenue_count_total), 0) AS ad_revenue_count_total,
  IFNULL(IF(media_source = 'organic', o.ad_revenue_usd, c.ad_revenue_usd), 0) AS ad_revenue_usd,
  IFNULL(IF(media_source = 'organic', o.ad_revenue_usd_total, c.ad_revenue_usd_total), 0) AS ad_revenue_usd_total,
  IFNULL(unique_users, 0) AS unique_users,
  IFNULL(session_count, 0) AS session_count,
  IFNULL(session_count_total,  0) AS session_count_total,

  IFNULL(r.re_attribution_iap_revenue_unique, 0) AS re_attribution_iap_revenue_unique,
  IFNULL(r.re_attribution_iap_revenue_count, 0) AS re_attribution_iap_revenue_count,
  IFNULL(r.re_attribution_iap_revenue_count_total, 0) AS re_attribution_iap_revenue_count_total,
  IFNULL(r.re_attribution_iap_revenue_usd, 0) AS re_attribution_iap_revenue_usd,
  IFNULL(r.re_attribution_iap_revenue_usd_total, 0) AS re_attribution_iap_revenue_usd_total,
  IFNULL(r.re_attribution_ad_revenue_unique, 0) AS re_attribution_ad_revenue_unique,
  IFNULL(r.re_attribution_ad_revenue_count, 0) AS re_attribution_ad_revenue_count,
  IFNULL(r.re_attribution_ad_revenue_count_total, 0) AS re_attribution_ad_revenue_count_total,
  IFNULL(r.re_attribution_ad_revenue_usd, 0) AS re_attribution_ad_revenue_usd,
  IFNULL(r.re_attribution_ad_revenue_usd_total, 0) AS re_attribution_ad_revenue_usd_total,

  IFNULL(r.re_engagement_iap_revenue_unique, 0) AS re_engagement_iap_revenue_unique,
  IFNULL(r.re_engagement_iap_revenue_count, 0) AS re_engagement_iap_revenue_count,
  IFNULL(r.re_engagement_iap_revenue_count_total, 0) AS re_engagement_iap_revenue_count_total,
  IFNULL(r.re_engagement_iap_revenue_usd, 0) AS re_engagement_iap_revenue_usd,
  IFNULL(r.re_engagement_iap_revenue_usd_total, 0) AS re_engagement_iap_revenue_usd_total,
  IFNULL(r.re_engagement_ad_revenue_unique, 0) AS re_engagement_ad_revenue_unique,
  IFNULL(r.re_engagement_ad_revenue_count, 0) AS re_engagement_ad_revenue_count,
  IFNULL(r.re_engagement_ad_revenue_count_total, 0) AS re_engagement_ad_revenue_count_total,
  IFNULL(r.re_engagement_ad_revenue_usd, 0) AS re_engagement_ad_revenue_usd,
  IFNULL(r.re_engagement_ad_revenue_usd_total, 0) AS re_engagement_ad_revenue_usd_total,

FROM user_level_cohort_data c
FULL OUTER JOIN re_att_data r
USING(date_,registration_date,cohort_day,media_source,campaign,campaign_id,adset,adset_id,ad,ad_id,country_code,platform,keywords)
LEFT JOIN ad_revenue_organic o
USING(date_,registration_date,cohort_day,media_source,campaign,campaign_id,adset,adset_id,ad,ad_id,country_code,platform,keywords)
)
SELECT
COALESCE(cohort_base.date_, cohort_data.date_) AS date_,
COALESCE(cohort_base.registration_date, cohort_data.registration_date) AS registration_date,
COALESCE(cohort_base.cohort_day, cohort_data.cohort_day) AS cohort_day,
COALESCE(cohort_base.media_source, cohort_data.media_source) AS media_source,
IF(COALESCE(cohort_base.media_source, cohort_data.media_source) = 'organic', 'organic', COALESCE(cohort_base.campaign, cohort_data.campaign)) AS campaign,
COALESCE(cohort_base.campaign_id, cohort_data.campaign_id) AS campaign_id,
COALESCE(cohort_base.adset, cohort_data.adset) AS adset,
COALESCE(cohort_base.adset_id, cohort_data.adset_id) AS adset_id,
COALESCE(cohort_base.ad, cohort_data.ad) AS ad,
COALESCE(cohort_base.ad_id, cohort_data.ad_id) AS ad_id,
COALESCE(cohort_base.country_code, cohort_data.country_code) AS country_code,
country_codes_map.geo_country_name AS country_name,
COALESCE(cohort_base.platform, cohort_data.platform) AS platform,
COALESCE(cohort_base.keywords, cohort_data.keywords) AS keywords,
cohort_base.cost,
cohort_base.impressions,
cohort_base.clicks,
cohort_base.installs,
cohort_data.iap_revenue_unique,
cohort_data.iap_revenue_count,
cohort_data.iap_revenue_count_total,
cohort_data.iap_revenue_usd,
cohort_data.iap_revenue_usd_total,
cohort_data.payer_conversions,
cohort_data.payer_conversions_total,
cohort_data.ad_revenue_unique,
cohort_data.ad_revenue_count, 
cohort_data.ad_revenue_count_total,
cohort_data.ad_revenue_usd,
cohort_data.ad_revenue_usd_total,
cohort_data.unique_users,
cohort_data.session_count,
cohort_data.session_count_total,
cohort_data.iap_revenue_usd * IFNULL(vat.net_revenue_index, 1) AS iap_net_revenue_usd,
cohort_data.iap_revenue_usd * IFNULL(vat.net_revenue_index, 1) AS iap_net_revenue_usd_total,
cohort_base.re_engagements,
cohort_base.re_attributions,

cohort_data.re_attribution_iap_revenue_unique,
cohort_data.re_attribution_iap_revenue_count,
cohort_data.re_attribution_iap_revenue_count_total,
cohort_data.re_attribution_iap_revenue_usd,
cohort_data.re_attribution_iap_revenue_usd_total,
cohort_data.re_attribution_ad_revenue_unique,
cohort_data.re_attribution_ad_revenue_count,
cohort_data.re_attribution_ad_revenue_count_total,
cohort_data.re_attribution_ad_revenue_usd,
cohort_data.re_attribution_ad_revenue_usd_total,
cohort_data.re_engagement_iap_revenue_unique,
cohort_data.re_engagement_iap_revenue_count,
cohort_data.re_engagement_iap_revenue_count_total,
cohort_data.re_engagement_iap_revenue_usd,
cohort_data.re_engagement_iap_revenue_usd_total,
cohort_data.re_engagement_ad_revenue_unique,
cohort_data.re_engagement_ad_revenue_count,
cohort_data.re_engagement_ad_revenue_count_total,
cohort_data.re_engagement_ad_revenue_usd,
cohort_data.re_engagement_ad_revenue_usd_total,
cohort_data.re_attribution_iap_revenue_usd * IFNULL(vat.net_revenue_index, 1) AS re_attribution_iap_net_revenue_usd,
cohort_data.re_attribution_iap_revenue_usd_total * IFNULL(vat.net_revenue_index, 1) AS re_attribution_iap_net_revenue_usd_total,
cohort_data.re_engagement_iap_revenue_usd * IFNULL(vat.net_revenue_index, 1) AS re_engagement_iap_net_revenue_usd,
cohort_data.re_engagement_iap_revenue_usd_total * IFNULL(vat.net_revenue_index, 1) AS re_engagement_iap_net_revenue_usd_total
FROM cohort_base 
FULL OUTER JOIN cohort_data
USING(date_, registration_date, media_source, campaign, campaign_id, adset, adset_id, ad, ad_id, country_code, platform,keywords)
LEFT JOIN `{project_id}.gametuner_common.appsflyer_country_codes_map` AS country_codes_map
ON country_codes_map.geo_country = cohort_base.country_code
LEFT JOIN `{project_id}.gametuner_common.country_vat` AS vat
ON vat.appsflyer_country_code = cohort_base.country_code;


CREATE OR REPLACE TEMP TABLE aggregated_history_daily_{app_id}
AS
WITH user_level_cohort_data AS
(
SELECT
  date_,
  registration_date,
  cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  IF(IFNULL(GetMediaSource(media_source), '') = 'organic', 'organic', '') AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(registration_country, '') AS country_code,
  registration_platform AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(daily_payers) AS iap_revenue_unique,
  SUM(transactions_count) AS iap_revenue_count,
  SUM(transactions_count_total) AS iap_revenue_count_total,
  SUM(gross_revenue_usd) AS iap_revenue_usd,
  SUM(gross_revenue_usd_total) AS iap_revenue_usd_total,
  SUM(new_payers) AS payer_conversions,
  SUM(new_payers) AS payer_conversions_total,
  SUM(IF(ad_revenue_usd > 0, 1, 0)) AS ad_revenue_unique,
  SUM(ad_revenue_count) AS ad_revenue_count,
  SUM(ad_revenue_count_total) AS ad_revenue_count_total,
  SUM(ad_revenue_usd) AS ad_revenue_usd,
  SUM(ad_revenue_usd_total) AS ad_revenue_usd_total,
  SUM(dau) AS unique_users,
  SUM(sessions_count) AS session_count,
  SUM(sessions_count_total) AS session_count_total,

  CAST(NULL AS INT64) AS re_attribution_iap_revenue_unique,
  CAST(NULL AS INT64) AS re_attribution_iap_revenue_count,
  CAST(NULL AS INT64) AS re_attribution_iap_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_attribution_iap_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_attribution_iap_revenue_usd_total,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_unique,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_count,
  CAST(NULL AS INT64) AS re_attribution_ad_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_attribution_ad_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_attribution_ad_revenue_usd_total,

  CAST(NULL AS INT64) AS re_engagement_iap_revenue_unique,
  CAST(NULL AS INT64) AS re_engagement_iap_revenue_count,
  CAST(NULL AS INT64) AS re_engagement_iap_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_engagement_iap_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_engagement_iap_revenue_usd_total,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_unique,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_count,
  CAST(NULL AS INT64) AS re_engagement_ad_revenue_count_total,
  CAST(NULL AS FLOAT64) AS re_engagement_ad_revenue_usd,
  CAST(NULL AS FLOAT64) AS re_engagement_ad_revenue_usd_total,
FROM `{project_id}.{app_id}_main.appsflyer_user_history`
WHERE date_ = execution_date
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), re_att_data AS
(
  SELECT
  DATE(event_date) AS date_,
  DATE(conversion_date) AS registration_date,
  DATE_DIFF(DATE(event_date), DATE(conversion_date), DAY) AS cohort_day,
  IFNULL(GetMediaSource(media_source), '') AS media_source,
  '' AS campaign,
  IFNULL(campaign_id, '') AS campaign_id,
  IFNULL(adset, '') AS adset,
  IFNULL(adset_id, '') AS adset_id,
  IFNULL(ad, '') AS ad,
  IFNULL(ad_id, '') AS ad_id,
  IFNULL(geo, '') AS country_code,
  IF(app_id = '{android_app_id}', 'android', IF(app_id = '{ios_app_id}', 'ios', '')) AS platform,
  IFNULL(keywords, '') AS keywords,

  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(unique_users AS INT64), 0)) AS re_attribution_iap_revenue_unique,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0)) AS re_attribution_iap_revenue_count,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0)) AS re_attribution_iap_revenue_count_total,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_iap_revenue_usd,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_iap_revenue_usd_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(unique_users AS INT64), 0)) AS re_attribution_ad_revenue_unique,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0))AS re_attribution_ad_revenue_count,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(event_count AS INT64), 0))AS re_attribution_ad_revenue_count_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_ad_revenue_usd,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-attribution', CAST(revenue_usd AS FLOAT64), 0)) AS re_attribution_ad_revenue_usd_total,

  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(unique_users AS INT64), 0)) AS re_engagement_iap_revenue_unique,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0)) AS re_engagement_iap_revenue_count,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0)) AS re_engagement_iap_revenue_count_total,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_iap_revenue_usd,
  SUM(IF(event_name = 'af_purchase' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_iap_revenue_usd_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(unique_users AS INT64), 0)) AS re_engagement_ad_revenue_unique,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0))AS re_engagement_ad_revenue_count,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(event_count AS INT64), 0))AS re_engagement_ad_revenue_count_total,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_ad_revenue_usd,
  SUM(IF(event_name = 'af_ad_revenue' AND conversion_type = 're-engagement', CAST(revenue_usd AS FLOAT64), 0)) AS re_engagement_ad_revenue_usd_total,
FROM `{project_id}.{app_id}_external.af_cohort_unified`
WHERE DATE(event_date) = execution_date
AND DATE(dt) BETWEEN DATE_SUB(execution_date, INTERVAL 2 DAY)  AND DATE_ADD(execution_date, INTERVAL 2 DAY)
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
HAVING re_attribution_iap_revenue_unique > 0
    OR re_attribution_ad_revenue_unique > 0
    OR re_engagement_iap_revenue_unique > 0
    OR re_engagement_ad_revenue_unique > 0
), af_cohort_unified AS
(
SELECT
  date_,
  registration_date,
  cohort_day,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country_code,
  platform,
  keywords,

  IFNULL(iap_revenue_unique, 0) AS iap_revenue_unique,
  IFNULL(iap_revenue_count, 0) AS iap_revenue_count,
  IFNULL(iap_revenue_count_total, 0) AS iap_revenue_count_total,
  IFNULL(iap_revenue_usd, 0) AS iap_revenue_usd,
  IFNULL(iap_revenue_usd_total, 0) AS iap_revenue_usd_total,
  IFNULL(payer_conversions, 0) AS payer_conversions,
  IFNULL(payer_conversions, 0) AS payer_conversions_total,
  IFNULL(ad_revenue_unique, 0) AS ad_revenue_unique,
  IFNULL(ad_revenue_count, 0) AS ad_revenue_count,
  IFNULL(ad_revenue_count_total, 0) AS ad_revenue_count_total,
  IFNULL(ad_revenue_usd, 0) AS ad_revenue_usd,
  IFNULL(ad_revenue_usd_total, 0) AS ad_revenue_usd_total,
  IFNULL(unique_users, 0) AS unique_users,
  IFNULL(session_count, 0) AS session_count,
  IFNULL(session_count_total,  0) AS session_count_total,

  IFNULL(r.re_attribution_iap_revenue_unique, 0) AS re_attribution_iap_revenue_unique,
  IFNULL(r.re_attribution_iap_revenue_count, 0) AS re_attribution_iap_revenue_count,
  IFNULL(r.re_attribution_iap_revenue_count_total, 0) AS re_attribution_iap_revenue_count_total,
  IFNULL(r.re_attribution_iap_revenue_usd, 0) AS re_attribution_iap_revenue_usd,
  IFNULL(r.re_attribution_iap_revenue_usd_total, 0) AS re_attribution_iap_revenue_usd_total,
  IFNULL(r.re_attribution_ad_revenue_unique, 0) AS re_attribution_ad_revenue_unique,
  IFNULL(r.re_attribution_ad_revenue_count, 0) AS re_attribution_ad_revenue_count,
  IFNULL(r.re_attribution_ad_revenue_count_total, 0) AS re_attribution_ad_revenue_count_total,
  IFNULL(r.re_attribution_ad_revenue_usd, 0) AS re_attribution_ad_revenue_usd,
  IFNULL(r.re_attribution_ad_revenue_usd_total, 0) AS re_attribution_ad_revenue_usd_total,

  IFNULL(r.re_engagement_iap_revenue_unique, 0) AS re_engagement_iap_revenue_unique,
  IFNULL(r.re_engagement_iap_revenue_count, 0) AS re_engagement_iap_revenue_count,
  IFNULL(r.re_engagement_iap_revenue_count_total, 0) AS re_engagement_iap_revenue_count_total,
  IFNULL(r.re_engagement_iap_revenue_usd, 0) AS re_engagement_iap_revenue_usd,
  IFNULL(r.re_engagement_iap_revenue_usd_total, 0) AS re_engagement_iap_revenue_usd_total,
  IFNULL(r.re_engagement_ad_revenue_unique, 0) AS re_engagement_ad_revenue_unique,
  IFNULL(r.re_engagement_ad_revenue_count, 0) AS re_engagement_ad_revenue_count,
  IFNULL(r.re_engagement_ad_revenue_count_total, 0) AS re_engagement_ad_revenue_count_total,
  IFNULL(r.re_engagement_ad_revenue_usd, 0) AS re_engagement_ad_revenue_usd,
  IFNULL(r.re_engagement_ad_revenue_usd_total, 0) AS re_engagement_ad_revenue_usd_total,

FROM user_level_cohort_data c
FULL OUTER JOIN re_att_data r
USING(date_,registration_date,cohort_day,media_source,campaign,campaign_id,adset,adset_id,ad,ad_id,country_code,platform,keywords)
)
SELECT
  date_,
  registration_date,
  cohort_day,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country_code,
  country_codes_map.geo_country_name AS country_name,
  platform,
  keywords,

  iap_revenue_unique,
  iap_revenue_count,
  iap_revenue_count_total,
  iap_revenue_usd,
  iap_revenue_usd_total,
  payer_conversions,
  payer_conversions_total,
  ad_revenue_unique,
  ad_revenue_count,
  ad_revenue_count_total,
  ad_revenue_usd,
  ad_revenue_usd_total,
  unique_users,
  session_count,
  session_count_total,
  IFNULL(iap_revenue_usd * IFNULL(vat.net_revenue_index, 1), 0) AS iap_net_revenue_usd,

  re_attribution_iap_revenue_unique,
  re_attribution_iap_revenue_count,
  re_attribution_iap_revenue_count_total,
  re_attribution_iap_revenue_usd,
  re_attribution_iap_revenue_usd_total,
  re_attribution_ad_revenue_unique,
  re_attribution_ad_revenue_count,
  re_attribution_ad_revenue_count_total,
  re_attribution_ad_revenue_usd,
  re_attribution_ad_revenue_usd_total,

  re_engagement_iap_revenue_unique,
  re_engagement_iap_revenue_count,
  re_engagement_iap_revenue_count_total,
  re_engagement_iap_revenue_usd,
  re_engagement_iap_revenue_usd_total,
  re_engagement_ad_revenue_unique,
  re_engagement_ad_revenue_count,
  re_engagement_ad_revenue_count_total,
  re_engagement_ad_revenue_usd,
  re_engagement_ad_revenue_usd_total,

  IFNULL(re_attribution_iap_revenue_usd * IFNULL(vat.net_revenue_index, 1), 0) AS re_attribution_iap_net_revenue_usd,
  IFNULL(re_engagement_iap_revenue_usd * IFNULL(vat.net_revenue_index, 1), 0) AS re_engagement_iap_net_revenue_usd,
FROM af_cohort_unified
LEFT JOIN `{project_id}.gametuner_common.appsflyer_country_codes_map` AS country_codes_map
ON country_codes_map.geo_country = IFNULL(af_cohort_unified.country_code, '')
LEFT JOIN `{project_id}.gametuner_common.country_vat` AS vat
ON vat.appsflyer_country_code = IFNULL(af_cohort_unified.country_code, '');

CREATE TABLE IF NOT EXISTS `{project_id}.{app_id}_main.appsflyer_aggregated_user_history`
LIKE aggregated_history_new_{app_id}
OPTIONS(expiration_timestamp=NULL);

CREATE OR REPLACE TEMPORARY TABLE aggregated_history_existing_{app_id}
PARTITION BY date_
AS
SELECT
  execution_date AS date_,
  cohort_history.registration_date AS registration_date,
  DATE_DIFF(execution_date, cohort_history.registration_date, DAY) AS cohort_day,
  cohort_history.media_source AS media_source,
  cohort_history.campaign AS campaign,
  cohort_history.campaign_id AS campaign_id,
  cohort_history.adset AS adset,
  cohort_history.adset_id AS adset_id,
  cohort_history.ad AS ad,
  cohort_history.ad_id AS ad_id,
  cohort_history.country_code AS country_code,
  cohort_history.country_name AS country_name,
  cohort_history.platform AS platform,
  cohort_history.keywords AS keywords,

  cohort_history.cost AS cost,
  cohort_history.impressions AS impressions,
  cohort_history.clicks AS clicks,
  cohort_history.installs AS installs,

  IFNULL(cohort_history_daily.iap_revenue_unique, 0) AS iap_revenue_unique,
  IFNULL(cohort_history_daily.iap_revenue_count, 0) AS iap_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.iap_revenue_count_total, 0) + cohort_history_daily.iap_revenue_count, cohort_history.iap_revenue_count_total) AS iap_revenue_count_total,
  IFNULL(cohort_history_daily.iap_revenue_usd, 0) AS iap_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.iap_revenue_usd_total, 0) + cohort_history_daily.iap_revenue_usd, cohort_history.iap_revenue_usd_total) AS iap_revenue_usd_total,
  IFNULL(cohort_history_daily.payer_conversions, 0) AS payer_conversions,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.payer_conversions_total, 0) + cohort_history_daily.payer_conversions, cohort_history.payer_conversions_total) AS payer_conversions_total,

  IFNULL(cohort_history_daily.ad_revenue_unique, 0) AS ad_revenue_unique,
  IFNULL(cohort_history_daily.ad_revenue_count, 0) AS ad_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.ad_revenue_count_total, 0) + cohort_history_daily.ad_revenue_count, cohort_history.ad_revenue_count_total) AS ad_revenue_count_total,
  IFNULL(cohort_history_daily.ad_revenue_usd, 0) AS ad_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.ad_revenue_usd_total, 0) + cohort_history_daily.ad_revenue_usd, cohort_history.ad_revenue_usd_total) AS ad_revenue_usd_total,
  IFNULL(cohort_history_daily.unique_users, 0) AS unique_users,
  IFNULL(cohort_history_daily.session_count, 0) AS session_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.session_count_total, 0) + cohort_history_daily.session_count, cohort_history.session_count_total) AS session_count_total,
  IFNULL(cohort_history_daily.iap_net_revenue_usd, 0) AS iap_net_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.iap_net_revenue_usd_total, 0) + cohort_history_daily.iap_net_revenue_usd, cohort_history.iap_net_revenue_usd_total) AS iap_net_revenue_usd_total,

  cohort_history.re_engagements,
  cohort_history.re_attributions,

  IFNULL(cohort_history_daily.re_attribution_iap_revenue_unique, 0) AS re_attribution_iap_revenue_unique,
  IFNULL(cohort_history_daily.re_attribution_iap_revenue_count, 0) AS re_attribution_iap_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_attribution_iap_revenue_count_total, 0) + cohort_history_daily.re_attribution_iap_revenue_count, cohort_history.re_attribution_iap_revenue_count_total) AS re_attribution_iap_revenue_count_total,
  IFNULL(cohort_history_daily.re_attribution_iap_revenue_usd, 0) AS re_attribution_iap_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_attribution_iap_revenue_usd_total, 0) + cohort_history_daily.re_attribution_iap_revenue_usd, cohort_history.re_attribution_iap_revenue_usd_total) AS re_attribution_iap_revenue_usd_total,
  IFNULL(cohort_history_daily.re_attribution_ad_revenue_unique, 0) AS re_attribution_ad_revenue_unique,
  IFNULL(cohort_history_daily.re_attribution_ad_revenue_count, 0) AS re_attribution_ad_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_attribution_ad_revenue_count_total, 0) + cohort_history_daily.re_attribution_ad_revenue_count, cohort_history.re_attribution_ad_revenue_count_total) AS re_attribution_ad_revenue_count_total,
  IFNULL(cohort_history_daily.re_attribution_ad_revenue_usd, 0) AS re_attribution_ad_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_attribution_ad_revenue_usd_total, 0) + cohort_history_daily.re_attribution_ad_revenue_usd, cohort_history.re_attribution_ad_revenue_usd_total) AS re_attribution_ad_revenue_usd_total,

  IFNULL(cohort_history_daily.re_engagement_iap_revenue_unique, 0) AS re_engagement_iap_revenue_unique,
  IFNULL(cohort_history_daily.re_engagement_iap_revenue_count, 0) AS re_engagement_iap_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_engagement_iap_revenue_count_total, 0) + cohort_history_daily.re_engagement_iap_revenue_count, cohort_history.re_engagement_iap_revenue_count_total) AS re_engagement_iap_revenue_count_total,
  IFNULL(cohort_history_daily.re_engagement_iap_revenue_usd, 0) AS re_engagement_iap_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_engagement_iap_revenue_usd_total, 0) + cohort_history_daily.re_engagement_iap_revenue_usd, cohort_history.re_engagement_iap_revenue_usd_total) AS re_engagement_iap_revenue_usd_total,
  IFNULL(cohort_history_daily.re_engagement_ad_revenue_unique, 0) AS re_engagement_ad_revenue_unique,
  IFNULL(cohort_history_daily.re_engagement_ad_revenue_count, 0) AS re_engagement_ad_revenue_count,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_engagement_ad_revenue_count_total, 0) + cohort_history_daily.re_engagement_ad_revenue_count, cohort_history.re_engagement_ad_revenue_count_total) AS re_engagement_ad_revenue_count_total,
  IFNULL(cohort_history_daily.re_engagement_ad_revenue_usd, 0) AS re_engagement_ad_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_engagement_ad_revenue_usd_total, 0) + cohort_history_daily.re_engagement_ad_revenue_usd, cohort_history.re_engagement_ad_revenue_usd_total) AS re_engagement_ad_revenue_usd_total,

  IFNULL(cohort_history_daily.re_attribution_iap_net_revenue_usd, 0) AS re_attribution_iap_net_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_attribution_iap_net_revenue_usd_total, 0) + cohort_history_daily.re_attribution_iap_net_revenue_usd, cohort_history.re_attribution_iap_net_revenue_usd_total) AS re_attribution_iap_net_revenue_usd_total,
  IFNULL(cohort_history_daily.re_engagement_iap_net_revenue_usd, 0) AS re_engagement_iap_net_revenue_usd,
  IF(cohort_history_daily.registration_date IS NOT NULL, IFNULL(cohort_history.re_engagement_iap_net_revenue_usd_total, 0) + cohort_history_daily.re_engagement_iap_net_revenue_usd, cohort_history.re_engagement_iap_net_revenue_usd_total) AS re_engagement_iap_net_revenue_usd_total,

FROM `{project_id}.{app_id}_main.appsflyer_aggregated_user_history` AS cohort_history
LEFT JOIN aggregated_history_daily_{app_id} cohort_history_daily
USING(registration_date, media_source, campaign, campaign_id, adset, adset_id, ad, ad_id, country_code, platform, keywords)
WHERE cohort_history.date_ = DATE_SUB(execution_date, INTERVAL 1 DAY);

DELETE FROM `{project_id}.{app_id}_main.appsflyer_aggregated_user_history` WHERE date_ = execution_date;

INSERT INTO `{project_id}.{app_id}_main.appsflyer_aggregated_user_history`
SELECT * FROM aggregated_history_existing_{app_id}
UNION ALL
SELECT * FROM aggregated_history_new_{app_id}
WHERE (registration_date, media_source, campaign, campaign_id, adset, adset_id, ad, ad_id, country_code, platform, keywords) NOT IN (
  SELECT (registration_date, media_source, campaign, campaign_id, adset, adset_id, ad, ad_id, country_code, platform, keywords)
  FROM  `{project_id}.{app_id}_main.appsflyer_aggregated_user_history`
  WHERE date_ = DATE_SUB(execution_date, INTERVAL 1 DAY)
);

DROP FUNCTION GetMediaSource;

CREATE OR REPLACE VIEW `{project_id}.{app_id}_main.v_appsflyer_aggregated_user_history`
AS
WITH campaign_names_cte AS
(
  SELECT
    campaign_id,
    MIN(date_) AS date_
FROM `{project_id}.{app_id}_external.af_cost_geo`
GROUP BY 1
), campaign_names AS
(
SELECT
  DISTINCT campaign_id, FIRST_VALUE(campaign) OVER(PARTITION BY campaign_id ORDER BY date_ ASC) AS campaign
FROM `{project_id}.{app_id}_external.af_cost_geo`
WHERE (campaign_id, date_) IN (SELECT (campaign_id, date_) FROM campaign_names_cte)
)
SELECT
  date_,
  registration_date,
  cohort_day,
  IF(media_source = 'restricted', 'facebook ads', media_source) AS media_source,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country_code,
  country_name,
  platform,
  keywords,
  IFNULL(cost, 0) AS cost,
  IFNULL(impressions, 0) AS impressions,
  IFNULL(clicks, 0) AS clicks,
  IFNULL(installs, 0) AS installs,

  GREATEST(0, IFNULL(iap_revenue_unique, 0) - IFNULL(re_attribution_iap_revenue_unique, 0) - IFNULL(re_engagement_iap_revenue_unique, 0)) AS iap_revenue_unique,
  GREATEST(0, IFNULL(iap_revenue_count, 0) - IFNULL(re_attribution_iap_revenue_count, 0) - IFNULL(re_engagement_iap_revenue_count, 0)) AS iap_revenue_count,
  GREATEST(0, IFNULL(iap_revenue_count_total, 0) - IFNULL(re_attribution_iap_revenue_count_total, 0) - IFNULL(re_engagement_iap_revenue_count_total, 0)) AS iap_revenue_count_total,
  GREATEST(0, IFNULL(iap_revenue_usd, 0) - IFNULL(re_attribution_iap_revenue_usd, 0) - IFNULL(re_engagement_iap_revenue_usd, 0)) AS iap_revenue_usd,
  GREATEST(0, IFNULL(iap_revenue_usd_total, 0) - IFNULL(re_attribution_iap_revenue_usd_total, 0) - IFNULL(re_engagement_iap_revenue_usd_total, 0)) AS iap_revenue_usd_total,
  IFNULL(payer_conversions, 0) AS payer_conversions,
  IFNULL(payer_conversions_total, 0) AS payer_conversions_total,

  IFNULL(ad_revenue_unique, 0) AS ad_revenue_unique,
  IFNULL(ad_revenue_count, 0) AS ad_revenue_count,
  IFNULL(ad_revenue_count_total, 0) AS ad_revenue_count_total,
  IFNULL(ad_revenue_usd, 0) AS ad_revenue_usd,
  IFNULL(ad_revenue_usd_total, 0) AS ad_revenue_usd_total,

  IFNULL(unique_users, 0) AS unique_users,
  IFNULL(session_count, 0) AS session_count,
  IFNULL(session_count_total, 0) AS session_count_total,

  GREATEST(0, IFNULL(iap_net_revenue_usd, 0) - IFNULL(re_attribution_iap_net_revenue_usd, 0) - IFNULL(re_engagement_iap_net_revenue_usd, 0)) AS iap_net_revenue_usd,
  GREATEST(0, IFNULL(iap_net_revenue_usd_total, 0) - IFNULL(re_attribution_iap_net_revenue_usd_total, 0) - IFNULL(re_engagement_iap_net_revenue_usd_total, 0)) AS iap_net_revenue_usd_total,

  IFNULL(re_engagements, 0) AS re_engagements,
  IFNULL(re_attributions, 0) AS re_attributions,

  IFNULL(re_attribution_iap_revenue_unique, 0) AS re_attribution_iap_revenue_unique,
  IFNULL(re_attribution_iap_revenue_count, 0) AS re_attribution_iap_revenue_count,
  IFNULL(re_attribution_iap_revenue_count_total, 0) AS re_attribution_iap_revenue_count_total,
  IFNULL(re_attribution_iap_revenue_usd, 0) AS re_attribution_iap_revenue_usd,
  IFNULL(re_attribution_iap_revenue_usd_total, 0) AS re_attribution_iap_revenue_usd_total,

  IFNULL(re_attribution_ad_revenue_unique, 0) AS re_attribution_ad_revenue_unique,
  IFNULL(re_attribution_ad_revenue_count, 0) AS re_attribution_ad_revenue_count,
  IFNULL(re_attribution_ad_revenue_count_total, 0) AS re_attribution_ad_revenue_count_total,
  IFNULL(re_attribution_ad_revenue_usd, 0) AS re_attribution_ad_revenue_usd,
  IFNULL(re_attribution_ad_revenue_usd_total, 0) AS re_attribution_ad_revenue_usd_total,

  IFNULL(re_engagement_iap_revenue_unique, 0) AS re_engagement_iap_revenue_unique,
  IFNULL(re_engagement_iap_revenue_count, 0) AS re_engagement_iap_revenue_count,
  IFNULL(re_engagement_iap_revenue_count_total, 0) AS re_engagement_iap_revenue_count_total,
  IFNULL(re_engagement_iap_revenue_usd, 0) AS re_engagement_iap_revenue_usd,
  IFNULL(re_engagement_iap_revenue_usd_total, 0) AS re_engagement_iap_revenue_usd_total,

  IFNULL(re_engagement_ad_revenue_unique, 0) AS re_engagement_ad_revenue_unique,
  IFNULL(re_engagement_ad_revenue_count, 0) AS re_engagement_ad_revenue_count,
  IFNULL(re_engagement_ad_revenue_count_total, 0) AS re_engagement_ad_revenue_count_total,
  IFNULL(re_engagement_ad_revenue_usd, 0) AS re_engagement_ad_revenue_usd,
  IFNULL(re_engagement_ad_revenue_usd_total, 0) AS re_engagement_ad_revenue_usd_total,

  IFNULL(re_attribution_iap_net_revenue_usd, 0) AS re_attribution_iap_net_revenue_usd,
  IFNULL(re_attribution_iap_net_revenue_usd_total, 0) AS re_attribution_iap_net_revenue_usd_total,

  IFNULL(re_engagement_iap_net_revenue_usd, 0) AS re_engagement_iap_net_revenue_usd,
  IFNULL(re_engagement_iap_net_revenue_usd_total, 0) AS re_engagement_iap_net_revenue_usd_total,

  cn.campaign AS campaign,
  DATE_TRUNC(registration_date, WEEK(MONDAY)) AS registration_week,
  DATE_TRUNC(registration_date, MONTH) AS registration_month
FROM `{project_id}.{app_id}_main.appsflyer_aggregated_user_history` ch
LEFT JOIN campaign_names cn
USING(campaign_id);
