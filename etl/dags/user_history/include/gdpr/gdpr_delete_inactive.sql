-- Description: This file contains the SQL for deleting inactive users from the gdpr tables.
-- Inactive users are users that have not been active for 2 years. Thay did not have any event in the last 2 years.

DECLARE gdpr_tables ARRAY<STRING>;
DECLARE i INT64;

BEGIN

  BEGIN TRANSACTION;

  CREATE OR REPLACE TEMP TABLE delete_unique_ids_inactive_{app_id}
  AS 
  WITH active_before AS 
  (
  SELECT 
    DISTINCT unique_id
  FROM `{project_id}.{app_id}_main.user_history_daily`
  WHERE date_ = DATE_SUB('{execution_date}', INTERVAL 365 * 2 DAY)
  ), active AS
  (
  SELECT 
    DISTINCT unique_id
  FROM `{project_id}.{app_id}_main.user_history_daily`
  WHERE date_ > DATE_SUB('{execution_date}', INTERVAL 365 * 2 DAY)
  AND unique_id IN (SELECT unique_id FROM active_before)
  )
  SELECT 
    unique_id, TIMESTAMP('{execution_date}') AS event_tstamp
  FROM active_before
  WHERE unique_id NOT IN (SELECT unique_id FROM active);

  # get gdpr tables
  SET gdpr_tables = ARRAY(SELECT table_name FROM `{project_id}.{app_id}_gdpr`.INFORMATION_SCHEMA.TABLES WHERE table_name <> 'gdpr_delete_request_log');

  # set original gdpr values to null
  SET i = 0;
  WHILE i < ARRAY_LENGTH(gdpr_tables) DO
  
    EXECUTE IMMEDIATE CONCAT("",
    " UPDATE `{project_id}.{app_id}_gdpr.",gdpr_tables[OFFSET(i)],"`",
    " SET ",
    "   deleted_at = CURRENT_TIMESTAMP(),",
    "   field_original_value = NULL",
    " WHERE field_original_value IS NOT NULL",
    " AND unique_id IN (",
    "   SELECT ",
    "     unique_id",
    "   FROM delete_unique_ids_inactive_{app_id}",
    " );");
    SET i = i + 1;
  END WHILE; 

  # insert delete log
  INSERT INTO `{project_id}.{app_id}_gdpr.gdpr_delete_request_log` (unique_id, requested_at, deleted_at, reason, email, is_email_sent)
  SELECT 
    d.unique_id,
    TIMESTAMP('{execution_date}') AS requested_at,
    CURRENT_TIMESTAMP() AS deleted_at,
    'INACTIVE_USER' AS reason,
    CAST(NULL AS STRING) AS email,
    FALSE AS is_email_sent
  FROM delete_unique_ids_inactive_{app_id} d
  LEFT JOIN `{project_id}.{app_id}_gdpr.gdpr_delete_request_log` log
  ON d.event_tstamp = log.requested_at AND d.unique_id = log.unique_id
  WHERE log.unique_id IS NULL;

COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Roll back the transaction inside the exception handler.
  SELECT @@error.message;
  ROLLBACK TRANSACTION;
END;