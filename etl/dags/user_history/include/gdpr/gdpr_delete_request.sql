-- Description: This file contains the SQL for deleting user data based on GDPR delete request.

DECLARE gdpr_tables ARRAY<STRING>;
DECLARE i INT64;

BEGIN

  BEGIN TRANSACTION;

  CREATE OR REPLACE TEMP TABLE delete_unique_ids_{app_id}
  AS 
  SELECT 
    unique_id, MIN(event_tstamp) AS event_tstamp, ANY_VALUE(params.email) AS email
  FROM `{project_id}.{app_id}_raw.gdpr_delete_request`
  WHERE date_ = '{execution_date}'
  GROUP BY 1;

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
    "   FROM delete_unique_ids_{app_id}",
    " );");
    SET i = i + 1;
  END WHILE; 

  # insert delete log
  INSERT INTO `{project_id}.{app_id}_gdpr.gdpr_delete_request_log` (unique_id, requested_at, deleted_at, reason, email, is_email_sent)
  SELECT 
    d.unique_id,
    d.event_tstamp AS requested_at,
    CURRENT_TIMESTAMP() AS deleted_at,
    'USER_REQUEST' AS reason,
    d.email,
    FALSE AS is_email_sent
  FROM delete_unique_ids d
  LEFT JOIN `{project_id}.{app_id}_gdpr.gdpr_delete_request_log` log
  ON d.event_tstamp = log.requested_at AND d.unique_id = log.unique_id
  WHERE log.unique_id IS NULL;

  # delete event from gdpr_delete_request table
  DELETE FROM `{project_id}.{app_id}_raw.gdpr_delete_request`
  WHERE date_ = '{execution_date}'
  AND unique_id IN (
    SELECT 
      unique_id
    FROM delete_unique_ids_{app_id}
  );

COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Roll back the transaction inside the exception handler.
  SELECT @@error.message;
  ROLLBACK TRANSACTION;
END;