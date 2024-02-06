   
    
    -- Set temp table names
    SET temp_table_name = CONCAT('event_clean_data_', temp_table_name_prefix, "_", app_id, "_", event_name);
 
    --Gets column names for event table
    EXECUTE IMMEDIATE CONCAT("(SELECT ARRAY (SELECT DISTINCT column_name FROM `", project_id, ".",app_id, "_", source_dataset_suffix, "`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '",event_name,"'))") INTO event_columns;

    --Gets data from events table. Query takes data from X days before start_timestamp, because of deduplication. 
    EXECUTE IMMEDIATE CONCAT("",
    "  CREATE OR REPLACE TEMPORARY TABLE ", temp_table_name, " AS",
    "  WITH processing_data AS",
    " (",
    "  SELECT ",
          ARRAY_TO_STRING(event_columns, ','),", ",
    "     ROW_NUMBER() OVER (PARTITION BY unique_id, event_fingerprint ORDER BY collector_tstamp, enricher_tstamp, load_tstamp ASC) AS rank_event_id",
    "  FROM `", project_id, ".", app_id, "_", source_dataset_suffix, ".", event_name, "`",
    "  WHERE date_ BETWEEN DATE_SUB('", execution_date, "', INTERVAL ", days_for_deduplication, " DAY) AND '", execution_date, "'",
    "   AND (sandbox_mode IS FALSE OR sandbox_mode IS NULL)",
    "   AND unique_id NOT IN (SELECT unique_id FROM `{project_id}.{app_id}_fix.excluded_unique_ids`)",
    " ), deduplicated_data AS",
    " (",
    "   SELECT",
    "     *",
    "   FROM processing_data",
    "   WHERE rank_event_id = 1",
    " )",
    " SELECT ",
    "   d.* EXCEPT(rank_event_id)",
    " FROM deduplicated_data d",
    " WHERE",
    "   date_ = '", execution_date, "'");
