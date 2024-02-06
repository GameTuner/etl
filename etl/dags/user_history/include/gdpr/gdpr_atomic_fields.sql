-- Description: Create atomic tables for GDPR data and pseudonymize data.

    DECLARE execution_date DATE DEFAULT DATE("{execution_date}");
    DECLARE app_id STRING DEFAULT '{app_id}';
    DECLARE project_id STRING DEFAULT '{project_id}';
    DECLARE temp_table_name STRING;

    DECLARE pseudonymize_fields_name_arr ARRAY<STRING>; 
    DECLARE events_name_arr ARRAY<STRING>; 

    DECLARE load_data_temp_query ARRAY<STRING>; 


    -- Variables used in loops
    DECLARE i INT64;
    DECLARE temp_str STRING;

    SET pseudonymize_fields_name_arr = [
      {gdpr_columns}
    ];

    SET events_name_arr = [
      {events_names}
    ];

    SET temp_table_name = CONCAT('event_clean_data_atomic_', app_id, "_", execution_date);
  

    SET i = 0;
    WHILE i < ARRAY_LENGTH(events_name_arr) DO
      
      IF events_name_arr[OFFSET(i)] NOT LIKE 'ctx_%' THEN
        SET temp_str = CONCAT("",
        "  SELECT unique_id, collector_tstamp, ", ARRAY_TO_STRING(pseudonymize_fields_name_arr, ','),
        "  FROM `", project_id, ".", app_id, "_{source_dataset_suffix}.", events_name_arr[OFFSET(i)] , "`",
        "  WHERE date_ = '", execution_date, "'");

        SET load_data_temp_query = ARRAY(
          SELECT * FROM UNNEST(load_data_temp_query) 
          UNION ALL 
          SELECT temp_str);
      END IF;  
      SET i = i + 1;
    END WHILE;

    EXECUTE IMMEDIATE CONCAT("",
    " CREATE OR REPLACE TEMPORARY TABLE ", temp_table_name, " AS ",
     ARRAY_TO_STRING(load_data_temp_query, ' UNION ALL'), ";");  
  
----------------------------------------------------------------------------------------------
    -- Pseudonymization
    -- In this part is programming code used for pseudonymization
    -- Process of pseunymization pseudonymize atomic columns. 

    SET i = 0;
    WHILE i < ARRAY_LENGTH(pseudonymize_fields_name_arr) DO
        
        EXECUTE IMMEDIATE CONCAT("",
        "   CREATE TABLE IF NOT EXISTS `", project_id, ".", app_id, "_gdpr.atomic` (",
        "     unique_id STRING,",
        "     field_name STRING,",
        "     field_original_value STRING,",
        "     field_modified_value STRING,",
        "     inserted_at TIMESTAMP,",
        "     updated_at TIMESTAMP,",
        "     deleted_at TIMESTAMP);");

        EXECUTE IMMEDIATE CONCAT(
            "MERGE `", project_id, ".", app_id, "_gdpr.atomic` T ",
            "USING (SELECT unique_id, ", pseudonymize_fields_name_arr[OFFSET(i)], ", MAX(collector_tstamp) AS updated_at, MIN(collector_tstamp) AS inserted_at FROM ", temp_table_name, " WHERE ", pseudonymize_fields_name_arr[OFFSET(i)], " IS NOT NULL GROUP BY 1, 2 ) S ",
            "ON T.unique_id = S.unique_id AND T.field_name = '", pseudonymize_fields_name_arr[OFFSET(i)] ,"' AND T.field_original_value = S.", pseudonymize_fields_name_arr[OFFSET(i)], " ",
            "WHEN NOT MATCHED THEN ",
            "INSERT(unique_id, field_name, field_original_value, field_modified_value, inserted_at, updated_at) ",
            "VALUES(S.unique_id, '", pseudonymize_fields_name_arr[OFFSET(i)], "', S.", pseudonymize_fields_name_arr[OFFSET(i)], ", TO_BASE64(SHA256(CAST(S.", pseudonymize_fields_name_arr[OFFSET(i)], " AS STRING))), S.inserted_at, S.updated_at) ",
            "WHEN MATCHED THEN ",
            "UPDATE SET updated_at = S.updated_at"
          );   

      SET i = i + 1;
    END WHILE;




