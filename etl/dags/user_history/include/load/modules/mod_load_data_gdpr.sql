----------------------------------------------------------------------------------------------
    -- Pseudonymization
    -- In this part is programming code used for pseudonymization
    -- Process of pseunymization pseudonymize columns in events_clean_temp temporary table, 
    -- because during process of data insertion in extracted tables data are inserted from events_clean_temp table.

    -- Processes of pseudonymization are differ for different types of columns, and in following WHILE loop is passed one time per different types
    --SET pseudonymize_types_arr = (SELECT ARRAY(SELECT DISTINCT type FROM pseudonymize_columns_temp));

    SET i = 0;
    WHILE i < ARRAY_LENGTH(pseudonymize_fields_name_arr) DO
        
      SET pseudonymize_command = CONCAT("UPDATE ", temp_table_name," SET ");

      IF NOT STARTS_WITH(pseudonymize_fields_name_arr[OFFSET(i)], 'ctx_')
      THEN
        EXECUTE IMMEDIATE CONCAT("",
        "   CREATE TABLE IF NOT EXISTS `", project_id, ".", app_id, "_gdpr.", event_name, "` (",
        "     unique_id STRING,",
        "     field_name STRING,",
        "     field_original_value STRING,",
        "     field_modified_value STRING,",
        "     inserted_at TIMESTAMP,",
        "     updated_at TIMESTAMP,",
        "     deleted_at TIMESTAMP);");

        EXECUTE IMMEDIATE CONCAT(
            "MERGE `", project_id, ".", app_id, "_gdpr.", event_name, "` T ",
            "USING (SELECT unique_id, ", pseudonymize_fields_name_arr[OFFSET(i)], " AS ", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.',''), ", MAX(collector_tstamp) AS updated_at, MIN(collector_tstamp) AS inserted_at FROM ", temp_table_name, " WHERE ", pseudonymize_fields_name_arr[OFFSET(i)], " IS NOT NULL GROUP BY 1, 2 ) S ",
            "ON T.unique_id = S.unique_id AND T.field_name = '", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.','') ,"' AND T.field_original_value = S.", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.',''), " ",
            "WHEN NOT MATCHED THEN ",
            "INSERT(unique_id, field_name, field_original_value, field_modified_value, inserted_at, updated_at) ",
            "VALUES(S.unique_id, '", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.',''), "', S.", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.',''), ", TO_BASE64(SHA256(CAST(S.", REPLACE(pseudonymize_fields_name_arr[OFFSET(i)], 'params.',''), " AS STRING))), S.inserted_at, S.updated_at) ",
            "WHEN MATCHED THEN ",
            "UPDATE SET updated_at = S.updated_at"
          );
        END IF;

      SET pseudonymize_command = CONCAT(pseudonymize_command, pseudonymize_fields_name_arr[OFFSET(i)], " = TO_BASE64(SHA256(CAST(", pseudonymize_fields_name_arr[OFFSET(i)], " AS STRING))) WHERE TRUE;" );

      EXECUTE IMMEDIATE pseudonymize_command;        

      SET i = i + 1;
    END WHILE;

  --Pseudonymize atomic fields and update in temp table
  --Exception is made for context tables
    SET i = 0;
    WHILE i < ARRAY_LENGTH(pseudonymize_atomic_fields_name_arr) DO
      SET pseudonymize_command = CONCAT("UPDATE ", temp_table_name, " SET ");

      SET pseudonymize_command = CONCAT(pseudonymize_command, pseudonymize_atomic_fields_name_arr[OFFSET(i)], " = TO_BASE64(SHA256(CAST(", pseudonymize_atomic_fields_name_arr[OFFSET(i)], " AS STRING))) WHERE TRUE;" );

      EXECUTE IMMEDIATE pseudonymize_command;        

      SET i = i + 1;
    END WHILE;