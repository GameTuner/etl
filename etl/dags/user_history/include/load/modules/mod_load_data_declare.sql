 
    DECLARE execution_date DATE DEFAULT DATE("{execution_date}");
    DECLARE event_name STRING DEFAULT '{event_name}';
    DECLARE app_id STRING DEFAULT '{app_id}';
    DECLARE days_for_deduplication INT64 DEFAULT {days_for_deduplication};
    DECLARE project_id STRING DEFAULT '{project_id}';
    DECLARE temp_table_name_prefix STRING DEFAULT '{dag_id}';
    DECLARE temp_table_name STRING;
    DECLARE source_dataset_suffix STRING DEFAULT '{source_dataset_suffix}';
    DECLARE event_columns ARRAY<STRING>;

    DECLARE pseudonymize_fields_name_arr ARRAY<STRING>; 
    DECLARE pseudonymize_atomic_fields_name_arr ARRAY<STRING>;    

    -- Variables used in loops
    DECLARE i INT64;

    -- Variables used to build commands
    DECLARE pseudonymize_command STRING;
    DECLARE insert_command STRING;

    SET pseudonymize_fields_name_arr = [
      {gdpr_columns}
    ];

    SET pseudonymize_atomic_fields_name_arr = [
      {atomic_gdpr_columns}
    ];

    IF (
      (SELECT COUNT(*) FROM `{project_id}.{app_id}_raw.{event_name}` WHERE date_ = "{execution_date}") > 0
      AND
      (SELECT COUNT(*) FROM `{project_id}.{app_id}_load.{event_name}` WHERE date_ = "{execution_date}") = 0
     ) THEN
      RETURN;
    END IF;