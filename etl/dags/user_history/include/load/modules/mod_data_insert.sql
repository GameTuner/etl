
    EXECUTE IMMEDIATE CONCAT("INSERT INTO `", project_id, ".", app_id, "_raw.", event_name, "` ",
    "(", ARRAY_TO_STRING(event_columns, ','), ") ",
    " SELECT ", ARRAY_TO_STRING(event_columns, ','), " FROM ", temp_table_name,
    " WHERE event_id NOT IN (SELECT event_id FROM `",
    project_id, ".", app_id, "_raw.", event_name, "`", " WHERE date_ = '", execution_date, "');"
    );
