
/*
    
    Procedure description:
        Procedure for importing data from load to raw dataset. Script is used for every event separately.
        Procedure include pseudonymization of pii fields, deduplication of data and closing data patition.
        Next steps are provided:
            - Load data from load tables to temp table.
            - Deduplicate data by event_fingerprint and event_id.
            - Calculate event_tstamp and event_quality (Mark late/future events).
            - Pseudonymize pii fields.
    
    Procedure dependencies:
        - _gdpr dataset.
        - _load dataset.
        - _load.ctx_event_context table.
        - _load.event_name table.
        - _raw.event_name table.

    Procedure arguments:
        - DECLARE start_timestamp TIMESTAMP DEFAULT TIMESTAMP(start_timestamp);
        - DECLARE end_timestamp TIMESTAMP DEFAULT TIMESTAMP(end_timestamp);
        - DECLARE event_name STRING DEFAULT 'event_name';
        - DECLARE app_id STRING DEFAULT 'app_id';
        - DECLARE days_for_deduplication INT64 DEFAULT days_for_deduplication;
        - DECLARE project_id STRING DEFAULT 'project_id';
*/


    {mod_declare}

    {mod_get_data}

    {mod_gdpr}    

    {mod_insert}





