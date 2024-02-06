# GameTuner ETL service

## Overview

This is a ETL service for GameTuner project. It is used to extract data from different sources, transform it and load it into the database. It is implemented using Apache Airflow and project structure is adopted to be deployd on Google Cloud Composer service.

## Installation

### Dependencies

GameTuner ETL service is dependent on [GameTuner Metadata][gametuner-metadata] service. MetaData service is used for fetching configurations of applications in order to set DAGs and tasks. If MetaData service is not available, ETL service will continue to work but it will not have any DAG configured.

### Configuration

All configuration is done over GameTuner Terraform project. Project uses enviroment variables to configure itself.

## Usage

There are 4 main DAGs of ETL service:

- [`DAG_fetch_app_config`][dag-fetch-app-config] - Fetches application configurations from MetaData service and stores it locally. Since Airflow is deployed using Google Cloud Composer, fetched app configurations are stored in Google Cloud Storage. This DAG is scheduled to run every 10 minutes.

- ['DAG_currency_extractions_from_API'][dag-currency] - Extracts currency exchange rates from [API](https://currencyapi.com) and stores it in database. This DAG is scheduled to run once a day. If currency API key is not provided, DAG will not run.

- ['DAG_update_geoip_db'][dag-geoip] - Updates GeoIP database. This DAG is scheduled to run every wennesday at 08:00. If GeoIP API key is not provided, DAG will not run.

- ['DAG_user_history'][dag-user-history] - This is the main DAG of ETL service. There is couple responsibilities of this DAG:
    - Extracts raw user level data from `_load` dataset in BigQuery.
    - Transform events data and load it into `_raw` dataset in BigQuery. During this process data is partitioned by date, deduplicated, enriched with additional data, pseudonymized and anonymized.
    - Extracts data from `_raw` dataset, transforms it and loads it into `_main` dataset in BigQuery. During this process data is aggregated. 
    - Creates user history table in BigQuery.
    - Runs tests between different stages of ETL process.

    

## Testing

## Deploy

To deploy ETL service on Google Cloud Composer, you need to have Google Cloud SDK installed. After that, you can deploy it using following command:

```bash
gcloud builds submit --config=cloudbuild.yaml .
```

## Licence

The GameTuner ETL service is copyright 2022-2024 AlgebraAI.

GameTuner ETL service is released under the [Apache 2.0 License][license].

[gametuner-metadata]:https://github.com/GameTuner/metadata.git
[license]: https://www.apache.org/licenses/LICENSE-2.0
[dag-fetch-app-config]:etl/dags/DAG_fetch_app_config.py
[dag-currency]:etl/dags/DAG_get_currencies_data.py
[dag-geoip]:etl/dags/DAG_update_geoip_db.py
[dag-user-history]:etl/dags/user_history/DAG_user_history.py
[license]: https://www.apache.org/licenses/LICENSE-2.0