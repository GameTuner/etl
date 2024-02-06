# Copyright (c) 2024 AlgebraAI All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from operators.bigquery import BigQueryExecuteLazyQueryOperator

from lib import environment
from lib.utils import get_sql_query_from_file


def get_apps_config():
    r = requests.get(f'http://{environment.metadata_ip_address()}/api/v1/apps-detailed')
    r.raise_for_status()
    return r.json()

def serialize_config():
    app_configs_json = get_apps_config()
    logging.info(f'Downloaded config: {app_configs_json}')
    with open(f'{environment.DATA_DIR}/apps_config.json', "w") as f:
        json.dump(app_configs_json, f)


with DAG(
        'DAG_fetch_app_config',
        start_date=datetime(2022, 1, 1),
        schedule_interval='*/10 * * * *',
        max_active_runs=1,
        catchup=False) as dag:

    serialize_config_task = PythonOperator(
        task_id='serialize_config',
        python_callable=serialize_config)
    
    with TaskGroup("maintain_common_tables", tooltip="Maintain common tables and create on first AirFlow run") as maintain_common_tables:
        create_country_vat_table = BigQueryExecuteLazyQueryOperator(
            dag=dag,
            task_id="create_country_vat_table",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id='all',
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/vat/create_country_vat_table.sql',
                project_id=environment.gcp_project_id()
            ),
        )

        create_country_map_table = BigQueryExecuteLazyQueryOperator(
            dag=dag,
            task_id="create_country_map_table",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id='all',
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/create_coutry_map.sql',
                project_id=environment.gcp_project_id()
            ),
        )

        create_currency_rate_table = BigQueryExecuteLazyQueryOperator(
                dag=dag,
                task_id="create_currency_rate_table",
                location=environment.bigquery_datasets_location(),
                gcp_conn_id='google_cloud_default',
                app_id='all',
                query_generator=lambda execution_date: get_sql_query_from_file(
                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/currency/create_currency_rate_table.sql',
                    project_id=environment.gcp_project_id()
                ),
            )

        create_country_vat_table
        create_country_map_table
        create_currency_rate_table

    serialize_config_task >> maintain_common_tables


