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

from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from lib.test_framework import get_test_task
from operators.bigquery import BigQueryExecuteLazyQueryOperator

from lib.utils import task_failed_slack_alert
from external_sources.apps_flyer import apps_flyer
from external_sources.stores.itunes import store_itunes
from external_sources.stores.google_play import store_google_play
from lib import environment, datasource
from lib.app_config import app_config
from lib.app_config.app_config import AppConfig
from lib.utils import get_sql_query_from_file

app_configs = app_config.load_from_json_file_if_exists(f"{environment.DATA_DIR}/apps_config.json")

def create_dag(dag_id: str, app_id: str, app_config: AppConfig):
    schedule = '30 5 * * *'
    default_args = {
        'owner': 'airflow',
        'start_date': app_config.created_midnight_utc(),
        'retries': 5,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': task_failed_slack_alert,
    }

    with DAG(dag_id,
             schedule_interval=schedule,
             default_args=default_args,
             max_active_runs=1) as dag:

        start = DummyOperator(task_id='start')

        end = DummyOperator(task_id='end')

        with TaskGroup("external_data_sources", tooltip="Ingest data from external sources") as external_data_sources:

            with TaskGroup("appsflyer_data", tooltip="Ingest appsflyer data") as appsflyer_data:
                af_data_loker_source = app_config.external_services.apps_flyer
                af_cost_etl_source = app_config.external_services.apps_flyer_cost_etl
            
                if af_data_loker_source is not None and af_cost_etl_source is not None:
                    af_data_locker_task = apps_flyer.get_af_operator(
                        app_id,
                        environment.appsflyer_dataproc_execution_project_id(),
                        environment.appsflyer_dataproc_execution_region(),
                        af_data_loker_source.reports,
                        '{{ yesterday_ds }}',
                        '{{ ds }}',
                        af_data_loker_source.app_ids,
                        environment.appsflyer_bigquery_project_id(),
                        environment.appsflyer_data_locker_bucket_name() if af_data_loker_source.external_bucket_name is None else af_data_loker_source.external_bucket_name,
                        af_data_loker_source.home_folder,
                        environment.appsflyer_dataproc_bucket_name()
                    )

                    data_loker_sensor = PythonSensor(
                        task_id="wait_for_data_locker_bucket_data",
                        poke_interval=900,
                        timeout=18000,
                        mode="reschedule",
                        python_callable=lambda execution_date, **kwargs: apps_flyer.wait_af_data_locker_bucket_data(
                            execution_date,
                            environment.appsflyer_data_locker_bucket_name() if af_data_loker_source.external_bucket_name is None else af_data_loker_source.external_bucket_name,
                            af_data_loker_source.home_folder,
                            af_data_loker_source.reports),
                    )

                    data_loker_sensor >> af_data_locker_task

                    af_cost_etl_task = apps_flyer.get_af_cost_etl_operator(
                        app_id,
                        environment.appsflyer_dataproc_execution_project_id(),
                        environment.appsflyer_dataproc_execution_region(),
                        af_cost_etl_source.reports,
                        '{{ yesterday_ds }}',
                        '{{ next_ds }}',
                        [af_cost_etl_source.android_app_id,af_cost_etl_source.ios_app_id],
                        environment.appsflyer_bigquery_project_id(),
                        af_cost_etl_source.bucket_name,
                        environment.appsflyer_dataproc_bucket_name()
                    )

                    elt_cost_sensor = PythonSensor(
                        task_id="wait_for_cost_etl_bucket_data",
                        poke_interval=900,
                        timeout=18000,
                        mode="reschedule",
                        python_callable=lambda execution_date, **kwargs: apps_flyer.wait_af_cost_etl_bucket_data(
                            execution_date,
                            af_cost_etl_source.bucket_name,
                            af_cost_etl_source.reports),
                    )

                    elt_cost_sensor >> af_cost_etl_task

                    with TaskGroup("appsflyer_aggregated", tooltip="Calculate aggregated appsflyer data") as appsflyer_aggregated:

                        appsflyer_user_map = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="update_appsflyer_user_map",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/update_user_map.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date))
                        )

                        appsflyer_user_history_2_days_ago = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_user_history_2_days_ago",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date - timedelta(days=2)),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_user_history_1_day_ago = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_user_history_1_day_ago",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date - timedelta(days=1)),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_user_history = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_user_history",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_aggregated_2_days_ago = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_aggregated_2_days_ago",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_aggregated_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date - timedelta(days=2)),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_aggregated_1_day_ago = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_aggregated_1_day_ago",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_aggregated_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date - timedelta(days=1)),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_aggregated_history_data = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="load_appsflyer_aggregated_data",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/appsflyer/insert_appsflyer_aggregated_user_history.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id,
                                execution_date=str(execution_date),
                                android_app_id=af_cost_etl_source.android_app_id,
                                ios_app_id=af_cost_etl_source.ios_app_id),
                        )

                        appsflyer_user_map >> \
                        appsflyer_user_history_2_days_ago >> appsflyer_user_history_1_day_ago >> appsflyer_user_history >> \
                        appsflyer_aggregated_2_days_ago >> appsflyer_aggregated_1_day_ago >> appsflyer_aggregated_history_data

                    [af_data_locker_task, af_cost_etl_task] >> appsflyer_aggregated
                    
                    update_appsflyer_datasource_freshness_task = PythonOperator(
                        task_id='update_appsflyer_datasource_freshness',
                        python_callable=lambda execution_date: datasource.update_datasource_freshness(app_id, 'appsflyer', execution_date.date())
                    )

                    appsflyer_aggregated >> update_appsflyer_datasource_freshness_task
                    
            with TaskGroup("stores_data", tooltip="Ingest data from stores") as stores_data:

                store_itunes_sales_report_source = app_config.external_services.store_itunes_sales_report
                store_google_play_sales_report_source = app_config.external_services.store_gogole_play_sales_report

                if store_itunes_sales_report_source is not None:
                    
                    store_itunes_data = PythonSensor(
                        task_id="wait_for_store_itunes_data",
                        poke_interval=900,
                        timeout=28800,
                        mode="reschedule",
                        python_callable=lambda execution_date, **kwargs: store_itunes.get_itunes_sales_sensor_task(
                            store_itunes_sales_report_source,
                            execution_date),
                    )
                    
                    store_itunes_create_tables = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="store_itunes_create_tables",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/stores/itunes/store_itunes_create_tables.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id),
                        )
                    
                    store_itunes_download_sales_report = PythonOperator(
                        task_id='store_itunes_download_sales_report',
                        python_callable=lambda execution_date: store_itunes.get_itunes_sales_report_task(
                            environment.gcp_project_id(),
                            app_id,
                            store_itunes_sales_report_source,
                            execution_date
                        )
                    )

                    store_itunes_process_data = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="store_itunes_process_data",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/stores/itunes/store_itunes_process_data.sql',
                                execution_date=str(execution_date),
                                project_id=environment.gcp_project_id(),
                                app_id=app_id),
                        )
                    
                    update_stores_datasource_freshness_task = PythonOperator(
                        task_id='update_stores_datasource_freshness',
                        python_callable=lambda execution_date: datasource.update_datasource_freshness(app_id, 'stores', execution_date.date())
                    )

                    store_itunes_data  >> store_itunes_create_tables >> store_itunes_download_sales_report >> \
                    store_itunes_process_data >> update_stores_datasource_freshness_task        

                if store_google_play_sales_report_source is not None:
                                        
                    store_google_play_create_tables = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="store_google_play_create_tables",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/stores/google_play/store_google_play_create_tables.sql',
                                project_id=environment.gcp_project_id(),
                                app_id=app_id),
                        )
                    
                    store_google_play_download_sales_report = PythonOperator(
                        task_id='store_google_play_download_sales_report',
                        python_callable=lambda execution_date: store_google_play.get_google_play_sales_report_task(
                            environment.gcp_project_id(),
                            app_id,
                            store_google_play_sales_report_source,
                            execution_date
                        )
                    )

                    store_google_play_process_data = BigQueryExecuteLazyQueryOperator(
                            dag=dag,
                            task_id="store_google_play_process_data",
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            app_id=app_id,
                            query_generator=lambda execution_date: get_sql_query_from_file(
                                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/stores/google_play/store_google_play_process_data.sql',
                                project_id=environment.gcp_project_id(),
                                execution_date=str(execution_date),
                                app_id=app_id),
                        )

                    update_stores_google_datasource_freshness_task = PythonOperator(
                        task_id='update_stores_google_datasource_freshness',
                        python_callable=lambda execution_date: datasource.update_datasource_freshness(app_id, 'stores', execution_date.date())
                    )

                    store_google_play_create_tables >> store_google_play_download_sales_report >> \
                    store_google_play_process_data >> update_stores_google_datasource_freshness_task           

            appsflyer_data
            stores_data
            
        start >> external_data_sources >> end

    return dag

if app_configs is not None:
    for app_id, app_config in app_configs.app_id_configs.items():
        dag_id = 'DAG_{}_external_source'.format(app_id)
        if app_config.external_services.has_external_services():
            globals()[dag_id] = create_dag(dag_id, app_id, app_config)

