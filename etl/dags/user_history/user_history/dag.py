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

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from lib import environment
from lib.app_config import materialized_fields_config
from lib.app_config.app_config import AppConfig, MetadataAppsConfig
from lib.utils import get_sql_query_from_file, get_sql_query, get_string_array_for_query
from operators.bigquery import BigQueryExecuteLazyQueryOperator
from user_history.user_history import user_history_parser

def create_event_transformations_task_group(dag, app_id, app_configs: MetadataAppsConfig, event_load_query, source_dataset_suffix):
    with TaskGroup("events_transformations", dag=dag, tooltip="Events transformation") as events_transformations:
        # we must use event_name outside of loop, otherwise all functions point to last value after loop completes
        event_query_generator_lambda = lambda event_name: lambda execution_date: get_sql_query(
            event_load_query,
            project_id=environment.gcp_project_id(),
            dag_id=dag.dag_id,
            days_for_deduplication=1,
            event_name=event_name,
            app_id=app_id,
            source_dataset_suffix=source_dataset_suffix,
            execution_date=str(execution_date),
            gdpr_columns=get_string_array_for_query(app_configs.event_gdpr_fields(app_id, event_name)),
            atomic_gdpr_columns=get_string_array_for_query(
                app_configs.common_configs.gdpr_atomic_parameters))

        for event_name in app_configs.all_event_names(app_id):
            BigQueryExecuteLazyQueryOperator(
                task_id="load_event_{}".format(event_name),
                location=environment.bigquery_datasets_location(),
                gcp_conn_id='google_cloud_default',
                app_id=app_id,
                query_generator=event_query_generator_lambda(event_name)
            )

        # we must use event_name outside of loop, otherwise all functions point to last value after loop completes
        context_query_generator_lambda = lambda context_name: lambda execution_date: get_sql_query(
            event_load_query,
            project_id=environment.gcp_project_id(),
            dag_id=dag.dag_id,
            days_for_deduplication=1,
            event_name=context_name,
            app_id=app_id,
            source_dataset_suffix=source_dataset_suffix,
            execution_date=str(execution_date),
            gdpr_columns=get_string_array_for_query(app_configs.context_gdpr_fields(app_id, context_name)),
            atomic_gdpr_columns=get_string_array_for_query(
                app_configs.common_configs.gdpr_atomic_parameters))

        for context_name in app_configs.common_configs.non_embedded_context_schemas.keys():
            BigQueryExecuteLazyQueryOperator(
                task_id="load_event_{}".format(context_name),
                location=environment.bigquery_datasets_location(),
                gcp_conn_id='google_cloud_default',
                app_id=app_id,
                query_generator=context_query_generator_lambda(context_name),
            )

        BigQueryExecuteLazyQueryOperator(
            task_id="gdpr_atomic_fields",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id=app_id,
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/gdpr/gdpr_atomic_fields.sql',
                project_id=environment.gcp_project_id(),
                app_id=app_id,
                source_dataset_suffix=source_dataset_suffix,
                execution_date=str(execution_date),
                gdpr_columns=get_string_array_for_query(
                    app_configs.common_configs.gdpr_atomic_parameters),
                events_names=get_string_array_for_query(app_configs.all_event_names(app_id))))
        return events_transformations

def create_fact_tables_task_group(dag, app_id: str):
    with TaskGroup("fact_tables", dag=dag, tooltip="Load data to fact tables") as fact_tables:

        BigQueryExecuteLazyQueryOperator(
            task_id="insert_registration",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id=app_id,
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/fact/insert_registration.sql',
                project_id=environment.gcp_project_id(),
                app_id=app_id,
                execution_date=str(execution_date)),
        )

        BigQueryExecuteLazyQueryOperator(
            task_id="insert_sessions",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id=app_id,
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/fact/insert_sessions.sql',
                project_id=environment.gcp_project_id(),
                app_id=app_id,
                execution_date=str(execution_date)),
        )
        return fact_tables

def create_user_history_task_group(dag, app_id: str, app_configs: MetadataAppsConfig):
    with TaskGroup("user_history", tooltip="Insert data in user history and it's derivates") as user_history:

        user_history_daily = BigQueryExecuteLazyQueryOperator(
            dag=dag,
            task_id="insert_user_history",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id=app_id,
            query_generator=lambda execution_date: user_history_parser.get_user_history_sql(
                environment.gcp_project_id(), app_id, str(execution_date), app_configs.get_semantic_layer_data(app_id, 'user_history')
                )
        )

        user_history_derivates = BigQueryExecuteLazyQueryOperator(
            dag=dag,
            task_id="insert_user_history_derivates",
            location=environment.bigquery_datasets_location(),
            gcp_conn_id='google_cloud_default',
            app_id=app_id,
            query_generator=lambda execution_date: get_sql_query_from_file(
                f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/user_history/insert_user_history_derivates.sql',
                project_id=environment.gcp_project_id(),
                app_id=app_id,
                execution_date=str(execution_date)),
        )
        user_history_daily >> user_history_derivates
        return user_history