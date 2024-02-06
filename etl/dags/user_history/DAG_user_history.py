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

from airflow import DAG
import datetime

from airflow.sensors.python import PythonSensor

from lib import environment, datasource

from lib.app_config import app_config
from operators.bigquery import BigQueryExecuteLazyQueryOperator
from operators.deadline import DeadlineSensor
from user_history.event_partition import wait_for_event_partition
from lib.utils import build_event_query, get_sql_query_from_file
from airflow.operators.dummy_operator import DummyOperator

from lib.utils import task_failed_slack_alert
from lib.test_framework import get_test_task
from user_history.user_history.dag import create_event_transformations_task_group, create_fact_tables_task_group, \
    create_user_history_task_group

app_configs = app_config.load_from_json_file_if_exists(f"{environment.DATA_DIR}/apps_config.json")


def create_dag(app_id: str, dag_id: str):
    app_id_config = app_configs.app_id_configs[app_id]
    default_args = {
        'owner': 'airflow',
        'start_date': app_id_config.created_midnight(),
        'retries': 5,
        'retry_delay': datetime.timedelta(minutes=1),
        'depends_on_past': True,
        'on_failure_callback': task_failed_slack_alert,
    }

    with DAG(dag_id, schedule_interval='@daily', default_args=default_args, concurrency=10, max_active_runs=1, catchup=True) as dag:
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')

        events_transformations = create_event_transformations_task_group(dag, app_id, app_configs,
                                                                         build_event_query(), 'load')
        fact_tables = create_fact_tables_task_group(dag, app_id)
        
        user_history = create_user_history_task_group(dag, app_id, app_configs)

        sensor_test = get_test_task("load_sensor_test",
                                    app_id,
                                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/load_sensor/mandatory',
                                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/load_sensor/optional',
                                    source_dataset_suffix='load')

        validations_test = get_test_task("validation_test", 
                                        app_id,
                                        f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/unique_id_validation/mandatory',
                                        f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/unique_id_validation/optional',
                                        source_dataset_suffix='load')
        
        events_test = get_test_task("event_transformation_test",
                                    app_id,
                                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/event_transformations/mandatory',
                                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/event_transformations/optional',
                                    source_dataset_suffix='load')

        fact_tables_test = get_test_task("fact_tables_test",
                                         app_id,
                                         f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/fact_tables/mandatory',
                                         f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/fact_tables/optional')

        user_history_test = get_test_task("user_history_test",
                                          app_id,
                                          f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/user_history/mandatory',
                                          f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/tests/user_history/optional')

        sensor = PythonSensor(
            task_id="wait_for_event_partition",
            poke_interval=5 * 60,
            mode="reschedule",
            python_callable=lambda execution_date, **kwargs: wait_for_event_partition(
                app_id, execution_date, app_configs.app_id_configs[app_id].timezone,
                app_configs.common_configs.close_event_partition_after_hours),
        )

        deadline_sensor = DeadlineSensor(
            task_id="deadline_sensor",
            task_id_to_wait_for=end.task_id,
            deadline_minutes=8 * 60,
            poke_interval=5 * 60,
            mode="reschedule",
            default_args={'depends_on_past': False}
        )

        start >> deadline_sensor

        update_datasource_freshness_task = PythonOperator(
            task_id='update_datasource_freshness',
            python_callable=lambda execution_date: datasource.update_datasource_freshness(app_id, 'user_history', execution_date.date())
        )

        with TaskGroup("gdpr_delete_data", dag=dag) as gdpr_delete_data:
            BigQueryExecuteLazyQueryOperator(
                task_id="gdpr_delete_data_on_request",
                location=environment.bigquery_datasets_location(),
                gcp_conn_id='google_cloud_default',
                app_id=app_id,
                query_generator=lambda execution_date: get_sql_query_from_file(
                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/gdpr/gdpr_delete_request.sql',
                    project_id=environment.gcp_project_id(),
                    app_id=app_id,
                    execution_date=str(execution_date)),
            )

            BigQueryExecuteLazyQueryOperator(
                task_id="gdpr_delete_data_inactive_users",
                location=environment.bigquery_datasets_location(),
                gcp_conn_id='google_cloud_default',
                app_id=app_id,
                query_generator=lambda execution_date: get_sql_query_from_file(
                    f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/gdpr/gdpr_delete_inactive.sql',
                    project_id=environment.gcp_project_id(),
                    app_id=app_id,
                    execution_date=str(execution_date)),
            )

        start >> \
        sensor >> sensor_test >> \
        validations_test >> \
        events_transformations >> events_test >> \
        fact_tables >> fact_tables_test >> \
        user_history >> user_history_test >> update_datasource_freshness_task >> \
        end

        events_test >> gdpr_delete_data >> end
        return dag


if app_configs is not None:
    for app_id in app_configs.app_id_configs.keys():
        dag_id = f'DAG_{app_id}_user_history'
        globals()[dag_id] = create_dag(app_id, dag_id)
