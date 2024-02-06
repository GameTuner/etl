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

from typing import List


import pytz
from airflow.exceptions import AirflowFailException, AirflowSensorTimeout

from lib import environment
from datetime import datetime, date

from slack_sdk.webhook import WebhookClient

def get_string_array_for_query(array: List[str]):
    ret_array = ','.join(["\"{}\"".format(elem) for elem in array])
    if len(ret_array) == 0:
        return ""
    return ret_array


def build_event_query():
    base_query = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/load/load_data.sql', 'r').read()

    mod_declare = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/load/modules/mod_load_data_declare.sql', 'r').read()
    mod_get_data = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/load/modules/mod_load_data_get_data.sql', 'r').read()
    mod_gdpr = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/load/modules/mod_load_data_gdpr.sql', 'r').read()
    mod_insert = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/load/modules/mod_data_insert.sql', 'r').read()

    base_query = base_query.format(
        mod_declare=mod_declare,
        mod_get_data=mod_get_data,
        mod_gdpr=mod_gdpr,
        mod_insert=mod_insert
    )

    return base_query


def get_sql_query(query: str, **kwargs):
    return query.format(**kwargs)


def get_sql_query_from_file(query_file: str, **kwargs):
    return open(query_file, 'r').read().format(**kwargs)


def get_local_date_from_utc_datetime(dt: datetime, timezone: str) -> date:
    timezone = pytz.timezone(timezone)
    local_execution_date = dt.astimezone(timezone)
    return local_execution_date.date()


def task_failed_slack_alert(context):
    if environment.slack_webhook_url() is None:
        return

    webhook = WebhookClient(environment.slack_webhook_url())
    slack_msg = """
:red_circle: Task Failed.
*Task*: {task}  
*Dag*: {dag} 
*Execution Date*: {exec_date}
*Project*: {project}
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        project=environment.gcp_project_id()
    )
    webhook.send(text=slack_msg)


def task_failed_detailed_slack_alert(context):
    if environment.slack_webhook_url() is None:
        return

    webhook = WebhookClient(environment.slack_webhook_url())
    slack_msg = """
:red_circle: Task Failed.
*Task*: <https://{composer_environment_url}/log?dag_id={dag}&task_id={task}&execution_date={exec_date}|{task}>  
*Dag*: {dag} 
*Execution Date*: {exec_date}
*Project*: {project}
*Error*: {exception_type} 
```{exception}```
            """.format(
        composer_environment_url=environment.composer_environment_url(),
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        project=environment.gcp_project_id(),
        exception=context.get('exception'),
        exception_type=type(context.get('exception')).__name__,
    )
    webhook.send(text=slack_msg)


def test_failed_detailed_slack_alert(context):
    if environment.slack_webhook_url() is None:
        return
    
    if type(context.get('exception')) == AirflowFailException:
        webhook = WebhookClient(environment.slack_webhook_url())
        header = ':warning: Optional Test Failed.' if '.optional_' in context.get('task_instance').task_id else ':no_entry: Mandatory Test Failed.'
        slack_msg = """
    {header}
    *Test*: <https://{composer_environment_url}/log?dag_id={dag}&task_id={task}&execution_date={exec_date}|{task}>  
    *Dag*: {dag} 
    *Execution Date*: {exec_date}
    *Project*: {project}
    Failed tests: 
    ```{exception}```
                """.format(
            composer_environment_url=environment.composer_environment_url(),
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            project=environment.gcp_project_id(),
            exception=context.get('exception'),
            header=header,
        )
        webhook.send(text=slack_msg)
    else:
        task_failed_detailed_slack_alert(context)


def deadline_exceeded_slack_alert(context):
    if environment.slack_webhook_url() is None:
        return

    if type(context.get('exception')) == AirflowSensorTimeout:
        webhook = WebhookClient(environment.slack_webhook_url())
        slack_msg = """
    :hourglass: DAG deadline exceeded.
    *Dag*: {dag} 
    *Sensor*: <https://{composer_environment_url}/log?dag_id={dag}&task_id={task}&execution_date={exec_date}|{task}>  
    *Execution Date*: {exec_date}
    *Project*: {project}
    """.format(
            composer_environment_url=environment.composer_environment_url(),
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            project=environment.gcp_project_id(),
        )
        webhook.send(text=slack_msg)
    else:
        task_failed_detailed_slack_alert(context)