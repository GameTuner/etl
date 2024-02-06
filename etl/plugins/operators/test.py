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

import logging

import yaml
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from lib.utils import test_failed_detailed_slack_alert
from operators.bigquery import get_local_date_from_utc_datetime


class ExecuteTestOperator(BaseOperator):

    def __init__(self, gcp_conn_id: str, gcp_project_id: str, app_id: str, unique_id_bq_connection: str, yaml_test_path: str,
                 location: str, additional_params,  *args, **kwargs):
        super().__init__(on_failure_callback=test_failed_detailed_slack_alert, *args, **kwargs)
        self.gcp_project_id = gcp_project_id
        self.app_id = app_id
        self.unique_id_bq_connection = unique_id_bq_connection
        self.yaml_test_path = yaml_test_path
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.additional_params = additional_params

    def execute(self, context):
        execution_date = get_local_date_from_utc_datetime(context['dag_run'].execution_date, context["dag"].timezone.name)
        labels = {'app_id': self.app_id, 'enviroment': 'production', 'service': 'etl'}

        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location, use_legacy_sql=False, labels=labels)

        self.additional_params['execution_date'] = execution_date
        self.additional_params['app_id'] = self.app_id
        self.additional_params['project'] = self.gcp_project_id
        self.additional_params['unique_id_bq_connection'] = self.unique_id_bq_connection

        failed_tests = []
        with open(self.yaml_test_path, 'r') as f:
            queries = yaml.safe_load(f)

            for query in queries:
                query_name = query['name'].format(**self.additional_params)
                query_sql = query['query'].format(**self.additional_params)

                result = bq_hook.get_first(query_sql)
                if result:
                    logging.info(f'{query_name} failed! Got result {result} for query {query_sql}')
                    failed_tests.append(f"Expected: {query_name}. Got: {result}")

        if failed_tests:
            raise AirflowFailException("Failed tests: \n" + "\n".join(failed_tests))