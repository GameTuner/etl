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

from datetime import date, datetime
from typing import Callable

import pytz
from airflow.utils.context import Context
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from lib.utils import task_failed_detailed_slack_alert


def get_local_date_from_utc_datetime(dt: datetime, timezone: str) -> date:
    timezone = pytz.timezone(timezone)
    local_execution_date = dt.astimezone(timezone)
    return local_execution_date.date()


class BigQueryExecuteLazyQueryOperator(BigQueryExecuteQueryOperator):
    def __init__(self, task_id: str, location: str, gcp_conn_id: str, app_id: str, query_generator: Callable[[date], str], **kwargs) -> None:
        super().__init__(
            task_id=task_id,
            location=location,
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            sql='',  # will be generated at runtime,
            on_failure_callback=task_failed_detailed_slack_alert,
            labels={'app_id':app_id, 'enviroment': 'production', 'service': 'etl'},
            **kwargs
        )
        self.query_generator = query_generator

    def execute(self, context: Context):
        self.log.info(f"Execution date: {context['dag_run'].execution_date} {type(context['dag_run'].execution_date)}")
        execution_date = get_local_date_from_utc_datetime(context['dag_run'].execution_date, context["dag"].timezone.name)
        self.sql = self.query_generator(execution_date)
        super().execute(context)


