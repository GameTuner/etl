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

from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSensorTimeout
from airflow.sensors.base import BaseSensorOperator

from lib.utils import deadline_exceeded_slack_alert


class DeadlineSensor(BaseSensorOperator):
    def __init__(self, task_id, task_id_to_wait_for, deadline_minutes: int, *args, **kwargs):
        super().__init__(task_id=task_id, on_failure_callback=deadline_exceeded_slack_alert, *args, **kwargs)
        self.deadline_minutes = deadline_minutes
        self.task_id_to_wait_for = task_id_to_wait_for

    def poke(self, context):
        if context['data_interval_end'] + timedelta(days=1) < datetime.utcnow().replace(tzinfo=timezone.utc):
            # avoid waiting for past dag runs
            return True

        if datetime.utcnow().replace(tzinfo=timezone.utc) > context['data_interval_end'] + timedelta(minutes=self.deadline_minutes):
            raise AirflowSensorTimeout()

        dagrun = self.dag.get_dagrun(execution_date=context['execution_date'])
        return dagrun.get_task_instance(task_id=self.task_id_to_wait_for).state == 'success'
