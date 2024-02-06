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
from datetime import date, timedelta, datetime

import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# This query takes into account the fact that some collectors/enrichers/loaders can be slow and fall behind
# We look for 1 hour of loader time and take minimum collector timestamp
# If collector timestamp is > than threshold, some slow services either didn't manage to send a single event or
# we completed the partition. For now, we will assume the latter.
# If needed, this can be solved accuratelly by making collectors report progress to some shared storage
from lib import utils

MIN_COLLECTOR_TS_QUERY_TEMPLATE = '''
SELECT 
  MIN(collector_tstamp)
FROM {app_id}_load.ctx_event_context
WHERE 
  date_ = "{execution_date}"
  AND load_tstamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)    
'''

# We force close the partition when collector time is after this value and there was no data
PARTITION_FORCE_CLOSE_AFTER_MIDNIGHT_HOURS = 12


def wait_for_event_partition(app_id: str, execution_date: datetime, timezone: str, close_event_partition_after_hours: int):
    day_after_local_execution_date = utils.get_local_date_from_utc_datetime(execution_date, timezone) + timedelta(days=1)
    day_after_execution_date_dt = pendulum.datetime(day_after_local_execution_date.year, day_after_local_execution_date.month, day_after_local_execution_date.day, tz=timezone)
    logging.info(f"For execution date {execution_date}, localized execution date is: {day_after_execution_date_dt} {timezone}")

    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    query = MIN_COLLECTOR_TS_QUERY_TEMPLATE.format(
        app_id=app_id,
        execution_date=day_after_local_execution_date)
    logging.info(f'Executing query: {query}')
    min_timestamp = next(hook.get_client().query(query).result())[0]
    if min_timestamp:
        deadline = day_after_execution_date_dt + timedelta(hours=close_event_partition_after_hours)
        logging.info(f'Min collector timestamp is: {min_timestamp}, partition close deadline is: {deadline} (app is in {timezone} timezone)')
        if min_timestamp > deadline:
            logging.info(f'Closing event partitions')
            return True
    else:
        deadline = day_after_execution_date_dt + timedelta(hours=PARTITION_FORCE_CLOSE_AFTER_MIDNIGHT_HOURS)
        logging.info(f"No new data found in last hour. Force closing deadline is: {deadline}")
        if pendulum.now(timezone) > deadline:
            logging.info(f'Closing event partitions')
            return True

    logging.info(f"Waiting, it's not time to close event partitions yet")
