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

from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator)
import logging
from datetime import date, datetime, timedelta
from typing import List
# from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from lib import utils
from lib import environment


def get_reports_to_load():
    return [
        "attributed_ad_revenue",
        "clicks",
        "clicks_retargeting",
        "cohort_retargeting",
        "cohort_unified",
        "cohort_user_acquisition",
        "conversions_retargeting",
        "impressions",
        "impressions_retargeting",
        "inapps",
        "inapps_retargeting",
        "installs",
        "organic_ad_revenue",
        "organic_uninstalls",
        "retargeting_ad_revenue",
        "sessions",
        "sessions_retargeting",
        "skad_inapps",
        "skad_installs",
        "skad_postbacks",
        "skad_redownloads",
        "uninstalls"
    ]


def get_batch_id(game_name, service_name):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"af-load-{service_name}-{game_name}-{timestamp}"


def get_af_arguments(reports, date_from, date_to, app_ids, gcp_project_id, bigquery_project_id, apps_flyer_bucket_name, apps_flyer_home_folder, game_name):
    args = []
    args.append("--reports")
    args.extend(reports)
    args.append( "--date_from")
    args.append(date_from)
    args.append("--date_to")
    args.append(date_to)
    args.append("--app_ids")
    args.extend(app_ids)
    args.append("--gcp_project_id")
    args.append(gcp_project_id)
    args.append("--bigquery_project_id")
    args.append(bigquery_project_id)
    args.append("--bucket_name")
    args.append(apps_flyer_bucket_name)
    args.append("--home_folder")
    args.append(apps_flyer_home_folder)
    args.append("--game_name")
    args.append(game_name)
    return args


def get_af_cost_etl_arguments(reports, date_from, date_to, app_ids, gcp_project_id, bigquery_project_id, apps_flyer_bucket_name, game_name):
    args = []
    args.append("--reports")
    args.extend(reports)
    args.append( "--date_from")
    args.append(date_from)
    args.append("--date_to")
    args.append(date_to)
    args.append("--app_ids")
    args.extend(app_ids)
    args.append("--gcp_project_id")
    args.append(gcp_project_id)
    args.append("--bigquery_project_id")
    args.append(bigquery_project_id)
    args.append("--bucket_name")
    args.append(apps_flyer_bucket_name)
    args.append("--game_name")
    args.append(game_name)
    return args


def get_batch_config(arguments, job_bucket_name, script_name):

    arguments.append("--temp_gcp_bucket")
    arguments.append(job_bucket_name)

    BATCH_CONFIG = {
        "pyspark_batch": {
            "main_python_file_uri": "gs://{}/dataproc/{}.py".format(job_bucket_name, script_name),
            "args": arguments,
            "python_file_uris": [],
            "jar_file_uris": [
                "file:///usr/lib/spark/external/spark-avro.jar",
                "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
            ]
        },
        "labels": {
            "system": "gametuner",
            "subsystem": "appsflyer_datalocker_load"
        },
        "environment_config": {
            "execution_config": {
                "service_account": environment.airflow_default_service_account(),
                "subnetwork_uri": environment.appsflyer_dataproc_subnetwork()
            },
            "peripherals_config": {
                "spark_history_server_config": {}
            }
        },
        "runtime_config": {
            "version": "1.1"
        }
    }

    return BATCH_CONFIG


def get_af_operator(game_name: str,
                    execution_project_id: str,
                    region: str,
                    reports: list,
                    date_from: str,
                    date_to: str,
                    app_ids: list,
                    bigquery_project_id: str,
                    apps_flyer_bucket_name: str,
                    apps_flyer_home_folder: str,
                    job_bucket_name: str):

    return DataprocCreateBatchOperator(
        retries=3,
        timeout=60*60,
        request_id="load_appsflyer_data_{}_{}_{}".format(game_name, str(date_from), str(date_to)),
        task_id="load_appsflyer_data_{}".format(game_name),
        project_id=execution_project_id,
        region=region,
        batch=get_batch_config(
            get_af_arguments(
                reports,
                date_from,
                date_to,
                app_ids,
                execution_project_id,
                bigquery_project_id,
                apps_flyer_bucket_name,
                apps_flyer_home_folder,
                game_name
            ),
            job_bucket_name,
            'spark_job_data_locker'),
        batch_id=get_batch_id(game_name, 'data-locker')
    )


def get_af_cost_etl_operator(game_name: str,
                             execution_project_id: str,
                             region: str,
                             reports: list,
                             date_from: str,
                             date_to: str,
                             app_ids: list,
                             bigquery_project_id: str,
                             apps_flyer_bucket_name: str,
                             job_bucket_name: str):
    return DataprocCreateBatchOperator(
        retries=3,
        timeout=60*60,
        request_id="load_appsflyer_cost_etl_data_{}_{}_{}".format(game_name, str(date_from), str(date_to)),
        task_id="load_appsflyer_cost_etl_data_{}".format(game_name),
        project_id=execution_project_id,
        region=region,
        batch=get_batch_config(
            get_af_cost_etl_arguments(
                reports,
                date_from,
                date_to,
                app_ids,
                execution_project_id,
                bigquery_project_id,
                apps_flyer_bucket_name,
                game_name
            ),
            job_bucket_name,
            'spark_job_cost_etl'),
        batch_id=get_batch_id(game_name, 'cost-etl')
    )


def wait_af_cost_etl_bucket_data(execution_date: datetime, bucket_name: str, reports: List):
    """
    Wait for AppsFlyer data to be available in the bucket
    """
    current_day = execution_date.date() + timedelta(days=1)
    logging.info("Waiting for AppsFlyer data to be available in the bucket for date: {}".format(execution_date))

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    for report in reports:
        second_batch = f'cost_etl/v1/dt={str(current_day)}/b=2/{report}/_SUCCESS'

        second_batch_exist = gcs_hook.exists(bucket_name, second_batch)

        logging.info("Checking if file {} exists in bucket {} - {}".format(second_batch, bucket_name, second_batch_exist))
        if not second_batch_exist:
            return False

    return True


def wait_af_data_locker_bucket_data(execution_date: datetime, bucket_name: str, home_folder: str, reports: List):
    """
    Wait for AppsFlyer data to be available in the bucket
    It checks if the data is available for the current day
    """
    current_day = execution_date.date() + timedelta(days=1)
    two_days_ago = execution_date.date() - timedelta(days=1)
    logging.info("Waiting for AppsFlyer data to be available in the bucket for date: {}".format(current_day))

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    for report in reports:
        if report in ['attributed_ad_revenue', 'retargeting_ad_revenue', 'organic_ad_revenue']:
            last_in_prev_day = f'{home_folder}/data-locker-hourly/t={report}/dt={str(two_days_ago)}'
        elif report.startswith('cohort'):
            last_in_prev_day = f'{home_folder}/data-locker-hourly/t={report}/dt={str(execution_date.date())}'
        elif report.startswith('skad'):
            last_in_prev_day = f'{home_folder}/data-locker-hourly/t={report}/dt={str(execution_date.date())}'
        else:
            last_in_prev_day = f'{home_folder}/data-locker-hourly/t={report}/dt={str(current_day)}'
        files_in_folder = gcs_hook.list(bucket_name, prefix=last_in_prev_day)

        logging.info("Checking if files exist in path: {}/{} - number of files in folder: {}".format(bucket_name,last_in_prev_day, len(files_in_folder)))
        if len(files_in_folder) == 0:
            logging.info("No files found in folder: {}".format(last_in_prev_day))
            return False

    return True