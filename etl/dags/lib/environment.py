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

import os

DATA_DIR = '/home/airflow/gcs/data'
USER_HISTORY_DAG_INCLUDE_DIR = '/home/airflow/gcs/dags/user_history/include'


def gcp_project_id():
    return os.environ['GCP_PROJECT_ID']


def bigquery_datasets_location():
    return os.environ['BQ_DATASETS_LOCATION']


def metadata_ip_address():
    return os.environ['METADATA_IP_ADDRESS']


def maxmind_bucket():
    try:
        return os.environ['MAXMIND_BUCKET']
    except KeyError:
        return None


def maxmind_licence_key():
    try:
        return os.environ['MAXMIND_LICENCE_KEY']
    except KeyError:
        return None


def appsflyer_data_locker_bucket_name():
    return os.environ['APPSFLYER_DATA_LOCKER_BUCKET_NAME']


def appsflyer_bigquery_project_id():
    return os.environ['APPSFLYER_BIGQUERY_PROJECT_ID']


def appsflyer_dataproc_bucket_name():
    return os.environ['APPSFLYER_DATAPROC_BUCKET_NAME']


def appsflyer_dataproc_execution_region():
    return os.environ['APPSFLYER_DATAPROC_EXECUTION_REGION']


def appsflyer_dataproc_execution_project_id():
    return os.environ['APPSFLYER_DATAPROC_EXECUTION_PROJECT_ID']


def appsflyer_dataproc_subnetwork():
    return os.environ['APPSFLYER_DATAPROC_SUBNETWORK_NAME']


def airflow_default_service_account():
    return os.environ['AIRFLOW_DEFAULT_SERVICE_ACCOUNT']


def slack_webhook_url():
    try:
        return os.environ['SLACK_WEBHOOK_URL']
    except KeyError:
        return None

def currency_api_key():
    try:
        return os.environ['CURRENCY_API_KEY']
    except KeyError:
        return None

def unique_id_bq_connection():
    return os.environ['UNIQUE_ID_BQ_CONNECTION']

def composer_environment_url():
    return os.environ['COMPOSER_ENVIRONMENT_URL']
