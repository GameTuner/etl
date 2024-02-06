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
import os
import shutil
from datetime import datetime, date
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import storage

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from lib.app_config.app_config import StoreGooglePlaySalesReportConfig


TEMP_FOLDER = 'temp_store_google_play_sales_report'
SCOPES = ['https://www.googleapis.com/auth/devstorage.read_only']

DELETE_CURRENT_DATE_DATA = '''
DELETE FROM `{project_id}.{app_id}_external.store_google_play_sales`
WHERE date_ = "{execution_date}";
'''

def get_credentials(credentials_data: str):
    credentials_dict = json.loads(credentials_data, strict=False)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    return credentials

def download_google_play_store_report(report_date: date, client: storage.Client, bucket_id: str):
    report_file_name = f'salesreport_{report_date.year}{report_date.month:02d}'
    report_to_download = f'sales/{report_file_name}.zip'

    if not os.path.exists(TEMP_FOLDER):
        os.makedirs(TEMP_FOLDER)

    # create bucket object
    bucket = client.get_bucket(bucket_id)
    # create blob object
    report_data = bucket.get_blob(report_to_download)

    file_name = f'google_play_sales_report_{report_date.isoformat()}'
    output_gzip_file = f'{TEMP_FOLDER}/{file_name}.zip'
    with open(output_gzip_file, 'wb') as file:
        file.write(report_data.download_as_string())

    # unpack zip file
    shutil.unpack_archive(output_gzip_file, TEMP_FOLDER)
    csv_file_name = f'{TEMP_FOLDER}/{report_file_name}.csv'

    return csv_file_name

def underscore_columns(df):
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()
    return df

def load_data_to_biqquery(project_id: str, app_id: str, file_name: str, app_bundle_id: str, report_date: date):
    sales_data = pd.read_csv(file_name, thousands=',')
    shutil.rmtree(TEMP_FOLDER)

    sales_data =  underscore_columns(sales_data)
    # sales_data = sales_data[sales_data['financial_status'] == 'Charged']
    sales_data = sales_data[sales_data['product_id'] == app_bundle_id]
    sales_data = sales_data[sales_data['order_charged_date'] == report_date.date().isoformat()]
    
    sales_data_purchase = sales_data[['order_charged_date', 'order_number', 'financial_status', 'sku_id', 'currency_of_sale', 'item_price', 'charged_amount', 'country_of_buyer']]
    columns_rename = {
        "order_charged_date": "date_", 
        "currency_of_sale": "currency_code",
        "charged_amount": "price",
        "country_of_buyer": "country_code",
        "sku_id": "package_id"}
    sales_data_purchase = sales_data_purchase.rename(columns=columns_rename)
    sales_data_purchase["date_"] = pd.to_datetime(sales_data_purchase["date_"], format="%Y-%m-%d", infer_datetime_format=True)
    
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    query = DELETE_CURRENT_DATE_DATA.format(
        project_id=project_id,
        app_id=app_id,
        execution_date=report_date.date().isoformat())
    logging.info(f'Executing query: {query}')
    hook.get_client(project_id).query(query).result()

    table_id = f'{app_id}_external.store_google_play_sales'
    sales_data_purchase.to_gbq(table_id, project_id=project_id, if_exists='append')

def get_google_play_sales_report_task(project_id: str, app_id: str, google_play_config: StoreGooglePlaySalesReportConfig, execution_date: date):
    credentials_ = get_credentials(google_play_config.service_account)
    client = storage.Client(credentials_.project_id, credentials_)

    file_name = download_google_play_store_report(execution_date, client, google_play_config.report_bucket_name)
    load_data_to_biqquery(project_id, app_id, file_name, google_play_config.app_bundle_id, execution_date)

