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
import jwt
import requests
from datetime import datetime, date
import gzip
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from lib.app_config.app_config import StoreItunesSalesReportConfig

# Set the JWT token and API endpoint URL
API_URL = 'https://api.appstoreconnect.apple.com/v1/salesReports'

# Set the date range for the sales report
TEMP_FOLDER = 'temp_store_itunes_sales_report'

DELETE_CURRENT_DATE_DATA = '''
DELETE FROM `{project_id}.{app_id}_external.store_itunes_sales`
WHERE date_ = "{execution_date}"
'''

def read_private_key_from_file(key_file: str):
    with open(key_file, 'r') as f:
        private_key = f.read()
    
    return private_key

def get_jwt_token(key_id: str, issuer_id: str, private_key: str):
    # Set the JWT header and payload data
    jwt_header = {
        'alg': 'ES256',
        'kid': key_id
    }
    jwt_payload = {
        'iss': issuer_id,
        'exp': int(datetime.now().timestamp()) + 1200,
        'aud': 'appstoreconnect-v1'
    }

    # Generate the JWT token using the private key   
    jwt_token = jwt.encode(jwt_payload, private_key, algorithm='ES256', headers=jwt_header)

    return jwt_token

def download_itunes_sales_report(jwt_token: str, vendor_number: str, report_date: datetime):
    # Set the request headers and parameters
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'application/json'
    }
    params = {
        'filter[frequency]': 'DAILY',
        'filter[reportType]': 'SALES',
        'filter[reportSubType]': 'SUMMARY',
        'filter[vendorNumber]': vendor_number,
        'filter[reportDate]': f'{report_date.date().isoformat()}'
    }

    if not os.path.exists(TEMP_FOLDER):
        os.makedirs(TEMP_FOLDER)
    
    # Send the API request and download the sales report
    response = requests.get(API_URL, headers=headers, params=params)
    if response.status_code == 200:
        file_name = f'itunes_sales_report_{report_date.isoformat()}'
        output_gzip_file = f'{TEMP_FOLDER}/{file_name}.gz'
        with open(output_gzip_file, 'wb') as file:
            file.write(response.content)

        data_file_name = f'{TEMP_FOLDER}/{file_name}.csv'
        with gzip.open(output_gzip_file, 'rb') as f:
                with open(data_file_name, 'wb') as f_out:
                    f_out.write(f.read())

        return data_file_name
    else:
        raise Exception(f'An error occurred: {response.status_code} - {response.text}')

def underscore_columns(df):
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()
    return df

def load_data_to_biqquery(project_id: str, app_id: str, execution_date: datetime, file_name: str, app_sku_id: str):
    # Load the data to BigQuery
    print(f'Loading data from {file_name} to BigQuery')

    sales_data = pd.read_csv(file_name, delimiter='\t')
    shutil.rmtree(TEMP_FOLDER)

    sales_data =  underscore_columns(sales_data)
    sales_data = sales_data[sales_data['product_type_identifier'] == 'IA1']
    sales_data = sales_data[sales_data['parent_identifier'] == app_sku_id]
    sales_data_purchase = sales_data[['begin_date', 'sku', 'version', 'country_code', 'currency_of_proceeds', 'units', 'customer_price', 'developer_proceeds']]
    columns_rename = {
        "begin_date": "date_", 
        "sku": "package_id",
        "version": "app_version",
        "currency_of_proceeds": "currency_code",
        "customer_price": "price",}
    sales_data_purchase = sales_data_purchase.rename(columns=columns_rename)
    sales_data_purchase['date_'] = sales_data_purchase['date_'].astype('datetime64[ns]')
    sales_data_purchase["date_"] = pd.to_datetime(sales_data_purchase["date_"], format="%Y-%m-%d", infer_datetime_format=True)
    
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    query = DELETE_CURRENT_DATE_DATA.format(
        project_id=project_id,
        app_id=app_id,
        execution_date=execution_date.date().isoformat())
    logging.info(f'Executing query: {query}')
    hook.get_client(project_id).query(query).result()

    table_id = f'{app_id}_external.store_itunes_sales'
    sales_data_purchase.to_gbq(table_id, project_id=project_id, if_exists='append')

def get_itunes_sales_sensor_task(itunes_config: StoreItunesSalesReportConfig, execution_date: date):
    try:
        download_itunes_sales_report(
                get_jwt_token(
                    itunes_config.key_id, 
                    itunes_config.issuer_id, 
                    itunes_config.key_value), 
                itunes_config.vendor_number, 
                execution_date)

        return True
    except Exception as e:
        logging.error(f'Faild to get iTunes report for sensor: {e}') 
        return False

def get_itunes_sales_report_task(project_id: str, app_id: str, itunes_config: StoreItunesSalesReportConfig, execution_date: date):
    report_file = download_itunes_sales_report(get_jwt_token(itunes_config.key_id, itunes_config.issuer_id, itunes_config.key_value), itunes_config.vendor_number, execution_date)
    load_data_to_biqquery(project_id, app_id, execution_date, report_file, itunes_config.app_sku_id)