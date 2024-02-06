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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.bigquery import BigQueryExecuteLazyQueryOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import requests
import json
from google.cloud import bigquery
import logging
from lib.utils import get_sql_query_from_file

from lib import environment

common_dataset_name = 'gametuner_common'
currency_table_name = 'currencies'

def update_currency_data(currency_date):
    client = bigquery.Client()
    
    url = f'https://currencyapi.com/api/v3/latest?apikey={environment.currency_api_key()}&base_currency=USD'
    
    r = requests.get(url)
    parsed = json.loads(r.text)

    dataset_ref = client.dataset(common_dataset_name)
    table_ref = dataset_ref.table(currency_table_name)
    table = client.get_table(table_ref)  # API call

    logging.info("Delete current data...")
    query_delete_for_today = f"DELETE FROM {common_dataset_name}.{currency_table_name} WHERE date_ BETWEEN '{currency_date}' AND '{currency_date}'"
    query_errors = client.query(query_delete_for_today)
    assert query_errors.errors == None

    rows_to_insert = []
    logging.info("Uploading data into BigQuery...")
    for key, value in parsed["data"].items():
        rows_to_insert.append( {"date_": str(currency_date), "currency": value['code'], "rate": value['value']} )

    load_job = client.load_table_from_json(rows_to_insert, table)
    assert load_job.errors == None

def currencies_etl(execution_date: datetime):    
    logging.info("Fetching data from the API...")
    # add 1 day to the execution date to get the data for the current day
    execution_date = execution_date + timedelta(days=1)
    date_ = execution_date.strftime('%Y-%m-%d')
    update_currency_data(date_)


with DAG('DAG_currency_extractions_from_API', 
        description='DAG for fetching currency data', 
        schedule_interval='0 1 * * *', 
        start_date=datetime(2021, 1, 1), 
        catchup=False) as dag:
    
    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')
    
    with TaskGroup("get_currency_data", tooltip="Currency data group") as get_currency_data:
    
        if environment.currency_api_key() is None:
            logging.info("Currency API key is not set. Skipping currency extraction.")
            currency_extraction_task = DummyOperator(task_id='currency_extraction')
        else:            
            currency_extraction_task = PythonOperator(
                task_id='currency_extraction', 
                python_callable=lambda execution_date:  currencies_etl(execution_date)
            )

            currency_extraction_task

    start >> get_currency_data >> end