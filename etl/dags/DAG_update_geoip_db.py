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
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup

import requests
import re
import shutil
import os

from lib import environment


def download_geoip_db():
    maxmind_db_link = f'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key={environment.maxmind_licence_key()}&suffix=tar.gz'
    temp_folder = 'temp'

    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
    request_call = requests.get(maxmind_db_link, allow_redirects=True)
    logging.info(str(request_call.headers))
    logging.info(request_call.status_code)
    logging.info(maxmind_db_link)
    content_disposition = request_call.headers['content-disposition']
    file_name = re.findall("filename=(.+)", content_disposition)[0]
    output_gzip_file = temp_folder + '/' + file_name
    with open(output_gzip_file, 'wb') as file:
        file.write(request_call.content)
    shutil.unpack_archive(filename=output_gzip_file, extract_dir=temp_folder)
    output_folder_name = file_name.split('.')[0]

    shutil.move(temp_folder + '/' + output_folder_name + '/' + 'GeoLite2-City.mmdb', 'GeoLite2-City.mmdb')
    shutil.rmtree(temp_folder)


with DAG(
        'DAG_update_geoip_db',
        start_date=datetime(2022, 1, 1),
        schedule_interval='0 8 * * 3',
        catchup=False) as dag:
    
    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    with TaskGroup("get_geoip_db", tooltip="Get GeoIP DB group") as get_geoip_db:

        if environment.maxmind_licence_key() is None:
            logging.info("Maxmind licence key not set, skipping download")
            DummyOperator(task_id='dummy_task_for_skip')
        else:
            download_geoip_db = PythonOperator(
                task_id='download_geoip_db',
                retries=3,
                python_callable=download_geoip_db)

            upload_file_to_bucket = LocalFilesystemToGCSOperator(
                task_id="upload_file_to_bucket",
                src='GeoLite2-City.mmdb',
                dst='GeoLite2-City.mmdb',
                bucket=environment.maxmind_bucket(),
            )

            download_geoip_db >> upload_file_to_bucket

    start >> get_geoip_db >> end
