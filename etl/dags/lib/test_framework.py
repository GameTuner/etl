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

from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from lib import environment
from operators.test import ExecuteTestOperator


def get_test_task(test_name: str, app_id: str, mandatory_tests_path: str, optional_test_path: str, **kwargs) -> TaskGroup:
    """
    Definition returns test task group. It contains mandatory and optional tests. 
    If mandatory test fails, it will be marked as failed. 
    If test is optional, it will be marked as skipped.
    
    :param test_name: Name of the test
    :param mandatory_tests_path: Path to mandatory tests
    :param optional_test_path: Path to optional tests
    :param **kwargs: Additional arguments
    :return: TaskGroup
    """
    with TaskGroup(test_name, tooltip="Test transformation step") as test:

        mandatory_test_start = DummyOperator(
            task_id="mandatory_test_start"                   
        )

        mandatory_test_end = DummyOperator(
            task_id="mandatory_test_end"                                                          
        )

        with TaskGroup("mandatory_tests", tooltip="Mandatory tests group") as mandatory_tests:
            DummyOperator(
                task_id="mandatory_dummy_test"
            )
            if mandatory_tests_path is not None and os.path.exists(mandatory_tests_path):
                for test_file in os.listdir(f'{mandatory_tests_path}'):
                    if test_file.endswith(".yaml"):
                        test_name = test_file.split(".")[0]
                        ExecuteTestOperator(
                            task_id="mandatory_test_{}".format(test_name),
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            gcp_project_id=environment.gcp_project_id(),
                            app_id=app_id,
                            unique_id_bq_connection=environment.unique_id_bq_connection(),
                            yaml_test_path=f'{mandatory_tests_path}/{test_file}',
                            additional_params=kwargs
                        )
                        

        optional_test_start = DummyOperator(
            task_id="optional_test_start"                
        )

        optional_test_end = DummyOperator(
            task_id="optional_test_end",
            trigger_rule="all_done"                                                       
        )
        
        with TaskGroup("optional_tests", tooltip="Optional tests group", default_args={
            'depends_on_past': False
        }) as optional_tests:
            DummyOperator(
                task_id="optional_dummy_test"
            )
            if optional_test_path is not None and os.path.exists(optional_test_path):
                for test_file in os.listdir(f'{optional_test_path}'):
                    if test_file.endswith(".yaml"):
                        test_name = test_file.split(".")[0]
                        ExecuteTestOperator(
                            task_id="optional_test_{}".format(test_name),
                            location=environment.bigquery_datasets_location(),
                            gcp_conn_id='google_cloud_default',
                            gcp_project_id=environment.gcp_project_id(),
                            app_id=app_id,
                            unique_id_bq_connection=environment.unique_id_bq_connection(),
                            yaml_test_path=f'{optional_test_path}/{test_file}',
                            additional_params=kwargs
                        )

        mandatory_test_start >> mandatory_tests >> mandatory_test_end
        optional_test_start >> optional_tests >> optional_test_end

    return test
    