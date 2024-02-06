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

import os.path
from dataclasses import dataclass
from typing import Dict, List
import logging
import yaml


@dataclass
class MaterializedField:
    metric_name: str
    select_expression: str
    data_type: str
    totals: bool = True
    user_history_formula: str = None

@dataclass
class MaterializedTable:
    table_name: str
    dataset: str
    materialized_fields: List[MaterializedField]


@dataclass
class SemanticLayerData:
    materialized_tables: List[MaterializedTable]

    @staticmethod
    def build_from_metrics_list(metrics: List) -> 'SemanticLayerData':
        materialized_tables = []
        for metric in metrics:
            if metric["table_name"] in [x.table_name for x in materialized_tables]:
                temp_materialized_table = next(x for x in materialized_tables if x.table_name == metric["table_name"])
            else:
                temp_materialized_table = MaterializedTable(
                    table_name=metric["table_name"],
                    dataset=metric["dataset"],
                    materialized_fields=list()
                )
                materialized_tables.append(temp_materialized_table)

            if metric["column_name"] in [x.metric_name for x in temp_materialized_table.materialized_fields]:
                logging.warning(f"Metric {metric['column_name']} already exists in table {metric['table_name']}")
            else:
                temp_materialized_table.materialized_fields.append(MaterializedField(
                    metric_name=metric["column_name"],
                    select_expression=metric["select_expression"],
                    data_type=metric["data_type"],
                    totals=metric.get("totals", True),
                    user_history_formula=metric.get("user_history_formula", None)
                ))
        
        return SemanticLayerData(materialized_tables)

@dataclass
class SemanticLayerCofig:
    apps_config: Dict[str, SemanticLayerData]

    def get_app_config(self, app_id: str) -> SemanticLayerData:
        return self.apps_config[app_id]


def load_from_yaml_if_exists(path: str) -> SemanticLayerCofig:
    if not os.path.exists(path):
        return None
    
    with open(path, "r") as f:
        data = yaml.safe_load(f)
        semantic_apps_data = {}
        for app_config in data["apps"]:
            materialized_tables = []

            for app_config_dict in app_config["materialized_tables"]:
                temp_materialized_table = MaterializedTable(
                    table_name=app_config_dict["table_name"],
                    dataset=app_config_dict["dataset"]
                )

                temp_materialized_table.materialized_fields= list()
                for field in app_config_dict["materialized_fields"]:
                    temp_materialized_table.materialized_fields.append(MaterializedField(
                        metric_name=field["metric_name"],
                        select_expression=field["select_expression"],
                        data_type=field["data_type"],
                        totals=field.get("totals", True),
                        user_history_formula=field.get("user_history_formula", None)
                    ))

                materialized_tables.append(temp_materialized_table)

            semantic_apps_data[app_config['app_id']] = SemanticLayerData(materialized_tables)

    return SemanticLayerCofig(semantic_apps_data)


def save_to_yaml(semantc_layer_data: SemanticLayerData, path: str):
    with open(path, "w") as f:
        # omit tags
        yaml.emitter.Emitter.process_tag = lambda self, *args, **kw: None
        yaml.dump(semantc_layer_data, f, sort_keys=False)