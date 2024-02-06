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

import json
import os.path
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Dict, Union
import pendulum

from lib.app_config.materialized_fields_config import SemanticLayerData


@dataclass
class Parameter:
    name: str
    type: str
    description: str


@dataclass
class Schema:
    url: str
    vendor: str
    name: str
    version: str
    description: str
    parameters: List[Parameter]


@dataclass
class AppsFlyerConfig:
    reports: List[str]
    home_folder: str
    app_ids: List[str]
    external_bucket_name: str


@dataclass
class AppsFlyerCostEtlConfig:
    bucket_name: str
    reports: List[str]
    android_app_id: str
    ios_app_id: str

@dataclass
class StoreItunesSalesReportConfig:
    app_sku_id: str
    apple_id: str
    issuer_id: str
    key_id: str
    key_value: str
    vendor_number: str

@dataclass
class StoreGooglePlaySalesReportConfig:
    app_bundle_id: str
    service_account: str
    report_bucket_name: str


@dataclass
class ExternalServices:
    apps_flyer: Union[AppsFlyerConfig, None]
    apps_flyer_cost_etl: Union[AppsFlyerCostEtlConfig, None]
    store_itunes_sales_report: Union[StoreItunesSalesReportConfig, None]
    store_gogole_play_sales_report: Union[StoreGooglePlaySalesReportConfig, None]

    def has_external_services(self):
        return self.apps_flyer is not None \
                or self.apps_flyer_cost_etl is not None \
                or self.store_itunes_sales_report is not None \
                or self.store_gogole_play_sales_report is not None \

        
@dataclass
class Datasource:
    id: str
    has_data_from: date
    has_data_up_to: date
    semantic_layer_data: SemanticLayerData


@dataclass
class AppConfig:
    app_id: str
    gdpr_event_parameters: Dict[str, List[str]]
    timezone: str
    created: date
    event_schemas: Dict[str, Schema]
    external_services: ExternalServices
    datasources: List[Datasource]

    def created_midnight(self):
        return pendulum.datetime(self.created.year, self.created.month, self.created.day, tz=self.timezone)

    def created_midnight_utc(self):
        return pendulum.datetime(self.created.year, self.created.month, self.created.day, tz='UTC')


@dataclass
class CommonConfigs:
    atomic_fields: Dict[str, str]
    gdpr_event_parameters: Dict[str, List[str]]
    gdpr_context_parameters: Dict[str, List[str]]
    gdpr_atomic_parameters: List[str]
    close_event_partition_after_hours: int
    non_embedded_context_schemas: Dict[str, Schema]


@dataclass
class MetadataAppsConfig:
    common_configs: CommonConfigs
    app_id_configs: Dict[str, AppConfig]

    def all_event_names(self, app_id: str):
        return list(self.app_id_configs[app_id].event_schemas.keys())

    def event_gdpr_fields(self, app_id: str, event_name: str):
        params = []
        if event_name in self.common_configs.gdpr_event_parameters:
            params.extend([f'params.{name}' for name in self.common_configs.gdpr_event_parameters[event_name]])

        if event_name in self.app_id_configs[app_id].gdpr_event_parameters:
            params.extend([f'params.{name}' for name in self.app_id_configs[app_id].gdpr_event_parameters[event_name]])

        # add embedded contexts
        for context_name, gdpr_params in self.common_configs.gdpr_context_parameters.items():
            params.extend([f'{context_name}.{name}' for name in gdpr_params])
        return params

    def context_gdpr_fields(self, app_id: str, context_name: str):
        if context_name in self.common_configs.gdpr_context_parameters:
            return [f'params.{name}' for name in self.common_configs.gdpr_context_parameters[context_name]]

        return []
    
    def get_semantic_layer_data(self, app_id: str, datasource_name: str):
        return next((x.semantic_layer_data for x in self.app_id_configs[app_id].datasources if x.id == datasource_name), None)


def build_external_service(app_id_config_dict) -> ExternalServices:
    apps_flyer = None
    apps_flyer_config = next((x['service_params'] for x in app_id_config_dict['external_services'] if x['service_name'] == "apps_flyer"), None)
    if apps_flyer_config:
        apps_flyer = AppsFlyerConfig(
            reports=apps_flyer_config['reports'],
            home_folder=apps_flyer_config['home_folder'],
            app_ids=apps_flyer_config['app_ids'],
            external_bucket_name=apps_flyer_config['external_bucket_name'] if 'external_bucket_name' in apps_flyer_config else None
        )
    apps_flyer_cost_etl = None
    apps_flyer_cost_etl_config = next((x['service_params'] for x in app_id_config_dict['external_services'] if x['service_name'] == "apps_flyer_cost_etl"), None)
    if apps_flyer_cost_etl_config:
        apps_flyer_cost_etl = AppsFlyerCostEtlConfig(
            bucket_name=apps_flyer_cost_etl_config['bucket_name'],
            reports=apps_flyer_cost_etl_config['reports'],
            android_app_id=apps_flyer_cost_etl_config['android_app_id'],
            ios_app_id=apps_flyer_cost_etl_config['ios_app_id']
        )
    
    store_itunes_sales_report = None
    store_itunes_sales_report_config = next((x['service_params'] for x in app_id_config_dict['external_services'] if x['service_name'] == "store_itunes"), None)
    if store_itunes_sales_report_config:
        store_itunes_sales_report = StoreItunesSalesReportConfig(
            app_sku_id=store_itunes_sales_report_config['app_sku_id'],
            apple_id=store_itunes_sales_report_config['apple_id'],
            issuer_id=store_itunes_sales_report_config['issuer_id'],
            key_id=store_itunes_sales_report_config['key_id'],
            key_value=store_itunes_sales_report_config['key_value'],
            vendor_number=store_itunes_sales_report_config['vendor_number']
        )

    store_gogole_play_sales_report = None
    store_gogole_play_sales_report_config = next((x['service_params'] for x in app_id_config_dict['external_services'] if x['service_name'] == "store_google_play"), None)
    if store_gogole_play_sales_report_config:
        store_gogole_play_sales_report = StoreGooglePlaySalesReportConfig(
            app_bundle_id=store_gogole_play_sales_report_config['app_bundle_id'],
            service_account=store_gogole_play_sales_report_config['service_account'],
            report_bucket_name=store_gogole_play_sales_report_config['report_bucket_name']
        )

    return ExternalServices(
        apps_flyer=apps_flyer,
        apps_flyer_cost_etl=apps_flyer_cost_etl,
        store_itunes_sales_report=store_itunes_sales_report,
        store_gogole_play_sales_report=store_gogole_play_sales_report
    )

def build_datasources(datasources_dict):
    return [Datasource(
        id=datasources_dict[datasource]["id"],
        has_data_from=datetime.strptime(datasources_dict[datasource]['has_data_from'], "%Y-%m-%d"),
        has_data_up_to=datetime.strptime(datasources_dict[datasource]["has_data_up_to"], "%Y-%m-%d") if datasources_dict[datasource]["has_data_up_to"] is not None else None,
        semantic_layer_data=SemanticLayerData.build_from_metrics_list(datasources_dict[datasource]["materialized_columns"])
    ) for datasource in datasources_dict]

def build_schemas(schemas_dict):
    return {id: Schema(
        url=schema_dict["url"],
        vendor=schema_dict["vendor"],
        name=schema_dict["name"],
        version=schema_dict["version"],
        description=schema_dict["description"],
        parameters=[Parameter(
            name=parameter_dict["name"],
            type=parameter_dict["type"],
            description=parameter_dict["description"]
        ) for parameter_dict in schema_dict["parameters"]]
    ) for id, schema_dict in schemas_dict.items()}


def load_from_dict(raw_dict) -> MetadataAppsConfig:
    return MetadataAppsConfig(
        common_configs=CommonConfigs(
            atomic_fields=raw_dict['common_configs']['atomic_fields'],
            gdpr_event_parameters=raw_dict['common_configs']['gdpr_event_parameters'],
            gdpr_context_parameters=raw_dict['common_configs']['gdpr_context_parameters'],
            gdpr_atomic_parameters=raw_dict['common_configs']['gdpr_atomic_parameters'],
            close_event_partition_after_hours=raw_dict['common_configs']['close_event_partition_after_hours'],
            non_embedded_context_schemas = build_schemas(raw_dict['common_configs']['non_embedded_context_schemas'])
        ),
        app_id_configs={app_id: AppConfig(
            app_id=app_id,
            gdpr_event_parameters=app_id_config_dict['gdpr_event_parameters'],
            timezone=app_id_config_dict['timezone'],
            created=datetime.strptime(app_id_config_dict['created'], "%Y-%m-%d"),
            event_schemas=build_schemas(app_id_config_dict['event_schemas']),
            external_services=build_external_service(app_id_config_dict),
            datasources=build_datasources(app_id_config_dict['datasources'])
        ) for app_id, app_id_config_dict in raw_dict['app_id_configs'].items()}
    )


def load_from_json_file_if_exists(path: str) -> MetadataAppsConfig:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return load_from_dict(json.load(f))
