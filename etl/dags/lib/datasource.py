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

from datetime import date

import requests

from lib import environment


def update_datasource_freshness(app_id: str, datasource_id: str, has_data_up_to_date: date):
    r = requests.put(f'http://{environment.metadata_ip_address()}/api/v1/apps/{app_id}/datasource-freshness/{datasource_id}/{has_data_up_to_date}')
    r.raise_for_status()