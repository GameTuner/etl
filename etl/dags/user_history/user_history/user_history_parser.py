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

from lib import environment
from lib.app_config.materialized_fields_config import SemanticLayerData

SQL_TEMPLATE_TEMP_TABLE_FOR_METRIC = """
CREATE OR REPLACE TEMPORARY TABLE {table_name}_temp_{app_id}
AS
SELECT
    unique_id,
    {select_clause}
FROM `{project_id}.{app_id}_{dataset}.{table_name}` -- table from semantic layer, TODO: tempatize dataset
WHERE date_ = execution_date
GROUP BY unique_id;
"""

SQL_TEMPLATE_SELECT_EXPRESSION = """
CAST({expression} AS {data_type}) AS {metric_name}, -- select expresion from semantic layer
"""

SQL_TEMPLATE_INSERT_OLD_SELECT = """
IFNULL({table_name}_temp_{app_id}.{metric_name}, 0) AS {metric_name},
"""

SQL_TEMPLATE_INSERT_OLD_SELECT_TOTAL = """
IF({table_name}_temp_{app_id}.{metric_name} IS NULL, user_history.{metric_name}_total, user_history.{metric_name}_total + {table_name}_temp_{app_id}.{metric_name}) AS {metric_name}_total,
"""

SQL_TEMPLATE_INSERT_OLD_TABLE = """
LEFT JOIN {table_name}_temp_{app_id} ON user_history.unique_id = {table_name}_temp_{app_id}.unique_id
"""

SQL_TEMPLATE_INSERT_NEW_SELECT = """
IFNULL({table_name}_temp_{app_id}.{metric_name}, 0) AS {metric_name},
"""

SQL_TEMPLATE_INSERT_NEW_SELECT_TOTAL = """
IFNULL({table_name}_temp_{app_id}.{metric_name}, 0) AS {metric_name}_total,
"""

SQL_TEMPLATE_NEW_TABLE = """
LEFT JOIN {table_name}_temp_{app_id} ON registered_users.unique_id = {table_name}_temp_{app_id}.unique_id
"""



def get_user_history_sql(project_id: str, app_id: str, execution_date: str, semantic_layer_data: SemanticLayerData):

    user_history_query = open(f'{environment.USER_HISTORY_DAG_INCLUDE_DIR}/user_history/insert_user_history.sql', 'r').read()

    temp_tables = []
    insert_old_select = []
    insert_new_select = []
    insert_old_table = []
    insert_new_table = []

    
    user_history_columns = {
        'date_':'DATE',
        'last_installation_id':'STRING',
        'user_id':'STRING',
        'unique_id':'STRING',
        'registration_date':'DATE',
        'registration_event_id':'STRING',
        'registration_platform':'STRING',
        'registration_country':'STRING',
        'registration_application_version':'STRING',
        'last_login_country':'STRING',
        'last_login_platform':'STRING',
        'last_login_application_version':'STRING',
        'days_active_last_7_days':'INT64',
        'days_active_last_28_days':'INT64',
        'days_active':'INT64',
        'last_login_day':'DATE',
        'previous_login_day':'DATE',
        'first_transaction_day':'DATE',
        'previous_transaction_day':'DATE',
        'last_transaction_day':'DATE',
        'is_payer':'BOOL',
        'is_repeated_payer':'BOOL',
        'cohort_day':'INT64',
        'cohort_size':'INT64',
        'sessions_count':'INT64',
        'playtime':'FLOAT64',
        'sessions_count_total':'INT64',
        'playtime_total':'FLOAT64',
        'dau':'INT64',
        'dau_yesterday':'INT64',
        'dau_2days_ago':'INT64',
        'wau':'INT64',
        'mau':'INT64',
        'mau_lost':'INT64',
        'mau_reactivated':'INT64',
        'net_revenue_usd':'FLOAT64',
        'net_revenue_usd_total':'FLOAT64',
        'gross_revenue_usd':'FLOAT64',
        'gross_revenue_usd_total':'FLOAT64',
        'transactions_count':'INT64',
        'transactions_count_total':'INT64',
        'new_payers':'INT64',
        'total_payers':'INT64',
        'daily_payers':'INT64',
        'days_since_last_purchase':'INT64' 
    }
    
    if semantic_layer_data is not None:
        for metric in semantic_layer_data.materialized_tables:

            select_expressions = []
            
            for field in metric.materialized_fields:

                user_history_columns[field.metric_name] = field.data_type
                if field.totals:
                    user_history_columns[f'{field.metric_name}_total'] = field.data_type

                select_expressions.append(SQL_TEMPLATE_SELECT_EXPRESSION.format(
                    expression=field.select_expression,
                    data_type=field.data_type,
                    metric_name=field.metric_name
                ))

                if field.user_history_formula is not None:
                    insert_old_select.append(
                        field.user_history_formula.format(
                            prev_value=f'user_history.{field.metric_name}',
                            new_value=f'{metric.table_name}_temp.{field.metric_name}'
                        ) + f' AS {field.metric_name},'
                    )
                else:
                    insert_old_select.append(SQL_TEMPLATE_INSERT_OLD_SELECT.format(
                        table_name=metric.table_name,
                        metric_name=field.metric_name,
                        app_id=app_id
                    ))

                if field.totals:
                    insert_old_select.append(SQL_TEMPLATE_INSERT_OLD_SELECT_TOTAL.format(
                        table_name=metric.table_name,
                        metric_name=field.metric_name,
                        app_id=app_id
                    ))

                insert_new_select.append(SQL_TEMPLATE_INSERT_NEW_SELECT.format(
                    table_name=metric.table_name,
                    metric_name=field.metric_name,
                    app_id=app_id
                ))

                if field.totals:
                    insert_new_select.append(SQL_TEMPLATE_INSERT_NEW_SELECT_TOTAL.format(
                        table_name=metric.table_name,
                        metric_name=field.metric_name,
                        app_id=app_id
                    ))

            insert_old_table.append(SQL_TEMPLATE_INSERT_OLD_TABLE.format(
                table_name=metric.table_name,
                app_id=app_id
            ))

            insert_new_table.append(SQL_TEMPLATE_NEW_TABLE.format(
                table_name=metric.table_name,
                app_id=app_id
            ))

            temp_tables.append(SQL_TEMPLATE_TEMP_TABLE_FOR_METRIC.format(
                    table_name=metric.table_name,
                    select_clause=''.join(select_expressions),
                    project_id=project_id,
                    app_id=app_id,
                    dataset=metric.dataset
            ))

    temp_tables = '\n'.join(temp_tables)
    insert_old_select = ''.join(insert_old_select)
    insert_new_select = ''.join(insert_new_select)
    insert_old_table = ''.join(insert_old_table)
    insert_new_table = ''.join(insert_new_table)

    user_history_columns_struct = ','.join([f'(\'{column_name}\', \'{data_type}\')' for column_name, data_type in user_history_columns.items()])
    user_history_columns = ','.join([f'{column_name}' for column_name in user_history_columns.keys()])

    user_history_query = user_history_query.format(
        temp_tables=temp_tables,
        insert_old_select=insert_old_select,
        insert_old_table=insert_old_table,
        insert_new_select=insert_new_select,
        insert_new_table=insert_new_table,
        user_history_columns_struct=user_history_columns_struct,
        user_history_columns=user_history_columns,
        execution_date=execution_date,
        project_id=project_id,
        app_id=app_id
    )

    return user_history_query