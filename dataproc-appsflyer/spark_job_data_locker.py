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

from http.client import TEMPORARY_REDIRECT
from ntpath import join
from numpy import array
from pyspark.sql import SparkSession
import argparse
import datetime
from datetime import date, timedelta
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType,BooleanType,DateType,TimestampType,IntegerType,FloatType,DoubleType
from pyspark.sql.functions import *
from google.cloud import bigquery
from pyspark.sql.functions import when, lit, col

from google.cloud import bigquery

APP_NAME = "appsflyer_extract"
# Arguments
ARG_REPORTS = "reports"
ARG_DATE_FROM = "date_from"
ARG_DATE_TO = "date_to"
ARG_APPSFLYER_APP_IDS = "app_ids"
ARG_GCP_PROJECT_ID = "gcp_project_id"
ARG_BIGQUERY_PROJECT_ID = "bigquery_project_id"
ARG_DATALOCKER_BUCKET_NAME = "bucket_name"
ARG_DATALOCKER_HOME_FOLDER = "home_folder"
ARG_GAME_NAME = "game_name"
ARG_TEMP_GCP_BUCKET = "temp_gcp_bucket"


#Set Spark session
spark = SparkSession.builder \
        .appName(APP_NAME) \
        .enableHiveSupport() \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

#Set logger
log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
logger = log_4j_logger.LogManager.getLogger(APP_NAME)

#Set script arguments
parser: argparse.ArgumentParser = argparse.ArgumentParser()

parser.add_argument(
    f"--{ARG_REPORTS}",
    dest=ARG_REPORTS,
    nargs='+',
    required=True,
    help='Reports to extract'    
)

parser.add_argument(
    f"--{ARG_DATE_FROM}",
    dest=ARG_DATE_FROM,
    required=True,
    help='Date from which to extract data'
)

parser.add_argument(
    f"--{ARG_DATE_TO}",
    dest=ARG_DATE_TO,
    required=True,
    help='Date to which to extract data'
)

parser.add_argument(
    f"--{ARG_APPSFLYER_APP_IDS}",
    dest=ARG_APPSFLYER_APP_IDS,
    nargs='+',
    required=True,
    help='App ids to filter data'
)

parser.add_argument(
    f"--{ARG_GCP_PROJECT_ID}",
    dest=ARG_GCP_PROJECT_ID,
    required=True,
    help='GCP project id'
)

parser.add_argument(
    f"--{ARG_BIGQUERY_PROJECT_ID}",
    dest=ARG_BIGQUERY_PROJECT_ID,
    required=True,
    help='BigQuery project id'
)

parser.add_argument(
    f"--{ARG_DATALOCKER_BUCKET_NAME}",
    dest=ARG_DATALOCKER_BUCKET_NAME,
    required=True,
    help='Bucket name set in AppsFlyer DataLocker'
)

parser.add_argument(
    f"--{ARG_DATALOCKER_HOME_FOLDER}",
    dest=ARG_DATALOCKER_HOME_FOLDER,
    required=True,
    help='Home folder set in AppsFlyer DataLocker'
)

parser.add_argument(
    f"--{ARG_GAME_NAME}",
    dest=ARG_GAME_NAME,
    required=True,
    help='Name of the game'
)

parser.add_argument(
    f"--{ARG_TEMP_GCP_BUCKET}",
    dest=ARG_TEMP_GCP_BUCKET,
    required=True,
    help='Temporary GCP bucket for executing job'
)

args : Dict[str, Any] = parser.parse_args()
args = vars(args)

logger.info(args)

def validate_date(date_text:str):
    """
    Validates date format
    :param date_text: date in string format
    :return: Date if date is valid, throw exception otherwise
    """
    try:        
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return date.fromisoformat(date_text)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

#Set arguments in variables
arg_report: array = args[ARG_REPORTS]
arg_date_from: date = validate_date(args[ARG_DATE_FROM])
arg_date_to: date = validate_date(args[ARG_DATE_TO])
arg_app_ids: array = args[ARG_APPSFLYER_APP_IDS]
arg_gcp_project_id: str = args[ARG_GCP_PROJECT_ID]
arg_bigquery_project_id: str = args[ARG_BIGQUERY_PROJECT_ID]
arg_datalocker_bucket_name: str = args[ARG_DATALOCKER_BUCKET_NAME]
arg_datalocker_home_folder: str = args[ARG_DATALOCKER_HOME_FOLDER]
arg_game_name: str = args[ARG_GAME_NAME]
arg_temp_gcp_bucket: str = args[ARG_TEMP_GCP_BUCKET]


# User input
BUCKET_NAME = arg_datalocker_bucket_name
HOME_FOLDER = arg_datalocker_home_folder
GAME_NAME = arg_game_name

# System/terraform input
TEMPORARY_GCS_BUCKET = arg_temp_gcp_bucket

DATE_FILTER_TEMPLATE = "dt={dates}/"
BIGQUERY_DATASET = "{game_name}_external".format(game_name=GAME_NAME)
PATH_PREFFIX = "gs://{}/{}/data-locker-hourly/".format(BUCKET_NAME, HOME_FOLDER)
TABLES_PREFIX = "af_"

#Set BigQuery client
client = bigquery.Client(project=arg_gcp_project_id)

def create_table(table_id):
    """
    Creates table in BigQuery
    :param table_id: Table id
    :return: None 
    """
    schema = [
        bigquery.SchemaField("dt", "DATE", mode="REQUIRED")
    ]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field='dt')

    table = client.create_table(table)  # Make an API request.
    logger.info(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    return table

def get_table(table_id):    
    """
    Gets table from BigQuery
    :param table_id: Table id
    :return: Table if exists, None otherwise
    """
    try:
        table = client.get_table(table_id)  # Make an API request.
        return table
    except:        
        return None

def get_or_create_table(table_id):    
    """
    Get or create table in BigQuery
    :param table_id: Table id
    :return: Table if exists, None otherwise
    """
    table = get_table(table_id)
    if not table:
        table = create_table(table_id)
    return table

def map_field_type_spark_to_bigquery(field_type):
    """
    Maps field type from Spark to BigQuery
    :param field_type: Field type from Spark
    :return: Field type from BigQuery
    """
    if field_type == "INT":
        return "INT64"
    else:
        return field_type

def map_field_type_bigquery(field_type):
    """
    Maps field type from BigQuery to Spark
    :param field_type: Field type from BigQuery
    :return: Field type from Spark
    """
    if field_type == "INTEGER":
        return "INT64"
    else:
        return field_type

def get_data_type_by_column_name(column_name: str):
    """
    Gets data type by column name
    :param column_name: Column name
    :return: Data type
    """
    if column_name == "is_retargeting":
        return "BOOLEAN"
    elif column_name == "device_download_time":
        return "TIMESTAMP"
    elif column_name == "event_time":
        return "TIMESTAMP"
    elif column_name == "install_time":
        return "TIMESTAMP"
    elif column_name == "is_lat":
        return "BOOLEAN"
    elif column_name == "wifi":
        return "BOOLEAN"
    elif column_name == "store_reinstall":
        return "BOOLEAN"
    elif column_name == "attributed_touch_time":
        return "TIMESTAMP"
    elif column_name == "contributor1_touch_time":
        return "TIMESTAMP"
    elif column_name == "contributor2_touch_time":
        return "TIMESTAMP"
    elif column_name == "contributor3_touch_time":
        return "TIMESTAMP"
    elif column_name == "is_primary_attribution":
        return "BOOLEAN"
    elif column_name == "is_receipt_validated":
        return "BOOLEAN"
    elif column_name == "gp_click_time":
        return "TIMESTAMP"
    elif column_name == "gp_install_begin":
        return "TIMESTAMP"
    else:
        return "STRING"

def update_table_schema(table_scheme_source : dict, table):
    """
    Updates table schema in BigQuery
    :param table_scheme_source: Table scheme from source
    :param table: Table to update
    :return: table with updated schema
    """
    table_scheme_destination    = {}
    
    for column in table.schema:
        table_scheme_destination[column.name] = map_field_type_bigquery(column.field_type)

    set_destination = set(table_scheme_destination.items())
    set_source = set(table_scheme_source.items())

    dict_columns_to_add = dict(set_source - set_destination)
    dict_columns_to_add = dict(sorted(dict_columns_to_add.items()))

    if len(dict_columns_to_add) > 0:
        new_schema = table.schema
        for key, value in dict_columns_to_add.items():
            new_schema.append(bigquery.SchemaField(key, get_data_type_by_column_name(key), mode="NULLABLE"))
            
        table.schema = new_schema        
        table = client.update_table(table, ['schema'])  # Make an API request.
        logger.info(
            "Updated table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    
    return table

def delete_current_data(date_from, date_to, table):
    """
    Deletes current data from BigQuery
    :param date_from: Date from
    :param date_to: Date to
    :param table: Table to delete data from
    :return: None
    """
    query_string = "DELETE FROM `{}.{}.{}` WHERE dt BETWEEN '{}' AND '{}'".format(table.project, table.dataset_id, table.table_id, date_from, date_to)
    logger.info(query_string)
    query_job = client.query(query_string)

    query_job.result()  # Waits for job to complete.

def bigquery_query(query_string):
    """
    Generic BigQuery query
    :param query_string: Query string
    :return: Query results as pandas dataframe
    """
    query_job = client.query(query_string)

    query_job.result()  # Waits for job to complete.
    return query_job.to_dataframe()

def modify_column_type(df, column_name):
    """
    Modifies column type in dataframe
    :param df: Dataframe
    :param column_name: Column name
    :return: Dataframe with modified column type
    """
    if column_name == "is_retargeting":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "device_download_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "event_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "install_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "is_lat":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "wifi":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "store_reinstall":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "attributed_touch_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "contributor1_touch_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "contributor2_touch_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "contributor3_touch_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "is_primary_attribution":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "is_receipt_validated":
        return df.withColumn(column_name,col(column_name).cast(BooleanType()))
    elif column_name == "gp_click_time":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "gp_install_begin":
        return df.withColumn(column_name,col(column_name).cast(TimestampType()))
    elif column_name == "h":
        return df.withColumn(column_name,col(column_name).cast(StringType()))
    else:
        return df
        
def get_dates_filter(date_from, date_to) -> str:
    """
    Gets dates filter for query
    :param date_from: Date from
    :param date_to: Date to
    :return: Dates filter
    """
    dates_array = [str(arg_date_from + timedelta(days=x)) for x in range((arg_date_to-arg_date_from).days + 1)]
    return ','.join(dates_array)

#Execude reports
logger.info("Validate dates")

if arg_date_from > arg_date_to:
    raise ValueError("Date from must be before date to")

logger.info("Load and Write reports")

#Iterate over reports and load and write to BigQuery
for report_type in arg_report:
    if report_type in ['attributed_ad_revenue', 'retargeting_ad_revenue', 'organic_ad_revenue']:
        temp_date_from = arg_date_from - timedelta(days=1)
        DATES_FILTER = get_dates_filter(temp_date_from, arg_date_to)
    else:
        DATES_FILTER = get_dates_filter(arg_date_from, arg_date_to)

    BASE_PATH = PATH_PREFFIX + "t=" + report_type + "/"
    FILE_PATH = PATH_PREFFIX + "t=" + report_type + "/" + DATE_FILTER_TEMPLATE.format(dates="{" + DATES_FILTER + "}")

    logger.info(f"Load report {report_type} for dates {DATES_FILTER}")

    try:
        df_load = spark.read.option("delimiter", ",") \
            .option("basePath", BASE_PATH) \
            .option("quoteAll","true") \
            .option("escape", "\"") \
            .csv(FILE_PATH, header=True, nullValue="null")
    except Exception as e:
        logger.info(f"Error loading report: {report_type}")
        logger.info(f'{e}')
        continue

    logger.info(f"Loaded report: {report_type}")

    logger.info(f"Filter app_id: {report_type}")
    df_load = df_load.filter(df_load.app_id.isin(arg_app_ids))

    logger.info(f"Prepare schame diff: {report_type}")
    for k,v in dict(df_load.dtypes).items():
        df_load = modify_column_type(df_load, k)

    table_scheme_source = dict(df_load.dtypes)
    for k,v in table_scheme_source.items():
        table_scheme_source.update({k: map_field_type_spark_to_bigquery(v.upper())})

    table_id = f'{arg_bigquery_project_id}.{BIGQUERY_DATASET}.{TABLES_PREFIX}{report_type}'

    logger.info(f"get or create table: {table_id}")
    table = get_or_create_table(table_id)

    logger.info(f"update table schema: {table_id}")
    update_table_schema(table_scheme_source, table)

    logger.info(f"delete current data: {table_id}")
    delete_current_data(arg_date_from, arg_date_to, table)
    
    logger.info(f"write data to temp table: {table_id}")
    df_load.write.format('com.google.cloud.spark.bigquery') \
        .option('temporaryGcsBucket', TEMPORARY_GCS_BUCKET) \
        .mode('overwrite') \
        .save(f'{arg_bigquery_project_id}.{BIGQUERY_DATASET}.temp_{TABLES_PREFIX}{report_type}')

    logger.info(f"copy data to final destination: {table_id}")

    """
    FIX:
    Since spark dataframe is not able to write to bigquery table, we have to use temporary table.
    It's because dt field is nullable in source spark DataFrame, but BigQuery requires it to be not nullable.
    """
    table_columns = ', '.join('`{}`'.format(key) for key, value in table_scheme_source.items())

    query_string = f"INSERT INTO `{arg_bigquery_project_id}.{BIGQUERY_DATASET}.{TABLES_PREFIX}{report_type}` ({table_columns})" \
                   f"SELECT {table_columns} FROM `{arg_bigquery_project_id}.{BIGQUERY_DATASET}.temp_{TABLES_PREFIX}{report_type}`"
                    
    bigquery_query(query_string)

    logger.info(f"delete temp table: {table_id}")
    query_string = f"DROP TABLE `{arg_bigquery_project_id}.{BIGQUERY_DATASET}.temp_{TABLES_PREFIX}{report_type}`"
    bigquery_query(query_string)

    logger.info(f"Done: {report_type}")
