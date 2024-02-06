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
from google.cloud.bigquery import QueryJobConfig
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
arg_game_name: str = args[ARG_GAME_NAME]
arg_temp_gcp_bucket: str = args[ARG_TEMP_GCP_BUCKET]


# User input
BUCKET_NAME = arg_datalocker_bucket_name
GAME_NAME = arg_game_name

# System/terraform input
TEMPORARY_GCS_BUCKET = arg_temp_gcp_bucket

DATE_FILTER_TEMPLATE = "dt={dates}/"
BULK_FILTER = "b={1,2,3,4}/"
BIGQUERY_DATASET = "{game_name}_external".format(game_name=GAME_NAME)
PATH_PREFFIX = "gs://{}/cost_etl/v1/".format(BUCKET_NAME)
TABLES_PREFIX = "af_cost_"


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
    elif field_type == "BIGINT":
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
            new_schema.append(bigquery.SchemaField(key, value, mode="NULLABLE"))
            
        table.schema = new_schema        
        table = client.update_table(table, ['schema'])  # Make an API request.
        logger.info(
            "Updated table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    
    return table

def bigquery_query(query_string: str):
    """
    Generic BigQuery query
    :param query_string: Query string
    :return: Query results as pandas dataframe
    """
    job_config = QueryJobConfig(use_query_cache=True)
    job_config.labels = {
        'app_id': GAME_NAME,
        'environment': 'production',
        'service': 'pyspark_etl',
    }
    query_job = client.query(query_string, job_config=job_config)

    query_job.result()  # Waits for job to complete.
    return query_job.to_dataframe()

def modify_column_type(df, column_name):
    """
    Modifies column type in dataframe
    :param df: Dataframe
    :param column_name: Column name
    :return: Dataframe with modified column type
    """
    integer_columns = ['bid_amount', 'original_bid_amount', 'impressions', 'clicks', 'reported_impressions', 
                    'reported_clicks', 'installs', 'reported_conversions', 're_engagements', 're_attributions',
                    'impressions_discrepancy', 'clicks_discrepancy', 'installs_discrepancy',
                     'video_25p_views', 'video_50p_views','video_75p_views', 'video_completions']

    float_columns = ['cost', 'original_cost','ctr', 'cvr', 'ecpm', 'cpi', 'ccvr','cvvr', 'reported_cvr', 'ecpc',
                    'cost_without_fees', 'original_cost_without_fees', 'fees']
    
    if column_name in integer_columns:
        return df.withColumn(column_name,col(column_name).cast(IntegerType()))
    elif column_name in float_columns:
        return df.withColumn(column_name,col(column_name).cast(FloatType()))
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
    DATES_FILTER = get_dates_filter(arg_date_from, arg_date_to)

    BASE_PATH = PATH_PREFFIX + "t=" + report_type + "/"
    FILE_PATH = PATH_PREFFIX + "t=" + report_type + "/" + DATE_FILTER_TEMPLATE.format(dates="{" + DATES_FILTER + "}")

    BASE_PATH = PATH_PREFFIX
    FILE_PATH = PATH_PREFFIX + DATE_FILTER_TEMPLATE.format(dates="{" + DATES_FILTER + "}") + "/" + BULK_FILTER + report_type    

    logger.info(f"Load report {report_type} for days {DATES_FILTER}")

    # try:
    df_load = spark.read.option("delimiter", ",") \
        .option("basePath", BASE_PATH) \
        .option("quoteAll","true") \
        .option("escape", "\"") \
        .option("compression","gzip") \
        .parquet(FILE_PATH)            
    # except Exception as e:
    #     logger.info(f"Error loading report: {report_type}")
    #     logger.info(f'{e}')
    #     continue

    logger.info(f"Loaded report: {report_type}")

    logger.info(f"Filter app_id: {report_type}")
    df_load = df_load.filter(df_load.app_id.isin(arg_app_ids))

    df_load.createOrReplaceTempView("datalocker_report")
    df_load = spark.sql("""
    WITH cte AS (
        SELECT date, dt AS dt, b, ROW_NUMBER() OVER(PARTITION BY date ORDER BY (dt, b) DESC) AS row_number FROM datalocker_report GROUP BY 1, 2, 3 ORDER BY 1, 2, 3 DESC
    ), dates_filtered AS (
        SELECT * FROM cte WHERE row_number = 1
    )
    SELECT datalocker_report.* 
    FROM datalocker_report
    INNER JOIN dates_filtered
    ON datalocker_report.date = dates_filtered.date
    AND datalocker_report.dt = dates_filtered.dt
    AND datalocker_report.b = dates_filtered.b
    """).withColumnRenamed("date", "date_")

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

    logger.info(f"Find distinct dates")
    # Find distinct dates
    dist_dates = df_load.select("date_").distinct().collect()
    dist_dates_str = ', '.join('"{}"'.format(item.date_) for item in dist_dates)

    # Create query to delete distinct dates
    logger.info(f"delete dates {dist_dates_str} from table {table_id}")
    delete_query = "DELETE FROM `{}.{}.{}` WHERE date_ IN ({})".format(table.project, table.dataset_id, table.table_id, dist_dates_str)
    bigquery_query(delete_query)

    logger.info(f"write data to temp table: {table_id}")
    df_load.write.format('com.google.cloud.spark.bigquery') \
        .option('temporaryGcsBucket', TEMPORARY_GCS_BUCKET) \
        .mode('overwrite') \
        .save(f'{table.project}.{table.dataset_id}.temp_{table.table_id}')

    logger.info(f"copy data to final destination: {table_id}")

    """
    FIX:
    Since spark dataframe is not able to write to bigquery table, we have to use temporary table.
    It's because dt field is nullable in source spark DataFrame, but BigQuery requires it to be not nullable.
    """
    table_columns = ', '.join('`{}`'.format(key) for key, value in table_scheme_source.items())

    query_string = f"INSERT INTO `{table.project}.{table.dataset_id}.{table.table_id}` ({table_columns})" \
                   f"SELECT {table_columns} FROM `{table.project}.{table.dataset_id}.temp_{table.table_id}`"
                    
    bigquery_query(query_string)

    logger.info(f"delete temp table: {table_id}")
    query_string = f"DROP TABLE `{table.project}.{table.dataset_id}.temp_{table.table_id}`"
    bigquery_query(query_string)

    logger.info(f"Done: {report_type}")
