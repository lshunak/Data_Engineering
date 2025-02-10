from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to path

# Import the actual processing functions from our existing scripts
from scripts.html_to_hdfs import process_files as hdfs_process
from scripts.html_to_mysql import process_files, load_config
from pyspark.sql import SparkSession

# Constants from current context
TIMESTAMP = "2025-02-10 14:51:54"
USER = "lshunak"
INPUT_DIR = "/home/liran/Documents/data_engineering/wikipedia_etl/tests/data"

def hdfs_task(**context):
    """Task that uses our existing HDFS processing function"""
    return hdfs_process()

def mysql_task(**context):
    """Task that uses our existing MySQL processing function"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("WikiMetadataProcessor") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Get MySQL config and process
        mysql_config = load_config()
        process_files(spark, INPUT_DIR, mysql_config)
    finally:
        spark.stop()

# DAG default arguments
default_args = {
    'owner': USER,
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
with DAG(
    'wikipedia_metadata_pipeline',
    default_args=default_args,
    description='Pipeline to process Wikipedia HTML files to HDFS and MySQL',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # Task to move files to HDFS
    hdfs_task = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=hdfs_task,
        provide_context=True,
        dag=dag
    )

    # Task to process and store in MySQL
    mysql_task = PythonOperator(
        task_id='process_to_mysql',
        python_callable=mysql_task,
        provide_context=True,
        dag=dag
    )

    # Set task dependencies
    hdfs_task >> mysql_task