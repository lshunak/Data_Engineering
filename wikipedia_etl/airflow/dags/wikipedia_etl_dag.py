from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess
import time
# Add scripts directory to path

# Import the actual processing functions from our existing scripts
from scripts.html_to_hdfs import HTMLProcessor
from scripts.html_to_mysql import process_files, load_config
from pyspark.sql import SparkSession

# Constants from current context
TIMESTAMP = "2025-02-10 14:51:54"
USER = "lshunak"
INPUT_DIR = "/home/liran/Documents/data_engineering/wikipedia_etl/tests/data"
HDFS_BASE = "hdfs://localhost:9000/user/lshunak/wikipedia"

SCRIPTS_DIR = "/home/liran/Documents/data_engineering/wikipedia_etl/scripts"
MANAGE_HADOOP_SCRIPT = os.path.join(SCRIPTS_DIR, "manage_hadoop.sh")

def check_hadoop_services():
    """Check if Hadoop services are running and start them if needed"""
    try:
        # Check Hadoop status
        status_cmd = f"{MANAGE_HADOOP_SCRIPT} status"
        status = subprocess.run(status_cmd, shell=True, capture_output=True, text=True)
        
        if "Hadoop services are not running" in status.stdout:
            # Start Hadoop services
            start_cmd = f"{MANAGE_HADOOP_SCRIPT} start"
            subprocess.run(start_cmd, shell=True, check=True)
            time.sleep(30)  # Wait for services to fully start
            
        return True
    except Exception as e:
        raise Exception(f"Failed to manage Hadoop services: {str(e)}")


def hdfs_task(**context):
    """Task that uses HTMLProcessor class"""
    try:
        processor = HTMLProcessor(
            input_dir=INPUT_DIR,
            hdfs_base=HDFS_BASE,
            current_timestamp=TIMESTAMP,
            current_user=USER
        )
        
        processed_files = processor.process_all_files()
        return processed_files
    
    finally:
        if 'processor' in locals():
            processor.cleanup()

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
    # Task to ensure Hadoop is running
    ensure_hadoop = PythonOperator(
        task_id='ensure_hadoop_running',
        python_callable=check_hadoop_services,
        dag=dag
    )

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
    ensure_hadoop >> hdfs_task