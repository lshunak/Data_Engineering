from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.operators.hdfs import HdfsPutFileOperator
from airflow.hooks.mysql_hook import MySqlHook  # Changed from MySqlOperator
from datetime import datetime, timedelta
import json
from bs4 import BeautifulSoup
import os

# ... (previous task definitions remain the same) ...

@task
def load_to_mysql(inserts):
    """Load data to MySQL using PyMySQL"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    for insert in inserts:
        mysql_hook.run(insert)
    return True

@dag(
    dag_id='wikipedia_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Process HTML files to JSON/MySQL and text to HDFS'
)
def wikipedia_etl():
    # Extract and transform HTML to JSON
    json_file = extract_transform()
    
    # Extract text for HDFS
    txt_file = extract_text(json_file)
    
    # Prepare and load MySQL metadata
    mysql_inserts = prepare_mysql_metadata(json_file)
    mysql_load = load_to_mysql(mysql_inserts)
    
    # Load text to HDFS
    upload_to_hdfs = HdfsPutFileOperator(
        task_id='upload_txt_to_hdfs',
        local_file=txt_file,
        remote_file=f"/wikipedia/articles/{datetime.now().strftime('%Y/%m/%d')}/data.txt",
        hdfs_conn_id='hdfs_default'
    )
    
    # Set dependencies
    json_file >> [txt_file, mysql_inserts]
    txt_file >> upload_to_hdfs
    mysql_inserts >> mysql_load

# Create DAG instance
dag = wikipedia_etl()