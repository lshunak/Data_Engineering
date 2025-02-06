"""
Wikipedia ETL Workflow DAG using the implemented extract_transform.py, load_data.py, and load_to_hdfs.py scripts
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import json
import mysql.connector
from scripts.extract_transform import load_config, extract_html_files, process_file
from scripts.load_data import insert_article_metadata, insert_categories, link_article_categories
from scripts.load_to_hdfs import HDFSLoader

# Default DAG arguments
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 6),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def extract_and_transform():
    """Extract and transform HTML files using implemented functions"""
    config = load_config()
    html_directory = config['paths']['html_storage']
    
    # Get list of HTML files
    html_files = extract_html_files(html_directory)
    
    processed_data = []
    for file_path in html_files:
        text, metadata = process_file(file_path)
        processed_data.append({
            'text': text,
            'metadata': metadata
        })
    
    # Save processed data
    output_dir = 'processed_data'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'{output_dir}/processed_data_{timestamp}.json'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)
        
    return {'output_file': output_file, 'count': len(processed_data)}

@task
def load_to_mysql(processed_data_file):
    """Load metadata to MySQL using implemented functions"""
    config = load_config()
    mysql_config = config['mysql']
    
    connection = mysql.connector.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()
    
    try:
        with open(processed_data_file, 'r') as f:
            data = json.load(f)
            
        for item in data:
            metadata = item['metadata']
            
            # Insert article metadata
            article_id = insert_article_metadata(
                cursor,
                metadata['title'],
                metadata['file_path'],
                metadata['processed_date']
            )
            
            # Insert categories and link them
            category_ids = insert_categories(cursor, metadata['categories'])
            link_article_categories(cursor, article_id, list(category_ids.values()))
            
            connection.commit()
            
        return True
    finally:
        connection.close()

@task
def load_to_hdfs(processed_data_file):
    """Load data to HDFS using implemented HDFSLoader"""
    config = load_config()
    hdfs_config = config['hdfs']
    
    hdfs_loader = HDFSLoader(
        hdfs_url=hdfs_config['url'],
        hdfs_user=hdfs_config['user']
    )
    
    hdfs_path = f"/tests/data/{datetime.now().strftime('%Y/%m/%d')}/data.json"
    success = hdfs_loader.upload_file(
        local_path=processed_data_file,
        hdfs_path=hdfs_path,
        overwrite=True
    )
    
    if not success:
        raise Exception("Failed to upload file to HDFS")
    
    return hdfs_path

@dag(
    dag_id='wikipedia_etl',
    default_args=default_args,
    description='Wikipedia ETL workflow using implemented scripts',
    schedule_interval=None,
    catchup=False,
    tags=['wikipedia', 'etl']
)
def wikipedia_etl_workflow():
    # Extract and transform
    extract_result = extract_and_transform()
    
    # Load to MySQL and HDFS in parallel
    mysql_task = load_to_mysql(extract_result['output_file'])
    hdfs_task = load_to_hdfs(extract_result['output_file'])
    
    # Set dependencies
    extract_result >> [mysql_task, hdfs_task]

# Create DAG instance
dag = wikipedia_etl_workflow()