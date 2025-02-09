"""
Wikipedia ETL Workflow DAG using the implemented extract_transform.py, load_data.py, and load_to_hdfs.py scripts
"""

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import os
import json
from scripts.extract_transform import extract_html_files, process_file
from scripts.load_data import insert_article_metadata, insert_categories, link_article_categories
from scripts.load_to_hdfs import HDFSLoader  # Use your existing implementation

# Get absolute paths
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(DAG_DIR)

# Default DAG arguments
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 6),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_config():
    """Load configuration from yaml file"""
    import yaml
    import os

    # Get the project root directory (two levels up from the DAG file)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    
    config_path = os.path.join(project_root, 'config', 'config.yaml')
    
    # Add debug logging
    print(f"Looking for config file at: {config_path}")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
        
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
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
    output_dir = os.path.join(BASE_DIR, 'processed_data')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f'processed_data_{timestamp}.json')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)
        
    return {'output_file': output_file, 'count': len(processed_data)}

@task
def load_to_mysql(processed_data_file):
    """Load metadata to MySQL using Airflow's MySqlHook"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    connection = mysql_hook.get_conn()
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
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
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

@task
def extract_text_for_hdfs(processed_data_file):
    """Extract text content from JSON and save as TXT for HDFS"""
    try:
        # Read JSON data
        with open(processed_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract texts
        all_texts = [article['text'] for article in data]
        combined_text = '\n\n---\n\n'.join(all_texts)
        
        # Save as txt file
        txt_file = processed_data_file.replace('.json', '.txt')
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(combined_text)
            
        return txt_file
    except Exception as e:
        raise Exception(f"Failed to extract text content: {str(e)}")

@task
def load_text_to_hdfs(txt_file):
    """Load text file to HDFS"""
    config = load_config()
    hdfs_config = config['hdfs']
    
    hdfs_loader = HDFSLoader(
        hdfs_url=hdfs_config['url'],
        hdfs_user=hdfs_config['user']
    )
    
    # Create HDFS path with timestamp
    timestamp = datetime.now().strftime('%Y/%m/%d')
    hdfs_path = f"/user/{hdfs_config['user']}/wikipedia/text/{timestamp}/content.txt"
    
    success = hdfs_loader.upload_file(
        local_path=txt_file,
        hdfs_path=hdfs_path,
        overwrite=True
    )
    
    if not success:
        raise Exception(f"Failed to upload text file to HDFS: {txt_file}")
        
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
    
    # Extract text content
    txt_file = extract_text_for_hdfs(extract_result['output_file'])
    
    # Load to MySQL and HDFS in parallel
    mysql_task = load_to_mysql(extract_result['output_file'])
    hdfs_json_task = load_to_hdfs(extract_result['output_file'])
    hdfs_text_task = load_text_to_hdfs(txt_file)
    
    # Set dependencies
    extract_result >> txt_file
    extract_result >> [mysql_task, hdfs_json_task]
    txt_file >> hdfs_text_task

# Create DAG instance
dag = wikipedia_etl_workflow()