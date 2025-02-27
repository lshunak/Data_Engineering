import boto3
import logging
import os
from typing import List, Dict, Any
import pandas as pd
import json
from decimal import Decimal

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_dynamodb(use_local: bool = False):  # Changed default to False
    """Get DynamoDB resource"""
    try:
        if use_local:
            # Local DynamoDB with persistence
            dynamodb = boto3.resource('dynamodb',
                endpoint_url='http://localhost:8000',
                region_name='eu-north-1',
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
            
            # Test connection and create tables if they don't exist
            dynamodb.meta.client.list_tables()
            logger.info("Connected to local DynamoDB")
            
            # Ensure the container keeps running
            os.system('docker container ls | grep dynamodb-local > /dev/null || '
                     'docker start dynamodb-local')
            
            return dynamodb
        else:
            # AWS DynamoDB
            dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
            dynamodb.meta.client.list_tables()
            logger.info("Connected to AWS DynamoDB")
            return dynamodb
            
    except Exception as e:
        logger.error(f"Error connecting to DynamoDB: {str(e)}")
        return None

def create_table(dynamodb, table_name: str) -> bool:
    """Create DynamoDB table if not exists"""
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},  # Partition key
                {'AttributeName': 'group', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': 'group', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.wait_until_exists()
        logger.info(f"Created table {table_name}")
        return True
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        logger.info(f"Table {table_name} already exists")
        return True
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
        return False

def load_csv_to_dynamodb(s3_client, dynamodb, bucket: str, file_key: str) -> bool:
    """Load CSV data from S3 to DynamoDB"""
    try:
        # Read CSV from S3 with more robust error handling
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(
            response['Body'],
            on_bad_lines='skip',  # Skip problematic lines
            encoding='utf-8'
        )
        
        # Log DataFrame info for debugging
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"DataFrame shape: {df.shape}")
        
        # Get table name from file
        table_name = file_key.split('/')[-1].replace('.csv', '').lower()
        table = dynamodb.Table(table_name)
        
        # Convert DataFrame to DynamoDB items
        with table.batch_writer() as batch:
            for index, row in df.iterrows():
                try:
                    # Convert pandas row to dict and handle Decimal
                    item = json.loads(
                        json.dumps(row.to_dict()), 
                        parse_float=Decimal
                    )
                    
                    # Clean up any NaN values
                    item = {k: v for k, v in item.items() if pd.notna(v)}
                    
                    # Add unique id and set group field while keeping original
                    item['id'] = f"{item.get('song_id', 'unknown')}_{index}"
                    
                    # Keep original field and set group for DynamoDB sort key
                    if 'age_group' in item:
                        item['group'] = item['age_group']
                    elif 'time_period' in item:
                        item['group'] = item['time_period']
                        
                    batch.put_item(Item=item)
                    
                except Exception as item_error:
                    logger.warning(f"Error processing row {index}: {str(item_error)}")
                    continue
                
        logger.info(f"Successfully loaded {len(df)} items into {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading file {file_key}: {str(e)}")
        return False

def scan_table(dynamodb, table_name: str) -> None:
    """Scan and display items from a DynamoDB table"""
    try:
        table = dynamodb.Table(table_name)
        response = table.scan()
        items = response.get('Items', [])
        
        logger.info(f"\nTable: {table_name}")
        logger.info(f"Total items: {len(items)}")
        
        if items:
            # Display first 5 items as sample
            logger.info("Sample items (first 5):")
            for item in items[:5]:
                logger.info(json.dumps(item, indent=2, default=str))
                
        return len(items)
        
    except Exception as e:
        logger.error(f"Error scanning table {table_name}: {str(e)}")
        return 0

def preview_csv(s3_client, bucket: str, file_key: str) -> None:
    """Preview CSV file content"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        # Read first few lines
        content = response['Body'].read(1024).decode('utf-8')
        logger.info(f"\nPreviewing first 1KB of {file_key}:")
        logger.info(content)
    except Exception as e:
        logger.error(f"Error previewing file {file_key}: {str(e)}")

def test_connection():
    """Test DynamoDB connection"""
    try:
        dynamodb = get_dynamodb(use_local=False)
        if not dynamodb:
            return False
            
        # List tables
        tables = list(dynamodb.tables.all())
        logger.info(f"Found {len(tables)} tables: {[t.name for t in tables]}")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

def main():
    """Main function to load OLAP data to DynamoDB"""
    if not test_connection():
        return 1

    # Initialize AWS clients
    try:
        s3_client = boto3.client('s3')
        dynamodb = get_dynamodb()
        if not dynamodb:
            return 1
            
        # Get S3 bucket
        bucket = os.environ.get('S3_BUCKET', 'lshunak-airflow-dags')
        
        # List OLAP files
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix='OLAP_data/'
        )
        
        files = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].endswith('.csv')
        ]
        
        if not files:
            logger.error("No CSV files found")
            return 1
            
        # Process each file
        success_count = 0
        for file_key in files:
            # Preview CSV content first
            preview_csv(s3_client, bucket, file_key)
            
            table_name = file_key.split('/')[-1].replace('.csv', '').lower()
            
            # Create table
            if create_table(dynamodb, table_name):
                # Load data
                if load_csv_to_dynamodb(s3_client, dynamodb, bucket, file_key):
                    success_count += 1
        
        # After loading, scan tables to verify
        logger.info("\nVerifying loaded data:")
        for file_key in files:
            table_name = file_key.split('/')[-1].replace('.csv', '').lower()
            items_count = scan_table(dynamodb, table_name)
            logger.info(f"Table {table_name} contains {items_count} items")
            
        return 0 if success_count == len(files) else 1
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        return 1

if __name__ == "__main__":
    import sys
    # Set use_local to False in test_connection
    def test_connection():
        return get_dynamodb(use_local=False) is not None
    sys.exit(main())