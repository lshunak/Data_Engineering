from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import logging
import os
import json
import yaml
from datetime import datetime

# Constants
CURRENT_TIMESTAMP = "2025-02-10 14:39:50"
CURRENT_USER = "lshunak"
INPUT_DIR = "/home/liran/Documents/data_engineering/wikipedia_etl/tests/data"
CONFIG_PATH = "/home/liran/Documents/data_engineering/wikipedia_etl/config/config.yaml"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as file:
        config = yaml.safe_load(file)
    return config['mysql']

def extract_metadata(html_content):
    """Extract metadata from HTML content"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract URL
        url = ""
        if soup.find('link', rel='canonical'):
            url = soup.find('link', rel='canonical')['href']
        
        # Extract page name
        page_name = soup.title.string.strip() if soup.title else ""
        
        # Extract categories
        categories = []
        cat_div = soup.find('div', {'id': 'mw-normal-catlinks'})
        if cat_div:
            categories = [cat.text.strip() for cat in cat_div.find_all('a') 
                        if cat.text != "Categories"]
        
        # Count words in main content
        main_content = soup.find('div', {'id': 'mw-content-text'})
        word_count = 0
        if main_content:
            for tag in main_content(['script', 'style', 'sup', 'ref']):
                tag.decompose()
            text = main_content.get_text()
            word_count = len(text.split())

        return (
            url,
            page_name,
            json.dumps(categories),
            word_count,
            CURRENT_TIMESTAMP,
            CURRENT_USER
        )
    except Exception as e:
        logger.error(f"Error extracting metadata: {str(e)}")
        return ('', '', '[]', 0, CURRENT_TIMESTAMP, CURRENT_USER)

def save_to_mysql(rows, mysql_config):
    """Save data to MySQL"""
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        query = """
        INSERT INTO metadata 
        (url, page_name, categories, word_count, processed_timestamp, processed_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(query, rows)
        conn.commit()
        logger.info(f"Saved {len(rows)} records to MySQL")
        
    except Error as e:
        logger.error(f"Error saving to MySQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def process_files(spark, input_dir, mysql_config):
    """Process HTML files and save metadata"""
    try:
        # Read HTML files
        html_files = []
        for f in os.listdir(input_dir):
            if f.endswith('.html'):
                with open(os.path.join(input_dir, f), 'r') as file:
                    html_files.append((f, file.read()))
        
        if not html_files:
            logger.warning("No HTML files found")
            return

        # Create RDD and process files
        rdd = spark.sparkContext.parallelize(html_files)
        metadata_rdd = rdd.map(lambda x: extract_metadata(x[1]))
        
        # Define schema for DataFrame
        schema = StructType([
            StructField("url", StringType(), True),
            StructField("page_name", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("word_count", IntegerType(), True),
            StructField("processed_timestamp", StringType(), True),
            StructField("processed_by", StringType(), True)
        ])
        
        # Create DataFrame and collect results
        metadata_df = spark.createDataFrame(metadata_rdd, schema)
        rows = metadata_df.collect()
        
        # Save to MySQL
        save_to_mysql(rows, mysql_config)
        
    except Exception as e:
        logger.error(f"Error processing files: {e}")
        raise

def main():
    try:
        # Initialize Spark
        spark = SparkSession.builder \
            .appName("WikiMetadataProcessor") \
            .master("local[*]") \
            .getOrCreate()
        
        # Load MySQL config
        mysql_config = load_config()
        
        # Process files
        process_files(spark, INPUT_DIR, mysql_config)
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()