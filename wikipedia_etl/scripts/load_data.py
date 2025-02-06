import json
import mysql.connector
from datetime import datetime
import logging
import yaml
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config() -> dict:
    """Load configuration from yaml file."""
    try:
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise

def create_tables(cursor) -> None:
    """Create necessary tables if they don't exist."""
    try:
        # Create articles table (metadata only)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                file_path VARCHAR(255),
                processed_date DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create categories table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            )
        """)

        # Create article_categories table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS article_categories (
                article_id INT,
                category_id INT,
                PRIMARY KEY (article_id, category_id),
                FOREIGN KEY (article_id) REFERENCES articles(id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        """)
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def insert_article_metadata(cursor, title: str, file_path: str, 
                          processed_date: str) -> int:
    """Insert article metadata and return its ID."""
    try:
        query = """
            INSERT INTO articles (title, file_path, processed_date)
            VALUES (%s, %s, %s)
        """
        cursor.execute(query, (title, file_path, processed_date))
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Error inserting article metadata: {e}")
        raise

def insert_categories(cursor, categories: List[str]) -> Dict[str, int]:
    """Insert categories if they don't exist and return their IDs."""
    category_ids = {}
    for category in categories:
        try:
            cursor.execute("""
                INSERT IGNORE INTO categories (name)
                VALUES (%s)
            """, (category,))
            
            cursor.execute("SELECT id FROM categories WHERE name = %s", (category,))
            category_ids[category] = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error processing category {category}: {e}")
            continue
    return category_ids

def link_article_categories(cursor, article_id: int, category_ids: List[int]) -> None:
    """Link article with its categories."""
    try:
        query = """
            INSERT INTO article_categories (article_id, category_id)
            VALUES (%s, %s)
        """
        cursor.executemany(query, [(article_id, cat_id) for cat_id in category_ids])
    except Exception as e:
        logger.error(f"Error linking article {article_id} with categories: {e}")
        raise

def load_processed_data(file_path: str) -> List[Dict[str, Any]]:
    """Load processed data from JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading processed data: {e}")
        raise

def main():
    """Main function to load metadata into MySQL."""
    try:
        # Load configuration
        config = load_config()
        mysql_config = config['mysql']

        # Connect to MySQL
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor()

        # Create tables if they don't exist
        create_tables(cursor)

        # Load processed data
        processed_data = load_processed_data('processed_data/processed_data_20250205_153221.json')  # Update with your actual filename
        
        # Process each article
        for item in processed_data:
            try:
                metadata = item['metadata']
                
                # Insert article metadata
                article_id = insert_article_metadata(
                    cursor,
                    metadata['title'],
                    metadata['file_path'],
                    metadata['processed_date']
                )

                # Insert categories and get their IDs
                category_ids = insert_categories(cursor, metadata['categories'])

                # Link article with categories
                link_article_categories(cursor, article_id, list(category_ids.values()))

                # Commit after each article
                connection.commit()
                
                logger.info(f"Successfully processed metadata for article: {metadata['title']}")

            except Exception as e:
                logger.error(f"Error processing article {metadata.get('title', 'Unknown')}: {e}")
                connection.rollback()
                continue

        logger.info("Metadata loading completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    main()