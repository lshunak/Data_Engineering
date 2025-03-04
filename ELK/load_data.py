"""
Script to load Spotify songs data into Elasticsearch
"""
import pandas as pd
import time
import logging
from elasticsearch import Elasticsearch
from spotify_mappings import get_spotify_mappings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_spotify_data(file_path, index_name="spotify_songs", recreate_index=True, chunk_size=1000):
    """Load Spotify songs data from CSV into Elasticsearch"""
    logger.info(f"Loading Spotify songs data from {file_path} into index '{index_name}'")
    
    # Connect to Elasticsearch
    es = Elasticsearch(["http://localhost:9200"])
    if not es.ping():
        logger.error("Failed to connect to Elasticsearch. Make sure it's running.")
        return False
    
    # Delete index if it exists and recreate is True
    if recreate_index and es.indices.exists(index=index_name):
        logger.info(f"Deleting existing index '{index_name}'...")
        es.indices.delete(index=index_name)
    
    # Create index with mappings
    if not es.indices.exists(index=index_name):
        logger.info(f"Creating index '{index_name}' with mappings...")
        mappings = get_spotify_mappings()
        es.indices.create(index=index_name, body=mappings)
    
    # Load and process data in chunks
    logger.info("Loading data from CSV...")
    start_time = time.time()
    
    # Read data in chunks to handle large dataset
    total_rows = 0
    chunk_num = 0
    
    # Process each chunk
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk_num += 1
        
        # Clean data - replace NaN with None for JSON compatibility
        chunk = chunk.where(pd.notna(chunk), None)
        
        # Convert to records for Elasticsearch
        records = chunk.to_dict(orient='records')
        total_rows += len(records)
        
        # Prepare bulk actions
        actions = []
        for record in records:
            # Use track_id as document ID for deduplication
            actions.append({"index": {"_index": index_name, "_id": record["track_id"]}})
            actions.append(record)
        
        # Execute bulk operation
        if actions:
            es.bulk(body=actions)
        
        logger.info(f"Processed chunk {chunk_num}: {len(records)} records")
    
    # Refresh index to make documents searchable immediately
    es.indices.refresh(index=index_name)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Completed loading {total_rows} records in {elapsed_time:.2f} seconds")
    
    # Get index stats
    stats = es.indices.stats(index=index_name)
    doc_count = stats["indices"][index_name]["total"]["docs"]["count"]
    logger.info(f"Total documents in index: {doc_count}")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load Spotify songs data into Elasticsearch")
    parser.add_argument("file_path", help="Path to the Spotify songs CSV file")
    parser.add_argument("--index", default="spotify_songs", help="Name of the Elasticsearch index")
    parser.add_argument("--no-recreate", action="store_false", dest="recreate", 
                        help="Do not recreate the index if it exists")
    parser.add_argument("--chunk-size", type=int, default=1000, 
                        help="Number of records to process at once")
    
    args = parser.parse_args()
    
    load_spotify_data(
        file_path=args.file_path,
        index_name=args.index,
        recreate_index=args.recreate,
        chunk_size=args.chunk_size
    )

