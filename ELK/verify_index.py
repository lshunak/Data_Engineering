"""
Simple script to verify Elasticsearch index for Spotify data
"""
from elasticsearch import Elasticsearch
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger()

def verify_spotify_index(index_name="spotify_songs"):
    """Verify that Spotify data was loaded correctly"""
    # Connect to Elasticsearch
    es = Elasticsearch(["http://localhost:9200"])
    if not es.ping():
        logger.error("Failed to connect to Elasticsearch")
        return False
    
    # Check if index exists
    if not es.indices.exists(index=index_name):
        logger.error(f"Index '{index_name}' does not exist")
        return False
    
    # Get index count
    count = es.count(index=index_name)
    logger.info(f"Total documents in index {index_name}: {count['count']}")
    
    # Display mapping structure
    mapping = es.indices.get_mapping(index=index_name)
    fields = mapping[index_name]["mappings"]["properties"].keys()
    logger.info(f"Index fields: {', '.join(sorted(fields))}")
    
    # Show some basic stats
    agg_query = {
        "size": 0,
        "aggs": {
            "genre_count": {
                "terms": {
                    "field": "track_genre",
                    "size": 10
                }
            },
            "popularity_stats": {
                "stats": {
                    "field": "popularity"
                }
            },
            "avg_duration": {
                "avg": {
                    "field": "duration_ms"
                }
            }
        }
    }
    
    results = es.search(index=index_name, body=agg_query)
    
    # Display genre distribution
    print("\n--- Genre Distribution (Top 10) ---")
    for bucket in results["aggregations"]["genre_count"]["buckets"]:
        print(f"{bucket['key']}: {bucket['doc_count']} songs")
    
    # Display popularity stats
    pop_stats = results["aggregations"]["popularity_stats"]
    avg_duration_ms = results["aggregations"]["avg_duration"]["value"]
    avg_duration_min = avg_duration_ms / (1000 * 60)
    
    print("\n--- Statistics ---")
    print(f"Average Song Popularity: {pop_stats['avg']:.2f}")
    print(f"Minimum Popularity: {pop_stats['min']}")
    print(f"Maximum Popularity: {pop_stats['max']}")
    print(f"Average Song Duration: {avg_duration_min:.2f} minutes")
    
    # Sample a few records
    sample_query = {
        "size": 3,
        "sort": [
            {"popularity": {"order": "desc"}}
        ]
    }
    
    sample_results = es.search(index=index_name, body=sample_query)
    
    print("\n--- Sample Records (Most Popular) ---")
    for hit in sample_results["hits"]["hits"]:
        doc = hit["_source"]
        print(f"\nTrack: {doc['track_name']}")
        print(f"Artist: {doc['artists']}")
        print(f"Album: {doc['album_name']}")
        print(f"Genre: {doc['track_genre']}")
        print(f"Popularity: {doc['popularity']}")
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify Spotify data in Elasticsearch")
    parser.add_argument("--index", default="spotify_songs", help="Name of the Elasticsearch index")
    
    args = parser.parse_args()
    
    verify_spotify_index(args.index)