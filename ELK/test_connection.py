"""
Simple script to test connection to Elasticsearch.
"""
from es_client import ElasticsearchClient
import json

def main():
    # Initialize the client - will automatically test connection during init
    client = ElasticsearchClient()
    
    # Get cluster info and print it
    info = client.es.info()
    print("\nElasticsearch Information:")
    print(f"Cluster Name: {info.get('cluster_name')}")
    print(f"Elasticsearch Version: {info.get('version', {}).get('number')}")
    
    # Get cluster health
    health = client.get_cluster_health()
    print("\nCluster Health:")
    print(f"Status: {health.get('status')}")
    print(f"Number of Nodes: {health.get('number_of_nodes')}")
    print(f"Active Shards: {health.get('active_shards')}")
    
    # List all indices - FIX: Use keyword argument instead of positional
    try:
        indices = client.es.indices.get(index='*')
        print("\nAvailable Indices:")
        if indices:
            for index_name in indices:
                print(f"- {index_name}")
        else:
            print("No indices found.")
    except Exception as e:
        print(f"\nNote: Could not fetch indices: {e}")
        print("This is likely because there are no indices yet in your fresh Elasticsearch instance.")

    print("\nConnection test completed successfully.")

if __name__ == "__main__":
    main()