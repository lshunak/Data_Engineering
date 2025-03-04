"""
Elasticsearch client utility module.
This module provides basic functions to interact with Elasticsearch.
"""
from elasticsearch import Elasticsearch
import pandas as pd
import json
from typing import Dict, List, Any, Optional


class ElasticsearchClient:
    def __init__(self, host: str = "localhost", port: int = 9200):
        """Initialize Elasticsearch client connection"""
        self.es = Elasticsearch([f"http://{host}:{port}"])
        self.check_connection()

    def check_connection(self) -> bool:
        """Check if connection to Elasticsearch is successful"""
        if self.es.ping():
            print("Connected to Elasticsearch!")
            return True
        else:
            print("Could not connect to Elasticsearch!")
            return False
            
    def get_cluster_health(self) -> Dict[str, Any]:
        """Get cluster health information"""
        return self.es.cluster.health()
        
    def create_index(self, index_name: str, mappings: Optional[Dict] = None) -> Dict:
        """Create a new index with optional mappings"""
        if not mappings:
            mappings = {}
        return self.es.indices.create(index=index_name, body=mappings)
        
    def delete_index(self, index_name: str) -> Dict:
        """Delete an index"""
        return self.es.indices.delete(index=index_name)
        
    def index_document(self, index_name: str, document: Dict, doc_id: Optional[str] = None) -> Dict:
        """Index a document into Elasticsearch"""
        return self.es.index(index=index_name, body=document, id=doc_id)
        
    def bulk_index(self, index_name: str, documents: List[Dict]) -> Dict:
        """Bulk index multiple documents"""
        bulk_data = []
        for doc in documents:
            # Add index action
            bulk_data.append({"index": {"_index": index_name}})
            # Add document data
            bulk_data.append(doc)
        return self.es.bulk(body=bulk_data)
    
    def search(self, index_name: str, query: Dict) -> Dict:
        """Search documents in an index"""
        return self.es.search(index=index_name, body=query)
    
    def to_dataframe(self, search_results: Dict) -> pd.DataFrame:
        """Convert Elasticsearch search results to pandas DataFrame"""
        hits = search_results.get('hits', {}).get('hits', [])
        data = [hit['_source'] for hit in hits]
        return pd.DataFrame(data)