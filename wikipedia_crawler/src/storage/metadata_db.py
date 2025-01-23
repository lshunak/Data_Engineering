from pymongo import MongoClient
from datetime import datetime

class MetadataRepository:
    def __init__(self, mongo_uri='mongodb://root:example@localhost:27017', db_name='crawler'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db.metadata
        # Test connection on init
        self.client.admin.command('ping')

    def save(self, url, file_path):
        """Save metadata about a fetched page."""
        metadata = {
            'url': url,
            'file_path': file_path,
            'timestamp': datetime.now(),
        }
        self.collection.insert_one(metadata)
        return metadata

    def find_by_url(self, url):
        """Retrieve metadata by URL."""
        return self.collection.find_one({'url': url})
