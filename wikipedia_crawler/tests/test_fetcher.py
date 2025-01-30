import unittest
import pika
import json
from time import sleep
from threading import Thread
from pymongo import MongoClient
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.abspath('../src'))
from fetcher.fetcher import Fetcher
from queue_service.queue_client import QueueClient
from storage.html_storage import HTMLStorage
from storage.metadata_db import MetadataRepository

class TestFetcherWithDocker(unittest.TestCase):
    def setUp(self):
        self.queue_client = QueueClient(host='localhost')
        self.storage = HTMLStorage(base_path='tests/test_html_storage')
        self.metadata_repo = MetadataRepository(
            mongo_uri='mongodb://root:example@localhost:27017'
        )
        self.fetcher = Fetcher(
            queue_client=self.queue_client,
            storage=self.storage,
            metadata_repo=self.metadata_repo
        )
        
        # Clear test data
        self.metadata_repo.collection.delete_many({})

    def test_fetcher_saves_metadata(self):
        test_url = "https://en.wikipedia.org/wiki/Main_Page"
        self.fetcher.fetch(test_url)
        
        # Verify metadata was saved
        metadata = self.metadata_repo.find_by_url(test_url)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['url'], test_url)
        self.assertTrue('file_path' in metadata)

    def tearDown(self):
        self.metadata_repo.client.close()

if __name__ == "__main__":
    unittest.main()
