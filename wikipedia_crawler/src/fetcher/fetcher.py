import requests
from queue_service.queue_client import QueueClient
from bs4 import BeautifulSoup
from storage.html_storage import HTMLStorage
from storage.metadata_db import MetadataRepository
import os
import logging

class Fetcher:
    def __init__(self, queue_client: QueueClient, storage=HTMLStorage, metadata_repo=MetadataRepository):
        self.queue_client = queue_client
        self.storage = storage
        self.metadata_repo = metadata_repo

    def fetch(self, url):
        """Fetch HTML content from the URL and send links to the parser"""
        print(f"Fetching URL: {url}")
        html_content = self._fetch_html(url)
        if html_content:
            # Save the HTML (optional)
            file_path = self.storage.save(html_content, url)
            self.metadata_repo.save(url, file_path)

            # Extract links and send to Parser
            links = self._extract_links(html_content)
 
            print(f"Publishing to parser_queue: {links[1:2]}")  # Add this line
            self.queue_client.publish('parser_queue', links)


    def _fetch_html(self, url):
        """Fetch HTML content from the URL"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def _extract_links(self, html):
        """Extract links from the HTML content"""
        soup = BeautifulSoup(html, 'html.parser')
        links = [a['href'] for a in soup.find_all('a', href=True)]
        return links

    def start(self):
        """Start the Fetcher to listen for URLs"""
        
        self.queue_client.consume('fetcher_queue', self.on_message)

    def on_message(self, channel, method, properties, body):
        """Process the incoming seed URLs."""
        url = body.decode('utf-8').strip('"') # Decode the message
        print(f"Processing URL: {url}")
        self.fetch(url)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        queue_client = QueueClient(host=os.getenv('RABBITMQ_HOST', 'localhost'))
        storage = HTMLStorage(base_path='/app/html_storage')
        metadata_repo = MetadataRepository(
            mongo_uri=os.getenv('MONGO_URI', 'mongodb://root:example@localhost:27017')
        )
        fetcher = Fetcher(queue_client, storage, metadata_repo)
        logger.info("Fetcher starting...")
        fetcher.start()
    except Exception as e:
        logger.error(f"Error starting Fetcher: {e}")
        raise
if __name__ == "__main__":
    main()