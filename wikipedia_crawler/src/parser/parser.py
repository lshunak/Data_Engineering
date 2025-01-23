import re
import os
import logging
from queue_service.queue_client import QueueClient

class Parser:
    def __init__(self, queue_client: QueueClient):
        self.queue_client = queue_client

    def parse(self, html_content):
        """Extract Wikipedia links from HTML content"""
        if not html_content:
            return []
        
        # Find all Wikipedia links in the HTML
        wikipedia_links = re.findall(r'href="(https://en.wikipedia.org/wiki/[^"]+)', html_content)
        return wikipedia_links

    def start(self):
        """Start the Parser to consume HTML"""
        
        self.queue_client.consume('parser_queue', self.on_message)

    def on_message(self, body):
        """Process the incoming HTML content"""
        html_content = body  # Already decoded by QueueClient
        wikipedia_links = self.parse(html_content)
        if wikipedia_links:
            print(f"Publishing {len(wikipedia_links)} links to filter_queue.")
            self.queue_client.publish('filter_queue', wikipedia_links)
        else:
            print("No links to publish.")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        queue_client = QueueClient(host=os.getenv('RABBITMQ_HOST', 'localhost'))
        parser = Parser(queue_client)
        logger.info("Starting Parser...")
        parser.start()
    except Exception as e:
        logger.error(f"Error starting Parser: {e}")
        raise

if __name__ == "__main__":
    main()