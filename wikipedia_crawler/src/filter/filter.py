from queue_service.queue_client import QueueClient
import logging
import os
import redis
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class Filter:
    def __init__(self, queue_client: QueueClient, redis_client):
        self.queue_client = queue_client
        self.redis_client = redis_client

    def filter_links(self, links):
        """Check if links have been processed already and add new ones."""
        new_links = []
        for link in links:
            if not self.redis_client.exists(link):
                new_links.append(link)
                self.redis_client.set(link, 'processed')
                # Log which links are being set
                print(f"Setting link {link} as processed")
        return new_links

    def start(self):
        """Start the Filter to consume links."""
        logging.info("Filter started.")
        self.queue_client.consume('filter_queue', self.on_message)

    def on_message(self, body):
        """Process the incoming links."""
        logging.info("Received links for filtering.")
        links = body.split(',')  # Simplified for demonstration
        new_links = self.filter_links(links)
        if new_links:
            logging.info(f"Publishing {len(new_links)} new links to fetcher_queue.")
            for link in new_links:
                self.queue_client.publish('fetcher_queue', link)
        else:
            logging.info("No new links to publish.")

def main():
    try:
        queue_client = QueueClient(host=os.getenv('RABBITMQ_HOST', 'localhost'))
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379))
        )
        filter_service = Filter(queue_client, redis_client)
        logger.info("Starting Filter...")
        filter_service.start()
    except Exception as e:
        logger.error(f"Error starting Filter: {e}")
        raise
    
if __name__ == "__main__":
    main()