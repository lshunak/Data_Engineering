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



    def start(self):
        """Start the Filter to consume links."""
        logging.info("Filter started.")

        self.queue_client.declare_queue('filter_queue')
        self.queue_client.declare_queue('fetcher_queue')
        self.queue_client.consume('filter_queue', self.on_message)

    def on_message(self, body):
        """Process the incoming links."""
        logging.info("Received links for filtering.")
        link = body
        if not self.redis_client.exists(link):     
            self.redis_client.set(link, 'processed')
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