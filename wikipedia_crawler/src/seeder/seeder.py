from queue_service.queue_client import QueueClient
import os
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def seed_queues():

    try:
        with QueueClient(host=os.getenv('RABBITMQ_HOST', 'localhost')) as queue_client:
            seed_url = "https://en.wikipedia.org/wiki/Main_Page"
            logger.info(f"Seeding URL: {seed_url}")
            queue_client.publish('fetcher_queue', seed_url)
            sys.exit(0)
    except Exception as e:
        logger.error(f"Error seeding URL: {e}")
        sys.exit(1)
    finally:
        queue_client.close()

if __name__ == "__main__":
    seed_queues()
