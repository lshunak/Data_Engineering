import re
import os
import logging
import json
from queue_service.queue_client import QueueClient

class Parser:
    def __init__(self, queue_client: QueueClient):
        self.queue_client = queue_client
    
    @staticmethod
    def is_wikipedia_url(url: str) -> bool:
        """
        Check if the URL is a valid Wikipedia link.
        """
        wikipedia_regex = re.compile(r'https?://(?:[a-z]{2,3}\.)?wikipedia\.org/wiki/.*')
        return bool(wikipedia_regex.match(url))

    def filter_wikipedia_links(self, urls: list) -> list:
        """Filter and validate Wikipedia links"""
        if not urls:
            logger.warning(f"Received empty URLs: {urls}")
            return []
        
        # Filter only Wikipedia links
        wikipedia_links = []
        for link in urls:
            if not isinstance(link, str):
                logger.debug(f"Skipping non-string URL: {link}")
                continue
                
            # Clean the link
            link = link.strip()
            
            # Convert relative links to absolute
            if link.startswith('/wiki/'):
                link = f"https://en.wikipedia.org{link}"
            
            # Check if it's a valid Wikipedia article link
            if self.is_wikipedia_url(link):
                wikipedia_links.append(link)
                logger.debug(f"Valid Wikipedia link found: {link}")
            else:
                logger.debug(f"Filtered out link: {link}")

        logger.info(f"Filtered {len(urls)} URLs to {len(wikipedia_links)} valid Wikipedia links")
        return list(set(wikipedia_links))  # Remove duplicates

    def start(self):
        """Start the Parser to consume URLs"""
        self.queue_client.declare_queue('parser_queue')
        self.queue_client.declare_queue('filter_queue')
        logger.info("Starting to consume from parser_queue")
        self.queue_client.consume('parser_queue', self.on_message)

    def on_message(self, body):
        """Process the incoming URLs"""
        try:
            logger.info(f"Received message of type: {type(body)}")
            logger.debug(f"Raw message content: {body[:200] if isinstance(body, (str, bytes)) else str(body)[:200]}...")  # Log start of message
    
            # Handle different input types
            if isinstance(body, bytes):
                decoded_body = body.decode('utf-8')
                logger.debug(f"Decoded from bytes: {decoded_body[:200]}...")
            else:
                decoded_body = body
                logger.debug(f"Body directly: {decoded_body[:200]}...")

            # Parse URLs from the message
            try:
                if isinstance(decoded_body, str):
                    urls = json.loads(decoded_body)
                    logger.debug(f"Decoded JSON: {urls[:5]}...")
                else:
                    urls = decoded_body
                    logger.info(f"Body is not a string, assuming it's a list")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                urls = [decoded_body] if isinstance(decoded_body, str) else []

            if not isinstance(urls, list):
                urls = [urls]

            logger.info(f"Processing {len(urls)} URLs")
            
            # Filter and process Wikipedia links
            wikipedia_links = self.filter_wikipedia_links(urls)
            
            if wikipedia_links:
                logger.info(f"Found {len(wikipedia_links)} valid Wikipedia links")
                for link in wikipedia_links:
                    logger.info(f"Publishing to filter_queue: {link}")
                    self.queue_client.publish('filter_queue', link)
            else:
                logger.warning("No valid Wikipedia links found in message")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            logger.debug(f"Sample of URLs that were filtered out: {urls[:5]}")

# Set the root logger to DEBUG level.
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Explicitly set the logger level to DEBUG
logger.setLevel(logging.DEBUG)

def main():
    try:
        queue_client = QueueClient(host=os.getenv('RABBITMQ_HOST', 'localhost'))
        parser = Parser(queue_client)
        logger.info("Starting Parser...")
        parser.start()
    except Exception as e:
        logger.error(f"Error starting Parser: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()