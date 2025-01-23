import pika
import json
from typing import Callable, Any
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueueClient:
    def __init__(self, host: str = 'localhost'):
        self.host = host
        self.connection = None
        self.channel = None
        self._connect()
        
    def _connect(self) -> None:
        retries = 5
        delay = 5

        for attempt in range(retries):
            try:
                logger.info(f"Connecting to RabbitMQ at {self.host}")
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.host, connection_attempts=3, retry_delay=delay))
                self.channel = self.connection.channel()
                logger.info("Connected to RabbitMQ")
                break
            except Exception as e:
                if attempt < retries - 1:  # Don't sleep on the last attempt
                    logger.warning(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{retries}): {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"Error connecting to RabbitMQ: {e}")
                    raise

    def declare_queue(self, queue: str) -> None:
        try:
            logger.info(f"Declaring queue: {queue}")
            self.channel.queue_declare(queue=queue, durable=True)
            logger.info(f"Declared queue: {queue}")
        except Exception as e:
            logger.error(f"Error declaring queue: {e}")
            raise

    def publish(self, queue: str, message: Any) -> None:
        try:
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
            elif not isinstance(message, str):
                message = str(message)

            self.declare_queue(queue)
            logger.info(f"Declared queue: {queue}")
            
            if not self.channel.is_open:
                raise Exception("Channel is closed")

            self.channel.basic_publish(
                exchange='', 
                routing_key=queue, 
                body=message, 
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Published message to {queue}")
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            raise
        
    def consume(self, queue: str, callback: Callable) -> None:
        try:
            self.channel.queue_declare(queue=queue, durable=True)
            logger.info(f"Declared queue: {queue}")
            
            def wrapped_callback(ch, method, properties, body):
                try:
                    decoded_body = json.loads(body)
                except json.JSONDecodeError:
                    decoded_body = body.decode('utf-8')
                try:
                    callback(decoded_body)
                except Exception as e:
                    logger.error(f"Error in callback: {str(e)}")
            
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=wrapped_callback,
                auto_ack=True
            )
            logger.info(f"Started consuming messages from: {queue}")
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            raise

    def close(self) -> None:
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Closed connection to RabbitMQ")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()