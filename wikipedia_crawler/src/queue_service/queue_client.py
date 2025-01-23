import pika
import json
from typing import Callable, Any

class QueueClient:
    def __init__(self, host: str = 'localhost'):
        self.host = host
        self.connection = None
        self.channel = None
        self._connect()
        
    def _connect(self) -> None:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        
    def publish(self, queue: str, message: Any) -> None:
        self.channel.declare_queue(queue)  # Ensure queue exists before publishing
        try:
            if self.channel.is_open:
                self.channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))
            else:
                raise Exception("Channel is not open.")
        except Exception as e:
            print(f"Error during publishing to {queue}: {e}")
        
    def consume(self, queue: str, callback: Callable) -> None:
        self.channel.queue_declare(queue=queue)
        print(f"Queue {queue} declared")
        
        self.channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=True
        )
        self.channel.start_consuming()
        
    def close(self) -> None:
        if self.connection:
            self.connection.close()
            
    def __enter__(self):
        self._connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()