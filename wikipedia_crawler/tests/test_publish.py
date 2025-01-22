from queue_service.queue_client import QueueClient

# Initialize the queue client
queue_client = QueueClient(host='localhost')

# Publish a test message
test_queue = 'test_queue'
test_message = {'message': 'Hello, RabbitMQ!'}

queue_client.publish(queue=test_queue, message=test_message)
print(f"Message published to queue '{test_queue}': {test_message}")

# Close the connection
queue_client.close()
