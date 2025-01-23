from queue_service.queue_client import QueueClient

def seed_queues():
    queue_client = QueueClient(host='localhost')
    queue_client.declare_queue('fetcher_queue')
    seed_urls = ["https://en.wikipedia.org/wiki/Main_Page"]
    for url in seed_urls:
        print(f"Seeding URL: {url}")
        queue_client.publish('fetcher_queue', url)

if __name__ == "__main__":
    seed_queues()
