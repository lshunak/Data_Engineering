import unittest
from unittest.mock import MagicMock
from filter.filter import Filter  # Adjust the import based on your project structure

class TestFilter(unittest.TestCase):
    
    def setUp(self):
        """Setup for Filter class test cases"""
        # Create mock objects for QueueClient and Redis client
        self.mock_queue_client = MagicMock()
        self.mock_redis_client = MagicMock()

        # Instantiate the Filter class with mocked clients
        self.filter = Filter(self.mock_queue_client, self.mock_redis_client)

    def test_filter_links_new_links(self):
        """Test the filtering logic for new links"""
        links = [
            "https://en.wikipedia.org/wiki/Page1",
            "https://en.wikipedia.org/wiki/Page2"
        ]

        # Mock the Redis exists call to return False (indicating new links)
        self.mock_redis_client.exists.return_value = False
        
        # Mock Redis set call to return True (simulate adding links to Redis)
        self.mock_redis_client.set = MagicMock()

        new_links = self.filter.filter_links(links)

        # Assert that both links are considered new and added to the list
        self.assertEqual(new_links, links)
        self.mock_redis_client.set.assert_called_with("https://en.wikipedia.org/wiki/Page2", 'processed')

    def test_filter_links_existing_links(self):
        """Test filtering when all links are already processed"""
        links = [
            "https://en.wikipedia.org/wiki/Page1",
            "https://en.wikipedia.org/wiki/Page2"
        ]

        # Mock the Redis exists call to return True (indicating all links are processed)
        self.mock_redis_client.exists.return_value = True
        
        new_links = self.filter.filter_links(links)

        # Assert that no new links are returned (all are processed)
        self.assertEqual(new_links, [])
        self.mock_redis_client.set.assert_not_called()  # No Redis set calls should be made

    def test_on_message(self):
        """Test the on_message method (message consumption)"""
        # Mock a queue message with comma-separated links
        body = "https://en.wikipedia.org/wiki/Page1,https://en.wikipedia.org/wiki/Page2"

        # Mock the Redis client behavior to simulate new links
        self.mock_redis_client.exists.side_effect = [False, False]  # Both links are new

        # Call the on_message method
        self.filter.on_message(body)

        # Check that the new links are published to the fetcher queue
        self.mock_queue_client.publish.assert_called_with('fetcher_queue', "https://en.wikipedia.org/wiki/Page2")

    def test_no_new_links_to_publish(self):
        """Test when no new links are available to publish"""
        # Mock a queue message with comma-separated links
        body = "https://en.wikipedia.org/wiki/Page1,https://en.wikipedia.org/wiki/Page2"

        # Mock the Redis client behavior to simulate all links already processed
        self.mock_redis_client.exists.side_effect = [True, True]  # All links exist in Redis

        # Call the on_message method
        self.filter.on_message(body)

        # Assert that no new links are published to the fetcher queue
        self.mock_queue_client.publish.assert_not_called()

if __name__ == '__main__':
    unittest.main()
