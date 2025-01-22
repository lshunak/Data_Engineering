import unittest
from unittest.mock import MagicMock
from parser.parser import Parser
from queue_service.queue_client import QueueClient

class TestParser(unittest.TestCase):
    def setUp(self):
        self.queue_client = MagicMock(QueueClient)
        self.parser = Parser(queue_client=self.queue_client)

    def test_parse(self):
        # Test HTML content
        html_content = '<a href="https://en.wikipedia.org/wiki/Test">Test</a>'
        result = self.parser.parse(html_content)
        self.assertEqual(result, ["https://en.wikipedia.org/wiki/Test"])

    def test_on_message(self):
        # Simulate receiving HTML content
        html_content = '<a href="https://en.wikipedia.org/wiki/Test">Test</a>'
        
        # Call on_message with the test content
        self.parser.on_message(html_content)

        # Assert publish was called with the parsed Wikipedia links
        self.queue_client.publish.assert_called_with('filter_queue', ["https://en.wikipedia.org/wiki/Test"])

if __name__ == "__main__":
    unittest.main()
