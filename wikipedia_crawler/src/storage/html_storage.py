import os
import hashlib

class HTMLStorage:
    def __init__(self, base_path='html_storage'):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def save(self, html_content, url):
        """Save HTML content to a file."""
        file_name = self._generate_file_name(url)
        file_path = os.path.join(self.base_path, file_name)

        os.makedirs(self.base_path, exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(html_content)
        return file_path

    def _generate_file_name(self, url):
        """Generate a unique file name based on the URL."""
        return url.replace('https://', '').replace('http://', '').replace('/', '_') + '.html'
