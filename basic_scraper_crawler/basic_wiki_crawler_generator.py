import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Generator, Set

def wiki_crawler(start_url: str, max_depth: int = 2) -> Generator[str, None, None]:
    """
    Generator that yields Wikipedia links up to specified depth
    """
    visited: Set[str] = set()
    
    def extract_links(url: str, depth: int) -> Generator[str, None, None]:
        if depth <= 0 or url in visited:
            return
            
        try:
            visited.add(url)
            print(f"Visiting {url} at depth {max_depth - depth}")
            
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find valid Wikipedia links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/wiki/') and ':' not in href:
                    full_url = urljoin('https://en.wikipedia.org', href)
                    if full_url not in visited:
                        yield full_url
                        # Recursively get links from found URLs
                        yield from extract_links(full_url, depth - 1)
                        
        except Exception as e:
            print(f"Error processing {url}: {e}")
            
    yield from extract_links(start_url, max_depth)

if __name__ == '__main__':
    # Test the generator
    start_url = "https://en.wikipedia.org/wiki/Python_(programming_language)"
    for url in wiki_crawler(start_url, max_depth=2):
        print(f"Found: {url}")