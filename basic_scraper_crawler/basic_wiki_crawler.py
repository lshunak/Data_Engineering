import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from typing import Set, List

class WikiScraper:
    def __init__(self, base_url: str = "https://en.wikipedia.org"):
        self.base_url = base_url
        self.visited_urls: Set[str] = set()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Educational-Bot/1.0)'
        }
    
    def get_internal_links(self, url: str) -> List[str]:
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all internal Wikipedia links
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/wiki/') and ':' not in href:
                    full_url = urljoin(self.base_url, href)
                    links.append(full_url)
            
            return list(set(links))  # Remove duplicates
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return []

    def crawl(self, start_url: str, max_depth: int = 2):
        if max_depth <= 0 or start_url in self.visited_urls:
            return

        print(f"Crawling: {start_url}")
        self.visited_urls.add(start_url)
        
        # Get links from current page
        links = self.get_internal_links(start_url)
        print(f"Found {len(links)} links on {start_url}")
        
        # Rate limiting
        time.sleep(1)
        
        # Recursively crawl found links
        for link in links:
            self.crawl(link, max_depth - 1)

if __name__ == '__main__':
    scraper = WikiScraper()
    start_url = "https://en.wikipedia.org/wiki/Python_(programming_language)"
    scraper.crawl(start_url, max_depth=2)
    
    print("\nCrawling complete!")
    print(f"Total pages visited: {len(scraper.visited_urls)}")
    for url in sorted(scraper.visited_urls):
        print(url)