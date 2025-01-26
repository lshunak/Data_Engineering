import requests
from bs4 import BeautifulSoup
import re

def get_words_by_size(url):
    """
    Retrieves all words from a web page and returns them sorted by word size.

    :param url: The URL of the page to scrape
    :return: List of words sorted by size (longest to shortest)
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        text = soup.get_text()

        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())

        sorted_words = sorted(set(words), key=len, reverse=True)
        
        return sorted_words

    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []
    except Exception as e:
        print(f"Error processing page: {e}")
        return []

if __name__ == '__main__':
    url = "https://en.wikipedia.org/wiki/David_Lynch"
    words = get_words_by_size(url)
    
    if words:
        print("\nWords sorted by length (longest to shortest):")
        for word in words:
            print(f"{word}: {len(word)} characters")
    else:
        print("No words found")
