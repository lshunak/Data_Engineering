import requests
from bs4 import BeautifulSoup

def get_alternative_languages(wikipedia_url):
    """
    Retrieves all alternative languages for a given Wikipedia page.

    :param wikipedia_url: URL of the Wikipedia page
    :return: Dictionary where keys are language codes and values are language names
    """
    try:
        print(f"Fetching URL: {wikipedia_url}")  # Debug print
        # Send a GET request to the Wikipedia URL
        response = requests.get(wikipedia_url)
        response.raise_for_status()

        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Simpler approach: find all <a> tags with hreflang attribute
        lang_links = soup.find_all('a', attrs={'hreflang': True})
        
        print(f"Found {len(lang_links)} language links")  # Debug print

        # Initialize languages dictionary
        languages = {}

        # Extract language codes and names
        for link in lang_links:
            lang_code = link.get('hreflang')
            lang_name = link.get('title', '').split(' – ')[0]
            if lang_code and lang_name:
                languages[lang_code] = lang_name

        print(f"Processed {len(languages)} valid languages")  # Debug print
        return languages

    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return {}
    except Exception as e:
        print(f"Error processing page: {e}")
        return {}

# Example usage
if __name__ == '__main__':
    url = "https://en.wikipedia.org/wiki/David_Lynch"
    alternative_languages = get_alternative_languages(url)

    if alternative_languages:
        print("\nFound languages:")
        for code, name in alternative_languages.items():
            print(f"{code}: {name}")
    else:
        print("No languages found")
