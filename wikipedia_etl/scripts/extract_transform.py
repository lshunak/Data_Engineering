import os
from bs4 import BeautifulSoup
import yaml
import json
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from yaml file."""
    try:
        with open('config/config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise

def extract_html_files(directory):
    html_files = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.html'):
                    html_files.append(os.path.join(root, file))
        logger.info(f"Found {len(html_files)} HTML files")
        return html_files
    except Exception as e:
        logger.error(f"Error extracting HTML files: {e}")
        raise

def transform_html_to_text(html_content):
    """Convert HTML content to plain text."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'header', 'footer', 'nav']):
            element.decompose()
        
        return soup.get_text(separator=' ', strip=True)
    except Exception as e:
        logger.error(f"Error transforming HTML to text: {e}")
        raise

def extract_metadata(html_content, file_path):
    """Extract metadata from HTML content."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract basic metadata
        metadata = {
            'title': soup.title.string if soup.title else 'No title',
            'url': '',  # You might want to extract this from your MongoDB
            'categories': [],
            'file_path': file_path,
            'processed_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Extract categories
        category_links = soup.find_all('div', {'class': 'mw-normal-catlinks'})
        for div in category_links:
            categories = [a.text for a in div.find_all('a')[1:]]  # Skip the first item
            metadata['categories'].extend(categories)
        
        return metadata
    except Exception as e:
        logger.error(f"Error extracting metadata: {e}")
        raise

def process_file(file_path):
    """Process a single HTML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        text = transform_html_to_text(html_content)
        metadata = extract_metadata(html_content, file_path)
        
        return text, metadata
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        raise

def main():
    """Main function to run the extraction and transformation."""
    try:
        # Load configuration
        config = load_config()
        html_directory = config['paths']['html_storage']
        
        # Get list of HTML files
        html_files = extract_html_files(html_directory)
        
        processed_data = []
        
        # Process each file
        for file_path in html_files:
            logger.info(f"Processing file: {file_path}")
            text, metadata = process_file(file_path)
            
            processed_data.append({
                'text': text,
                'metadata': metadata
            })
            
        # Save processed data (temporary storage)
        output_dir = 'processed_data'
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save processed data
        with open(f'{output_dir}/processed_data_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Successfully processed {len(processed_data)} files")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()