import pytest
import os
import json
from scripts.load_to_hdfs import HDFSLoader

@pytest.fixture
def hdfs_loader():
    """Create a HDFSLoader instance for testing"""
    hdfs_url = os.getenv('HDFS_URL', 'http://localhost:9870')
    hdfs_user = os.getenv('HDFS_USER', 'lshunak')
    return HDFSLoader(hdfs_url=hdfs_url, hdfs_user=hdfs_user)

def test_upload_file(hdfs_loader):
    """Test uploading a single file to HDFS"""
    # First extract text from JSON
    json_file = "processed_data/processed_data_20250205_153221.json"
    txt_file = json_file.replace('.json', '.txt')
    
    # Extract text from JSON list
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
        # Since data is a list, collect text from each item
        all_texts = [article['text'] for article in data]
        text = '\n\n---\n\n'.join(all_texts)  # Join with separator
    
    # Save text
    with open(txt_file, 'w', encoding='utf-8') as file:
        file.write(text)
    
    # Upload txt file to HDFS
    hdfs_path = '/test/wikipedia/processed_wiki_text.txt'
    assert hdfs_loader.upload_file(txt_file, hdfs_path, overwrite=True)
    assert hdfs_loader.check_file_exists(hdfs_path)

def test_upload_nonexistent_file(hdfs_loader):
    """Test uploading a non-existent file"""
    assert not hdfs_loader.upload_file(
        '/nonexistent/path/file.txt',
        '/test/wikipedia/nonexistent.txt'
    )

def test_upload_directory(hdfs_loader):
    """Test uploading a directory to HDFS"""
    source_dir = "processed_data"
    hdfs_dir = '/test/wikipedia/batch_upload'
    
    # First make sure we have the txt file
    test_upload_file(hdfs_loader)  # This will create the txt file
    
    assert hdfs_loader.upload_directory(source_dir, hdfs_dir, overwrite=True)
    
    # Verify JSON exists
    assert hdfs_loader.check_file_exists(f"{hdfs_dir}/processed_data_20250205_153221.json")

def test_file_exists(hdfs_loader):
    """Test file existence checking"""
    # First make sure we have the txt file
    test_upload_file(hdfs_loader)  # This will create the txt file
    
    hdfs_path = '/test/wikipedia/test_exists.txt'
    source_file = "processed_data/processed_data_20250205_153221.txt"
    
    # Upload and verify
    assert hdfs_loader.upload_file(source_file, hdfs_path, overwrite=True)
    assert hdfs_loader.check_file_exists(hdfs_path)