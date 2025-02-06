import os
from typing import Union, List
from hdfs import InsecureClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HDFSLoader:
    def __init__(self, hdfs_url: str, hdfs_user: str):
        """
        Initialize HDFS loader with connection details
        
        Args:
            hdfs_url (str): URL to the HDFS namenode (e.g., 'http://localhost:9870')
            hdfs_user (str): HDFS username for operations
        """
        self.client = InsecureClient(hdfs_url, user=hdfs_user)
        
    def upload_file(self, 
                   local_path: str, 
                   hdfs_path: str,
                   overwrite: bool = False) -> bool:
        """
        Upload a single file to HDFS
        
        Args:
            local_path (str): Path to local file
            hdfs_path (str): Destination path in HDFS
            overwrite (bool): Whether to overwrite existing files
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Local file not found: {local_path}")
                return False
                
            self.client.upload(hdfs_path, local_path, overwrite=overwrite)
            logger.info(f"Successfully uploaded {local_path} to HDFS path {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file to HDFS: {str(e)}")
            return False
            
    def upload_directory(self, 
                        local_dir: str, 
                        hdfs_dir: str,
                        overwrite: bool = False) -> bool:
        """
        Upload an entire directory to HDFS
        
        Args:
            local_dir (str): Path to local directory
            hdfs_dir (str): Destination directory in HDFS
            overwrite (bool): Whether to overwrite existing files
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            if not os.path.isdir(local_dir):
                logger.error(f"Local directory not found: {local_dir}")
                return False
                
            for root, _, files in os.walk(local_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, local_dir)
                    hdfs_path = os.path.join(hdfs_dir, relative_path)
                    
                    if not self.upload_file(local_path, hdfs_path, overwrite):
                        return False
                        
            return True
            
        except Exception as e:
            logger.error(f"Error uploading directory to HDFS: {str(e)}")
            return False

    def check_file_exists(self, hdfs_path: str) -> bool:
        """
        Check if a file exists in HDFS
        
        Args:
            hdfs_path (str): Path in HDFS to check
            
        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            return self.client.status(hdfs_path, strict=False) is not None
        except Exception:
            return False