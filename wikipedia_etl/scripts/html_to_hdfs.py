from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import logging
import os
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HTMLProcessor:
    def __init__(
        self,
        input_dir: str,
        hdfs_base: str,
        current_timestamp: str,
        current_user: str
    ):
        self.input_dir = input_dir
        self.hdfs_base = hdfs_base
        self.current_timestamp = current_timestamp
        self.current_user = current_user
        self.spark = self._create_spark_session()
        self._ensure_hdfs_dirs()

    def _create_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName("WikipediaToText") \
            .master("local[*]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()

    def _ensure_hdfs_dirs(self):
        """Ensure HDFS directories exist"""
        # Create base directory
        os.system(f"hdfs dfs -mkdir -p {self.hdfs_base}")
        # Set permissions
        os.system(f"hdfs dfs -chmod 755 {self.hdfs_base}")

    def _clean_filename(self, filename: str) -> str:
        """Clean filename to avoid HDFS issues"""
        # Remove .html extension
        name = filename.replace('.html', '')
        # Replace problematic characters
        name = name.replace('...', '_')
        return name

    def get_html_files(self) -> List[str]:
        """Get list of HTML files from input directory"""
        return [f for f in os.listdir(self.input_dir) if f.endswith('.html')]

    def _remove_if_exists(self, hdfs_path: str):
        """Remove HDFS path if it exists"""
        os.system(f"hdfs dfs -rm -r -f {hdfs_path}")

    def process_file(self, html_file: str) -> Optional[str]:
        try:
            page_name = self._clean_filename(html_file)
            input_path = f"file://{self.input_dir}/{html_file}"
            
            logger.info(f"Processing: {html_file}")
            
            # Read HTML
            df = self.spark.read \
                .option("wholetext", True) \
                .text(input_path)

            # Define paths
            temp_output = f"{self.hdfs_base}/temp_{page_name}"
            final_output = f"{self.hdfs_base}/{page_name}.txt"

            # Clean up any existing files
            self._remove_if_exists(temp_output)
            self._remove_if_exists(final_output)

            # Save as temporary file
            df.rdd.map(lambda x: x[0]).coalesce(1).saveAsTextFile(temp_output)

            # Create local temporary directory for merge operation
            local_temp = "/tmp/wiki_merge"
            os.makedirs(local_temp, exist_ok=True)
            
            # Merge to single file using local temporary storage
            merge_command = f"""
            hdfs dfs -getmerge {temp_output} {local_temp}/{page_name}.txt && \
            hdfs dfs -put -f {local_temp}/{page_name}.txt {final_output} && \
            rm -f {local_temp}/{page_name}.txt
            """
            os.system(merge_command)

            # Clean up temporary HDFS directory
            self._remove_if_exists(temp_output)

            logger.info(f"Saved to: {final_output}")
            return final_output

        except Exception as e:
            logger.error(f"Error processing {html_file}: {str(e)}")
            # Cleanup on error
            if 'temp_output' in locals():
                self._remove_if_exists(temp_output)
            return None

    def process_all_files(self) -> List[str]:
        """Process all HTML files in the input directory"""
        processed_files = []
        
        for html_file in self.get_html_files():
            output_path = self.process_file(html_file)
            if output_path:
                processed_files.append(output_path)

        return processed_files

    def cleanup(self):
        """Stop Spark session and cleanup temporary files"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
        # Clean up local temp directory
        os.system("rm -rf /tmp/wiki_merge")

def main(execution_date: Optional[now] = None) -> None:
    # Configuration
    INPUT_DIR = "/home/liran/Documents/data_engineering/wikipedia_etl/tests/data"
    HDFS_BASE = "hdfs://localhost:9000/user/lshunak/wikipedia"
    CURRENT_TIMESTAMP = "2025-02-10 14:03:16"
    CURRENT_USER = "lshunak"

    try:
        processor = HTMLProcessor(
            input_dir=INPUT_DIR,
            hdfs_base=HDFS_BASE,
            current_timestamp=CURRENT_TIMESTAMP,
            current_user=CURRENT_USER
        )

        processed_files = processor.process_all_files()

        logger.info(f"\nProcessed {len(processed_files)} files:")
        for file_path in processed_files:
            logger.info(f"- {file_path}")
            logger.info(f"  View content with: hdfs dfs -cat {file_path}")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        processor.cleanup()

if __name__ == "__main__":
    main()