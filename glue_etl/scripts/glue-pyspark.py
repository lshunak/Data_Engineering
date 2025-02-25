from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, when
from typing import Tuple, Optional

def init_glue_context() -> Tuple[GlueContext, Job]:
    """
    Initialize Glue context and job
    
    Returns:
        Tuple containing GlueContext and Job objects
    """
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    return glue_context, job

def print_environment_info(spark) -> None:
    """Print environment information"""
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Current User's Login: lshunak")
    print("AWS Glue Local Development Environment")
    print("--------------------------------------")
    print(f"Spark version: {spark.version}")

def test_s3_connectivity(bucket_name: str) -> bool:
    """
    Test S3 bucket connectivity
    
    Args:
        bucket_name: Name of the S3 bucket
    
    Returns:
        bool: True if connection successful
    """
    try:
        print("\nTesting S3 connectivity...")
        s3_client = boto3.client('s3')
        s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        print("S3 connection successful!")
        return True
    except Exception as e:
        print(f"S3 connection failed: {str(e)}")
        return False

def get_data_from_s3(glue_context: GlueContext, 
                     bucket_name: str, 
                     key: str, 
                     format: str = "csv",
                     format_options: dict = None) -> Optional[DynamicFrame]:
    """
    Read data from S3 using AWS Glue's create_dynamic_frame
    
    Args:
        glue_context: GlueContext instance
        bucket_name: S3 bucket name
        key: Path within the bucket
        format: File format (csv, parquet, json, etc.)
        format_options: Format-specific options
        
    Returns:
        DynamicFrame containing the data or None if error
    """
    try:
        s3_path = f"s3://{bucket_name}/{key}"
        print(f"Reading data from: {s3_path}")
        
        if format_options is None:
            format_options = {
                "withHeader": True,
                "separator": ","
            }
        
        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [s3_path],
                "recurse": True
            },
            format=format,
            format_options=format_options
        )
        
        print(f"Successfully read {dyf.count()} records from S3")
        return dyf
        
    except Exception as e:
        print(f"Error reading from S3: {str(e)}")
        return None

def transform_data(df, age_column: str = "age") -> Optional[DynamicFrame]:
    """
    Apply transformations to the DataFrame
    
    Args:
        df: Input DataFrame
        age_column: Name of the age column
        
    Returns:
        Transformed DynamicFrame or None if error
    """
    try:
        if age_column in df.columns:
            df_transformed = df.withColumn("age_category", 
                             when(col(age_column) < 30, "young")
                             .when(col(age_column) < 40, "middle_aged")
                             .otherwise("senior"))
            return df_transformed
        else:
            print(f"Column {age_column} not found in DataFrame")
            return None
    except Exception as e:
        print(f"Error in transformation: {str(e)}")
        return None

def main():
    """Main execution function"""
    try:
        # Initialize Glue context
        glue_context, job = init_glue_context()
        spark = glue_context.spark_session
        
        # Print environment information
        print_environment_info(spark)
        
        # S3 configuration
        BUCKET_NAME = "lshunak-airflow-dags"
        KEY = "data/users.csv"
        
        # Test S3 connectivity
        if not test_s3_connectivity(BUCKET_NAME):
            sys.exit(1)
        
        # Read data from S3
        dyf = get_data_from_s3(glue_context, BUCKET_NAME, KEY)
        if dyf is None:
            sys.exit(1)
        
        # Convert to DataFrame and show sample
        df = dyf.toDF()
        print("\nSample of data from S3:")
        df.show()
        
        # Transform data
        df_transformed = transform_data(df)
        if df_transformed is not None:
            print("\nTransformed DataFrame:")
            df_transformed.show()
            
            # Convert back to DynamicFrame
            dyf_transformed = DynamicFrame.fromDF(
                df_transformed, 
                glue_context, 
                "transformed_data"
            )
            print("\nTransformed Dynamic Frame Schema:")
            dyf_transformed.printSchema()
        
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"\nError in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()