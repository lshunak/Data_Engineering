from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import sys
import boto3
from datetime import datetime
from pyspark.sql.functions import col, when
from typing import Tuple, Optional, List
import logging
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    return glue_context, spark, job

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

def extract_from_s3(glue_context, bucket, key, format="csv"):
    """Extract data from S3 into a DynamicFrame"""
    try:
        s3_path = f"s3://{bucket}/{key}"
        logger.info(f"Extracting data from {s3_path}")
        
        # Add format options for CSV to handle headers
        format_options = {}
        if format.lower() == "csv":
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
            format_options=format_options  # Add this parameter
        )
        logger.info(f"Successfully extracted {dyf.count()} records")
        return dyf
    except Exception as e:
        logger.error(f"Error extracting from S3: {str(e)}")
        return None

def combine_dataframes(dfs: List[DataFrame]) -> DataFrame:
    """
    Combine multiple dataframes with the same schema into a single dataframe
    
    Args:
        dfs: List of dataframes to combine
        
    Returns:
        Combined dataframe
    """
    if not dfs:
        logger.warning("No dataframes to combine")
        return None
    
    if len(dfs) == 1:
        return dfs[0]
    
    try:
        logger.info(f"Combining {len(dfs)} dataframes")
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        logger.info(f"Combined dataframe has {combined_df.count()} records")
        return combined_df
    except Exception as e:
        logger.error(f"Error combining dataframes: {str(e)}")
        return None

def find_popular_songs_by_age_group(users_df, songs_df, streams_df):
    """
    Find the most popular songs for each age group
    
    Args:
        users_df: DataFrame containing user information
        songs_df: DataFrame containing song information
        streams_df: DataFrame containing streaming data
        
    Returns:
        DataFrame with popular songs by age group with columns:
        age_group, song_id, title, listening_count
    """
    try:
        logger.info("Finding most popular songs by age group")
        
        # First categorize users by age
        users_with_category = users_df.withColumn("age_category", 
                                when(col("user_age") < 30, "young")
                                .when(col("user_age") < 60, "middle_aged")
                                .otherwise("senior"))
        
        # Join users with streams to get user_id, track_id and age_category
        user_streams = streams_df.join(
            users_with_category.select("user_id", "age_category"),
            on="user_id",
            how="inner"
        )
        
        # Count streams by track_id and age_category (NOT song_id)
        song_popularity = user_streams.groupBy("track_id", "age_category") \
            .count() \
            .withColumnRenamed("count", "listening_count")
        
        # Join with songs to get song details including title
        # Notice the join is on track_id now (NOT song_id)
        result = song_popularity.join(
            songs_df.select("track_id", "track_name"),
            on="track_id",
            how="inner"
        )
        
        # Select only the required columns and rename age_category to age_group and track_name to title
        result = result.select(
            col("age_category").alias("age_group"),
            col("track_id").alias("song_id"),  # rename track_id to song_id for consistency with requirements
            col("track_name").alias("title"),
            col("listening_count")
        )
        
        # Order by age_group and listening_count in descending order
        result = result.orderBy(
            col("age_group"),
            col("listening_count").desc()
        )
        
        logger.info("Successfully found popular songs by age group")
        return result
        
    except Exception as e:
        logger.error(f"Error finding popular songs by age group: {str(e)}")
        return None

def find_popular_songs_by_time_of_day(songs_df, streams_df):
    """
    Find the most popular songs for each time of day period
    
    Time periods:
    - Morning: 6:00 - 11:59
    - Noon: 12:00 - 13:59
    - Afternoon: 14:00 - 17:59
    - Evening: 18:00 - 21:59
    - Night: 22:00 - 00:59
    - Late Night: 1:00 - 5:59
    
    Args:
        songs_df: DataFrame containing song information
        streams_df: DataFrame containing streaming data with listen_time column
        
    Returns:
        DataFrame with popular songs by time period with columns:
        time_period, song_id, title, listening_count
    """
    try:
        logger.info("Finding most popular songs by time of day")
        
        # Extract hour from listen_time string
        from pyspark.sql.functions import hour, to_timestamp
        
        # Convert listen_time string to timestamp and extract hour
        streams_with_hour = streams_df.withColumn(
            "hour", 
            hour(to_timestamp(col("listen_time"), "yyyy-MM-dd HH:mm:ss"))
        )
        
        # Categorize hours into time periods
        streams_with_period = streams_with_hour.withColumn(
            "time_period",
            when((col("hour") >= 6) & (col("hour") < 12), "morning")
            .when((col("hour") >= 12) & (col("hour") < 14), "noon")
            .when((col("hour") >= 14) & (col("hour") < 18), "afternoon")
            .when((col("hour") >= 18) & (col("hour") < 22), "evening")
            .when((col("hour") >= 22), "night")  # 22-23 hours
            .when(col("hour") < 1, "night")      # 0 hour (midnight)
            .when((col("hour") >= 1) & (col("hour") < 6), "late_night")
            .otherwise("unknown")  # Just in case there are any invalid values
        )
        
        # Count streams by track_id and time_period
        song_popularity = streams_with_period.groupBy("track_id", "time_period") \
            .count() \
            .withColumnRenamed("count", "listening_count")
        
        # Join with songs to get song details
        result = song_popularity.join(
            songs_df.select("track_id", "track_name"),
            on="track_id",
            how="inner"
        )
        
        # Select and rename columns
        result = result.select(
            col("time_period"),
            col("track_id").alias("song_id"),
            col("track_name").alias("title"),
            col("listening_count")
        )
        
        # Order by time_period and listening_count
        result = result.orderBy(
            col("time_period"),
            col("listening_count").desc()
        )
        
        logger.info("Successfully found popular songs by time of day")
        return result
        
    except Exception as e:
        logger.error(f"Error finding popular songs by time of day: {str(e)}")
        return None

def write_to_s3(glue_context, dynamic_frame, bucket, key_prefix, format_type="parquet"):
    """
    Write a DynamicFrame to S3
    
    Args:
        glue_context: GlueContext object
        dynamic_frame: DynamicFrame to write
        bucket: S3 bucket name
        key_prefix: S3 key prefix (path)
        format_type: Output format (default: parquet)
        
    Returns:
        bool: True if write was successful
    """
    try:
        logger.info(f"Writing {dynamic_frame.count()} records to s3://{bucket}/{key_prefix}")
        
        # Set output path
        output_path = f"s3://{bucket}/{key_prefix}"
        
        # Use a simpler write approach with fewer dependencies
        df = dynamic_frame.toDF()
        
        if format_type.lower() == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format_type.lower() == "csv":
            # For CSV, coalesce to single file and add .csv extension
            # Use coalesce(1) to ensure a single file output
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}_temp")
            
            # Use boto3 to list temp files and rename the single CSV file
            s3_client = boto3.client('s3')
            temp_path = f"{key_prefix}_temp"
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=temp_path)
            
            # Find the CSV file (not _SUCCESS or other metadata)
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    # Copy the file to the proper destination with .csv extension
                    final_key = f"{key_prefix}.csv"
                    s3_client.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': key},
                        Key=final_key
                    )
                    logger.info(f"Successfully copied CSV to s3://{bucket}/{final_key}")
                    break
            
            # Clean up temp directory
            for obj in response.get('Contents', []):
                s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
            
            logger.info(f"Cleaned up temporary files in {temp_path}")
        else:
            df.write.mode("overwrite").format(format_type).save(output_path)
        
        logger.info(f"Successfully wrote data to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        return False

def main():
    """Main execution function"""
    try:
        # Initialize Glue context
        glue_context, spark, job = init_glue_context()
        logger.info(f"Starting ETL job at {datetime.utcnow()}")

        sources = {
            "users": {"bucket": "lshunak-airflow-dags", "key": "data/users.csv"},
            "songs": {"bucket": "lshunak-airflow-dags", "key": "data/songs.csv"},
            "streams1": {"bucket": "lshunak-airflow-dags", "key": "data/streams1.csv"},
            "streams2": {"bucket": "lshunak-airflow-dags", "key": "data/streams2.csv"},
            "streams3": {"bucket": "lshunak-airflow-dags", "key": "data/streams3.csv"}
        }

        # Extract all sources
        dataframes = {}
        for name, config in sources.items():
            dyf = extract_from_s3(
                glue_context,
                config["bucket"],
                config["key"],
                config.get("format", "csv")
            )
            if dyf is not None:
                dataframes[name] = dyf.toDF()
            else:
                logger.error(f"Failed to extract {name}")
                sys.exit(1)

        # Convert to DataFrame 
        users_df = dataframes["users"]
        songs_df = dataframes["songs"]
        
        # Combine all stream dataframes into one
        stream_dfs = [
            dataframes["streams1"], 
            dataframes["streams2"], 
            dataframes["streams3"]
        ]
        combined_streams_df = combine_dataframes(stream_dfs)
        
        if combined_streams_df is None:
            logger.error("Failed to combine stream dataframes")
            sys.exit(1)
        
        combined_streams_df.show(5)

        # KPI: Find most popular songs by age group
        print("\n\nKPI: Most Popular Songs by Age Group")
        print("-" * 50)
        popular_songs = find_popular_songs_by_age_group(
            users_df, 
            songs_df, 
            combined_streams_df
        )
        if popular_songs is not None:
            print("\nMost Popular Songs by Age Group:")
            print("-" * 80)  # Clear separator
            popular_songs.cache()  # Cache the data to ensure it's computed
            
            # Force action to ensure data is processed
            count = popular_songs.count()
            print(f"Found {count} song popularity records")
            
            # Explicitly show the schema
            print("\nPopular Songs Schema:")
            popular_songs.printSchema()
            
            # Show a single, comprehensive table with all results
            print("\nPopular Songs by Age Group - Comprehensive Results:")
            
            # Option 1: Display top N songs per age group in a single table
            # First, create a window function to rank songs within each age group
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            
            window_spec = Window.partitionBy("age_group").orderBy(desc("listening_count"))
            ranked_songs = popular_songs.withColumn("rank", row_number().over(window_spec))
            
            # Get top 10 songs for each age group
            top_songs_by_age = ranked_songs.filter("rank <= 10").orderBy("age_group", "rank")
            
            # Show results in a single table for easy analysis
            print("\nTop 10 Songs for Each Age Group (Unified View):")
            top_songs_by_age.drop("rank").show(30, truncate=False)
            
            # Convert to DynamicFrame for potential writing to S3
            popular_songs_dyf = DynamicFrame.fromDF(
                popular_songs, 
                glue_context, 
                "popular_songs_by_age_group"
            )
            
            # Write only the full dataset to S3
            success = write_to_s3(
                glue_context,
                popular_songs_dyf,
                "lshunak-airflow-dags",
                "OLAP_data/popular_songs_by_age_group",
                "csv"  # CSV format
            )
            
            if success:
                print("\nSuccessfully wrote popular songs by age group data to S3")
            else:
                print("\nFailed to write popular songs by age group data to S3")

  
        # KPI: Find most popular songs by time of day
        print("\n\nKPI: Most Popular Songs by Time of Day")
        print("-" * 50)
        popular_songs_by_time = find_popular_songs_by_time_of_day(
            songs_df, 
            combined_streams_df
        )
        
        if popular_songs_by_time is not None:
            print("\nMost Popular Songs by Time of Day:")
            print("-" * 80)  # Clear separator
            popular_songs_by_time.cache()  # Cache the data to ensure it's computed
            
            # Force action to ensure data is processed
            count = popular_songs_by_time.count()
            print(f"Found {count} song popularity records by time of day")
            
            # Explicitly show the schema
            print("\nPopular Songs by Time Schema:")
            popular_songs_by_time.printSchema()
            
            # Use window function to rank songs within each time period
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            
            time_window_spec = Window.partitionBy("time_period").orderBy(desc("listening_count"))
            ranked_time_songs = popular_songs_by_time.withColumn("rank", row_number().over(time_window_spec))
            
            # Get top 10 songs for each time period
            top_songs_by_time = ranked_time_songs.filter("rank <= 10").orderBy("time_period", "rank")
            
            # Show results in a single table for easy analysis
            print("\nTop 10 Songs for Each Time of Day (Unified View):")
            top_songs_by_time.drop("rank").show(60, truncate=False)
            
            # Convert to DynamicFrame for potential writing to S3
            popular_time_dyf = DynamicFrame.fromDF(
                popular_songs_by_time, 
                glue_context, 
                "popular_songs_by_time_period"
            )
            
            popular_time_dyf.show(50)
            # Write to S3
            success = write_to_s3(
                glue_context,
                popular_time_dyf,
                "lshunak-airflow-dags",
                "OLAP_data/popular_songs_by_time_period",
                "csv"  # Changed format to CSV
            )
            
            if success:
                print("\nSuccessfully wrote popular songs by time period data to S3")
            else:
                print("\nFailed to write popular songs by time period data to S3")
            

        
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"\nError in main execution: {str(e)}")
        sys.exit(1)



if __name__ == "__main__":
    main()
