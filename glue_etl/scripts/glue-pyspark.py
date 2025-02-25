from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import col, when

# Initialize GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("Current Date and Time (UTC): 2025-02-25 09:47:24")
print("Current User's Login: lshunak")
print("AWS Glue Local Development Environment")
print("--------------------------------------")
print(f"Spark version: {spark.version}")

# Create a simple DataFrame
data = [
    (1, "John", 30),
    (2, "Jane", 25),
    (3, "Bob", 40),
    (4, "Alice", 35)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
print("\nSample DataFrame:")
df.show()

# Apply transformation
df_transformed = df.withColumn("age_category", 
                 when(col("age") < 30, "young")
                 .when(col("age") < 40, "middle_aged")
                 .otherwise("senior"))

print("\nTransformed DataFrame:")
df_transformed.show()

# Convert to Glue DynamicFrame
from awsglue.dynamicframe import DynamicFrame
dyf = DynamicFrame.fromDF(df_transformed, glueContext, "sample")
print("\nDynamic Frame Schema:")
dyf.printSchema()

print("\nTest completed successfully!")