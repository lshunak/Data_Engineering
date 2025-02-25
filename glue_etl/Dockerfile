FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Set working directory
WORKDIR /home/glue_user/workspace

# Copy your Glue scripts to the container
COPY scripts/ /home/glue_user/workspace/

# Set environment variables
ENV AWS_REGION=us-east-1
ENV PYTHONPATH=/home/glue_user/workspace

# Default command to run the Spark job
CMD ["spark-submit", "/home/glue_user/workspace/glue-pyspark.py"]