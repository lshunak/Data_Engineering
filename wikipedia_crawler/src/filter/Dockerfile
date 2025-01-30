# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the queue_service directory into the container
COPY queue_service /app/queue_service
RUN apt update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install the queue_service package
RUN pip install -e /app/queue_service

# Copy the requirements file and install dependencies
COPY filter/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the filter directory contents into the container
COPY filter .


# Command to run the application
CMD ["python", "filter.py"]
