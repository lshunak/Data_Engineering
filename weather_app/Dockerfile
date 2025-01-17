# Use the official Python base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file first for efficient caching
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the Flask port
EXPOSE 5000

# Run the Flask application
CMD ["python", "weather_app.py"]
