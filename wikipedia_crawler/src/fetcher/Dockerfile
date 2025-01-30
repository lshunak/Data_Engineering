FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY queue_service /app/queue_service
RUN pip install -e /app/queue_service

COPY storage /app/storage
RUN pip install --timeout=120 -e /app/storage

COPY fetcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create storage directory
RUN mkdir -p /app/html_storage

# Copy service code
COPY fetcher .

CMD ["python", "fetcher.py"]