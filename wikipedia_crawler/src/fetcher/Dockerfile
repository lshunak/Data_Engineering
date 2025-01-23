FROM python:3.10-slim

RUN apt update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Install dependencies
COPY queue_service /app/queue_service
RUN pip install -e /app/queue_service

COPY storage /app/storage
RUN pip install --timeout=120 -e /app/storage

COPY fetcher/requirements.txt .
RUN pip install -r requirements.txt

# Create storage directory
RUN mkdir -p /app/html_storage

# Copy service code
COPY fetcher .

CMD ["python", "-m" ,"fetcher.py"]