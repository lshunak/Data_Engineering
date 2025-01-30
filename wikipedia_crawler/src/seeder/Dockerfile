FROM python:3.10-slim

RUN apt update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY queue_service /app/queue_service
COPY seeder /app/seeder

RUN pip install pika

CMD ["python", "-m", "seeder.seeder"]