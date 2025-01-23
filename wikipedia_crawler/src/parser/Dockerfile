FROM python:3.10-slim
WORKDIR /app
RUN apt update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY queue_service /app/queue_service
RUN pip install -e /app/queue_service

COPY parser/requirements.txt .
RUN pip install -r requirements.txt
COPY parser .

CMD ["python", "-m", "parser.py"]
