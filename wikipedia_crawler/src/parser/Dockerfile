FROM python:3.10-slim
WORKDIR /app

COPY queue_service /app/queue_service
RUN pip install -e /app/queue_service

COPY parser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY parser .

CMD ["python", "parser.py"]
