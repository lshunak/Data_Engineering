#!/bin/bash

CONTAINER_NAME="dynamodb-local"
DATA_DIR=~/dynamodb-local/data
ENDPOINT="--endpoint-url http://localhost:8000"

case "$1" in
    "start")
        echo "Starting DynamoDB Local..."
        # Check if container exists
        if docker ps -a | grep -q $CONTAINER_NAME; then
            echo "Container exists, starting..."
            docker start $CONTAINER_NAME
        else
            echo "Creating new container..."
            mkdir -p $DATA_DIR
            docker run -d \
                --name $CONTAINER_NAME \
                -p 8000:8000 \
                -v $DATA_DIR:/home/dynamodblocal/data \
                amazon/dynamodb-local \
                -jar DynamoDBLocal.jar -sharedDb -dbPath /home/dynamodblocal/data
        fi
        ;;
    "stop")
        echo "Stopping DynamoDB Local..."
        docker stop $CONTAINER_NAME
        ;;
    "restart")
        echo "Restarting DynamoDB Local..."
        docker restart $CONTAINER_NAME
        ;;
    "clean")
        echo "Removing DynamoDB Local container and data..."
        docker stop $CONTAINER_NAME 2>/dev/null || true
        docker rm $CONTAINER_NAME 2>/dev/null || true
        rm -rf $DATA_DIR
        ;;
    "status")
        echo "DynamoDB Local Status:"
        if docker ps | grep -q $CONTAINER_NAME; then
            echo "Running"
            docker ps | grep $CONTAINER_NAME
        elif docker ps -a | grep -q $CONTAINER_NAME; then
            echo "Stopped"
            docker ps -a | grep $CONTAINER_NAME
        else
            echo "Not installed"
        fi
        ;;
    "list")
        echo "Listing tables..."
        aws dynamodb list-tables $ENDPOINT
        ;;

    "count")
        if [ -z "$2" ]; then
            echo "Usage: $0 count <table-name>"
            exit 1
        fi
        echo "Counting items in table $2..."
        aws dynamodb scan $ENDPOINT \
            --table-name "$2" \
            --select COUNT \
            --output json
        ;;

    "query")
        if [ -z "$2" ]; then
            echo "Usage: $0 query <table-name>"
            exit 1
        fi
        echo "Querying table $2..."
        aws dynamodb scan $ENDPOINT \
            --table-name "$2" \
            --max-items 5 \
            --output json
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|clean|status|list|count|query}"
        exit 1
esac