#!/bin/bash


echo "Setting up Glue ETL local environment..."

# Export AWS credentials
echo "Exporting AWS credentials..."
export AWS_KEY=$(aws configure get aws_access_key_id)
export AWS_SECRET=$(aws configure get aws_secret_access_key)

# Stop and remove existing container if it exists
echo "Cleaning up existing container..."
docker stop glue_local_dev 2>/dev/null || true
docker rm glue_local_dev 2>/dev/null || true

# Run the Docker container
echo "Starting Glue local development container..."
docker run -it \
  -v "$(pwd)/scripts:/home/glue_user/workspace" \
  -e AWS_ACCESS_KEY_ID="${AWS_KEY}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET}" \
  -e AWS_DEFAULT_REGION=eu-north-1 \
  -p 4040:4040 -p 18080:18080 \
  --name glue_local_dev \
  glue-local-dev

# Clean up environment variables
echo "Cleaning up environment variables..."
unset AWS_KEY
unset AWS_SECRET

echo "Done!"