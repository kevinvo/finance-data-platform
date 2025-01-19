#!/bin/bash

set -e  # Exit on error, removed -x flag to reduce logging

echo "Cleaning up old layer..."
rm -rf layer/

echo "Creating layer directory structure..."
mkdir -p layer/python

# Add jq to the dependencies since we'll use it to format logs
echo "Installing jq for log formatting..."
if ! command -v jq &> /dev/null; then
    if [ -f /etc/debian_version ]; then
        apt-get update && apt-get install -y jq
    elif [ -f /etc/redhat-release ]; then
        yum install -y jq
    elif command -v brew &> /dev/null; then
        brew install jq
    else
        echo "Please install jq manually"
        exit 1
    fi
fi

echo "Building Docker image..."
docker build -t lambda-layer:latest . > /dev/null

echo "Running container to copy dependencies..."
docker run --rm \
    -v "$(pwd)/layer/python:/output" \
    --entrypoint sh \
    lambda-layer:latest \
    -c "cp -r /lambda-layer/python/. /output/" > /dev/null

echo "Verifying layer contents..."
if [ ! "$(ls -A layer/python)" ]; then
    echo "Error: No files were copied to layer/python"
    exit 1
fi

echo "Layer built successfully in ./layer directory"