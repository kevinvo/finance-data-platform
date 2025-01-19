#!/bin/bash

set -ex  # Exit on error and print commands as they're executed

echo "Cleaning up old layer..."
rm -rf layer/

echo "Creating layer directory structure..."
mkdir -p layer/python

echo "Building Docker image..."
docker build -t lambda-layer:latest .

echo "Running container to copy dependencies..."
docker run --rm \
    -v "$(pwd)/layer/python:/output" \
    --entrypoint sh \
    lambda-layer:latest \
    -c "ls -la /lambda-layer/python/ && echo 'Copying files...' && cp -rv /lambda-layer/python/. /output/"

echo "Verifying layer contents..."
ls -la layer/python/

if [ ! "$(ls -A layer/python)" ]; then
    echo "Error: No files were copied to layer/python"
    exit 1
fi

echo "Layer built successfully in ./layer directory"