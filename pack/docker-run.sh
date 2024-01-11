#!/bin/bash
set -e
cd "${0%/*}/.."

TAG="${VERSION:-latest}"
IMAGE="${IMAGE:-cdf-tk}"

echo "Running image '$IMAGE:$TAG'"
echo "$(pwd)"

# Create a temporary directory for the build output
BUILD_DIR=$(mktemp -d -q $(pwd)/cdf-tk.XXXXXX)

# Build Command
set +e
docker run \
  -v $(realpath ./$1):/tmp/project:ro \
  -v $BUILD_DIR:/workspace/build_output \
  --entrypoint run \
  --rm \
  $IMAGE \
  --env-path=/tmp/project/.env \
  build \
  --build-dir=/workspace/build_output \
  --env=dev \
  --clean \
  /tmp/project
set -e

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed, exiting."
    exit 1
fi

echo "Build successful, starting deployment."

# Deploy Command
set +e
docker run \
  -v $(realpath ./$1):/tmp/project:ro \
  -v $BUILD_DIR:/workspace/build_output:ro \
  --entrypoint run \
  --rm \
  $IMAGE \
  --env-path=/tmp/project/.env \
  deploy \
  --drop \
  --env=dev \
  --dry-run \
  /workspace/build_output
set -e

IMAGE_SIZE=$(docker image inspect $IMAGE:$TAG --format='{{.Size}}')
echo "Size of the Docker image: $(($IMAGE_SIZE / (1024 * 1024)))MB"

RESULT=$?
exit $RESULT
