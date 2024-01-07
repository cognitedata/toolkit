#!/bin/bash
set -e
cd "${0%/*}/.."

TAG="${VERSION:-latest}"
IMAGE="${IMAGE:-cdf-tk}"

echo "Running image '$IMAGE:$TAG'"
echo "$(pwd)"

# Create a temporary directory for the build output
BUILD_DIR=$(mktemp -d)


# Build Command
set +e
docker run \
  --mount type=bind,source=$(pwd)/demo_project,target=/workspace/demo_project,readonly \
  --mount type=bind,source="$BUILD_DIR",target=/workspace/build_output \
  --entrypoint run \
  --rm \
  $IMAGE \
  build \
  --build-dir=/workspace/build_output \
  --env=demo \
  --clean \
  /workspace/demo_project
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
  --mount type=bind,source="$BUILD_DIR",target=/workspace/build_output,readonly \
  --entrypoint run \
  --rm \
  $IMAGE \
  deploy \
  --drop \
  --env=demo \
  --dry-run \
  /workspace/build_output
set -e

IMAGE_SIZE=$(docker image inspect $IMAGE:$TAG --format='{{.Size}}')
echo "Size of the Docker image: $IMAGE_SIZE bytes"

RESULT=$?
exit $RESULT
