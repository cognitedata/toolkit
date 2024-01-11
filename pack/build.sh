#!/bin/bash
set -e
cd "${0%/*}/.."

TAG="${VERSION:-latest}"
IMAGE="${IMAGE:-cdf-tk}"
PUBLISH="${PUBLISH:-false}"

echo "Building image $IMAGE:$TAG"

set +e
if $PUBLISH -eq "true"; then
  IMAGE="cognite/cdf-tk"  # official name of the tool within Docker Hub
  TAG=$(grep "^version" pyproject.toml | head -1 | awk -F '"' '{print $2}')  # Use the package version directly

  pack build "$IMAGE:$TAG" --buildpack paketo-buildpacks/python \
                          --builder paketobuildpacks/builder:base \
                          --buildpack paketo-buildpacks/source-removal \
                          --default-process=run \
                          --env BP_INCLUDE_FILES='cdf-tk:cognite_toolkit/cdf_tk/*:cognite_toolkit/*.py' \
                          --publish
else
  pack build "$IMAGE:$TAG" --buildpack paketo-buildpacks/python \
                          --builder paketobuildpacks/builder:base \
                          --buildpack paketo-buildpacks/source-removal \
                          --default-process=run \
                          --env BP_INCLUDE_FILES='cdf-tk:cognite_toolkit/cdf_tk/*:cognite_toolkit/*.py'
fi
RESULT=$?
set -e

exit $RESULT
