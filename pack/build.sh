#!/bin/bash
set -e
cd "${0%/*}/.."

TAG="${VERSION:-latest}"
IMAGE="${IMAGE:-cdf-tk}"
PUBLISH="${PUBLISH:-false}"

echo "Building image $IMAGE:$TAG"

set +e
if $PUBLISH -eq "true"; then
  # building a poetry project, which provides `cdf-tk` as a command
  # keeping `logs/*` in the image, so that the `cdf-tk` logging can write to it
  pack build "$IMAGE:$TAG" --buildpack paketo-buildpacks/python \
                          --builder paketobuildpacks/builder:base \
                          --buildpack paketo-buildpacks/source-removal \
                          --default-process=run \
                          --env BP_INCLUDE_FILES='cognite_toolkit/*:cdf-tk' \
                          --publish
else
  pack build "$IMAGE:$TAG" --buildpack paketo-buildpacks/python \
                          --builder paketobuildpacks/builder:base \
                          --buildpack paketo-buildpacks/source-removal \
                          --default-process=run \
                          --env BP_INCLUDE_FILES='cognite_toolkit/*:cdf-tk' \
                          --env BP_LIVE_RELOAD_ENABLED=true
fi
set -e

RESULT=$?
exit $RESULT