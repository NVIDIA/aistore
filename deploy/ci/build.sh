#!/bin/bash
set -e  # Exit immediately on error

GO_BASE_VERSION="1.25"
DOCKER="${DOCKER:-docker}"
INTERNAL_DOCKERFILE="k8s.dockerfile"
DOCKERFILE="${DOCKERFILE:-"fedora.dockerfile"}"
INTERNAL_IMAGE="operator-test"

# Get the latest patch version for the manual go install
GO_VERSION=$(curl -sfL https://go.dev/dl/?mode=json | \
    jq -r --arg base "go$GO_BASE_VERSION" '[.[] | select(.version | startswith($base))][0].version | sub("go"; "")')

if [[ -z "$GO_VERSION" || "$GO_VERSION" == "null" ]]; then
    echo "Error: Failed to retrieve Go version" >&2
    exit 1
fi

echo "Building internal cached ${INTERNAL_IMAGE} image"
"${DOCKER}" build --build-arg GO_VERSION="${GO_VERSION}" --no-cache -t ${INTERNAL_IMAGE} -f "${INTERNAL_DOCKERFILE}" .
echo "Exporting ${INTERNAL_IMAGE} image"
"${DOCKER}" save ${INTERNAL_IMAGE} | gzip > ${INTERNAL_IMAGE}.tar.gz
echo "Building CI image"
"${DOCKER}" build --build-arg GO_VERSION="${GO_VERSION}" --no-cache -t "${IMAGE_URL}" -f "${DOCKERFILE}" .
rm -f ${INTERNAL_IMAGE}.tar.gz
