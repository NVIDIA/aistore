#
# Usage:
#  $ env IMAGE_TAG="v3.1" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/authn" IMAGE_TAG="v3.2" \
#        make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/authn" IMAGE_TAG="v3.2-beta" \
#        make all
#
IMAGE_REPO   ?= aistorage/authn

# Build arguments
BUILD_CONTEXT = "../../../../."

include ../../docker.mk