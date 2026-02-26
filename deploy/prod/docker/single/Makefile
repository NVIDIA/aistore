#
# Usage:
#  $ IMAGE_TAG=latest make -e all
#  $ IMAGE_REPO=aistorage/cluster-minimal IMAGE_TAG=4.0 make -e all
#

IMAGE_REPO    ?= aistorage/cluster-minimal
BUILD_CONTEXT  = "../../../../."

BUILD_ARGS = --build-arg "mode=$(BUILD_MODE)" \
			 --build-arg "providers=$(AIS_BACKEND_PROVIDERS)"

include ../../docker.mk