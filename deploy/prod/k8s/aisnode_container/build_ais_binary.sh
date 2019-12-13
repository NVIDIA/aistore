#!/bin/bash

#
# Build the ais binary, ready for inclusion in a Docker image. This script
# is primarily intended for use from Jenkins build jobs. See usage below.
#

function usage {
    cat > /dev/stderr <<-EOM
$0 <path-to-new-ais-binary> [debug]

Builds the ais binary, ready for inclusion in a docker image. If run as part
of a Jenkins job, this script will go get the current AIS bits from github;
otherwise we work with AIS source bits in your GOPATH.

If the optional "debug" argument is included then we pass gcflags to include
debug info in the binary (to make delve happy).
EOM
}

function whinge {
    echo "$@" >&2
    exit 2
}

function check_go_version {
    gobin=$(which go)
    [[ -n "$gobin" ]] || whinge "go not found"

    ver=$(go version | awk '{print $3}')
    echo "Using $gobin (go version $ver)" >&2
    [[ $ver =~ go1.13 ]] || whinge "Go version 1.13.* is required"
}

if (( $# < 1 )); then
    usage
    exit 1
fi

DEST=$(readlink -f $1)     # need an absolute path for subshell below

set -e

AISTORE_SRC=github.com/NVIDIA/aistore

if [[ -n "$JENKINS_URL" ]]; then
    PATH=$PATH:/usr/local/go/bin

    check_go_version

    #
    # Create and populate a temporary go tree
    #
    export GOPATH=$(mktemp -d)
    [[ -n "$GOPATH" ]] || whinge "Failed to make temp dir for go" >&2
    mkdir $GOPATH/{bin,pkg,src}
    export GOBIN=$GOPATH/bin

    echo "Go get AIStore source from ${AISTORE_SRC}"
    go get -v ${AISTORE_SRC}/ais
else
    # Use go in existing PATH, assume AIS source already present
    check_go_version
    GOPATH=${GOPATH:=$HOME/go}
    GOBIN=${GOBIN:=$GOPATH/bin}
    [[ -d "${GOPATH}/src/${AISTORE_SRC}" ]] || whinge "${AISTORE_SRC} not found in ${GOPATH}/src"
fi

if [[ $2 == "debug" ]]; then
  echo "Build type: debug"
else
  echo "Build type: production"
fi

#
# It's tempting to default CLDPROVIDER if not set, but that then requires a reasonably
# complete config for that cloud service (e.g., from aws configure) which may not be
# what you want if just working with aisloader. So we require explicit choice of cloud
# provider in building the ais binary (and then go on to include that binary in a Docker
# image that is then also cloud-specific).
#
echo "Cloud provider set to: ${CLDPROVIDER}"
cd "${GOPATH}/src/${AISTORE_SRC}" && MODE=$2 GOBIN=${DEST} make node
[[ $? -eq 0 ]] || whinge "failed to compile 'node'"

exit 0
