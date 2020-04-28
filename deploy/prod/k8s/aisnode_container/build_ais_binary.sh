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

    gover=$(go version)
    echo "Using $gobin $gover" >&2
    [[ $gover =~ go1.13 || $gover =~ go1.14 ]] || whinge "Go version 1.13.* or 1.14.* is required"
}

if (( $# < 1 )); then
    usage
    exit 1
fi

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
    # Use go in existing PATH or try /usr/local/go, assume AIS source already present
    which go >/dev/null || PATH=$PATH:/usr/local/go/bin
    which go >/dev/null || whinge "Go not found in PATH or in /usr/local/bin"
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
cd "${GOPATH}/src/${AISTORE_SRC}" && MODE=$2 GOBIN=$1 make node cli xmeta
[[ $? -eq 0 ]] || whinge "failed to compile one of 'node', 'cli', or 'xmeta'"

exit 0
