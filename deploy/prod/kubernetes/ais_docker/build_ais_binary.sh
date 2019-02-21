#!/bin/bash

#
# Build the ais binary, ready for inclusion in a Docker image. This script
# is primarily intended for use from Jenkins build jobs. See usage below.
#
# XXX TODO enforce minimum required Go version for build
#


function usage {
    cat > /dev/stderr <<-EOM
    $1 <path-to-new-ais-binary> [debug]

    Builds the ais binary, ready for inclusion in a docker image. If run as part
    of a Jenkins job, this script will go get the current AIS bits from github;
    otherwise we work with AIS source bits in your GOPATH.

    If the optional "debug" argument is included then we pass gcflags to include
    debug info in the binary (to make delve happy).
EOM
}

DEST=$(readlink -f $1)     # need an absolute path for subshell below

set -e

AIS_SRC=github.com/NVIDIA/aistore/ais

if [[ -n "$JENKINS_URL" ]]; then
    PATH=$PATH:/usr/local/go/bin 

    #
    # Create and populate a temporary go tree
    #
    export GOPATH=$(mktemp -d)
    if [[ -z "$GOPATH" ]]; then
        echo "Failed to make temp dir for go" >&2
        exit 1
    fi
    mkdir $GOPATH/{bin,pkg,src}
    export GOBIN=$GOPATH/bin

    echo 'Go get AIStore source from $AIS_SRC'
    /usr/local/go/bin/go get -v $AIS_SRC
else
    GOPATH=${GOPATH:=$HOME/go}
    GOBIN=${GOBIN:=$GOPATH/bin}
    if [[ ! -d $GOPATH/src/$AIS_SRC ]]; then
        echo "$AIS_SRC not found in $GOPATH/src" >&2
        exit 1
    fi
fi

VERSION=$(cd $GOPATH/src/$AIS_SRC && git rev-parse --short HEAD)
BUILD=$(date +'%FT%T%z')

if [[ "X$2" == "Xdebug" ]]; then
    GCFLAGS="-gcflags='all=-N -l'"
    LDEXTRAFLAGS=""
    echo "Build type: debug"
else
    GCFLAGS=""
    LDEXTRAFLAGS="-w -s"
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

(
    cd $GOPATH/src/$AIS_SRC &&
    go build \
      -o $DEST \
      -tags="${CLDPROVIDER}" \
      -ldflags "$LDEXTRAFLAGS -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" \
      ${GCFLAGS:+"$GCFLAGS"} \
      setup/ais.go
)

if [[ $? -ne 0 ]]; then
    echo "go build failed" >&2
    exit 1
fi

exit 0