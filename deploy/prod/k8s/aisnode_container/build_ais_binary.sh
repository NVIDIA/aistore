#!/bin/bash

#
# Build the ais binary, ready for inclusion in a Docker image. This script
# is primarily intended for use from Jenkins build jobs. See usage below.
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

DEST=$(readlink -f $1)     # need an absolute path for subshell below

set -e

AIS_SRC=github.com/NVIDIA/aistore/ais

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

    echo 'Go get AIStore source from $AIS_SRC'
    /usr/local/go/bin/go get -v $AIS_SRC
    cd $GOPATH/src/$AIS_SRC && make mod-init
else
    # Use go in existing PATH, assume AIS source already present, mod-init run
    check_go_version
    GOPATH=${GOPATH:=$HOME/go}
    GOBIN=${GOBIN:=$GOPATH/bin}
    [[ -d $GOPATH/src/$AIS_SRC ]] || whinge "$AIS_SRC not found in $GOPATH/src"
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
      setup/aisnode.go
)

[[ $? -eq 0 ]] || whinge "go build failed"

exit 0
