#!/bin/bash

#
# Build the aisloader binary
#

function usage {
    cat > /dev/stderr <<-EOM
    $1 <path-to-new-ais-binary> [debug]

    Builds the aisloader binary, ready for inclusion in a docker image. If run as part
    of a Jenkins job, this script will go get the current AIS bits from github;
    otherwise we work with AIS source bits in your GOPATH.
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
    [[ $gover =~ go1.13 ]] || whinge "Go version 1.13.* is required"
}

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

    echo 'Go get AIStore source from $AISTORE_SRC'
    /usr/local/go/bin/go get -v $AISTORE_SRC
else
    # Use go in existing PATH
    check_go_version
    GOPATH=${GOPATH:=$HOME/go}
    GOBIN=${GOBIN:=$GOPATH/bin}
    [[ -d $GOPATH/src/$AISTORE_SRC ]] || whinge "$AISTORE_SRC not found in $GOPATH/src"
fi

VERSION=$(cd $GOPATH/src/$AISTORE_SRC && git rev-parse --short HEAD)
BUILD=$(date +'%FT%T%z')

(
    cd $GOPATH/src/$AISTORE_SRC/bench/aisloader &&
    go build \
      -o $DEST \
      -ldflags "-w -s $LDFLAGS -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'"
)

[[ $? -eq 0 ]] || whinge "go build failed"

exit 0
