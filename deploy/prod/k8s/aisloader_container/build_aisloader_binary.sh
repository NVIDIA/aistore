#!/bin/bash

#
# Build the aisloader binary
#

function usage {
    cat > /dev/stderr <<-EOM
$0 <path-to-new-ais-binary> [debug]

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
    [[ $gover =~ go1.16 ]] || whinge "Go version 1.16.* is required"
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
    check_go_version
    export GOPATH=${GOPATH:=$HOME/go}
    export GOBIN=${GOBIN:=$GOPATH/bin}
    [[ -d $GOPATH/src/$AISTORE_SRC ]] || whinge "$AISTORE_SRC not found in $GOPATH/src"
fi

cd $GOPATH/src/$AISTORE_SRC && make aisloader && cp $GOBIN/aisloader $1
[[ $? -eq 0 ]] || whinge "failed to compile 'aisloader'"

exit 0
