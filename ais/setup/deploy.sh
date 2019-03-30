#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3] [-statstime=<DURATION>]
#
# To deploy AIStore with code coverage enabled, set ENABLE_CODE_COVERAGE=1.
# After runs, to collect code coverage data:
# 1. run: make kill
#	wait until all AIStore processes are stopped, currently this is not automated, on screen, it will
#	show "coverage: x.y% of statements" for each process, this indicates the proper termination and
#	successful creation of coverage data for one process.
# 2. run: make code-coverage
#	this will generate ais_cov.html file under /tmp/ais
# 3. view the result
#	open /tmp/ais/ais_cov.html in a browser
#
# To deploy AIStore as a next tier cluster to the *already running*
# AIStore cluster set DEPLOY_AS_NEXT_TIER=1.
#
############################################

isCommandAvailable () {
	command=$1
	versionCheckArgs=$2
	$command $versionCheckArgs > /dev/null 2>&1
	if [[ $? -ne 0 ]]; then
		echo "Error: '$command' not available."
		exit 1
	fi
}

isCommandAvailable "lsblk" "--version"
isCommandAvailable "df" "--version"

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
USE_HTTPS=false
if [ "$DEPLOY_AS_NEXT_TIER" == "" ]
then
	PORT=${PORT:-8080}
	PORT_INTRA=${PORT_INTRA:-9080}
	PORT_REPL=${PORT_REPL:-10080}
	NEXT_TIER=
else
	PORT=${PORT:-11080}
	PORT_INTRA=${PORT_INTRA:-12080}
	PORT_REPL=${PORT_REPL:-13080}
	NEXT_TIER="_next"
fi
PROXYURL="http://localhost:$PORT"
if $USE_HTTPS; then
	PROXYURL="https://localhost:$PORT"
fi
LOGROOT="/tmp/ais$NEXT_TIER"
#### Authentication setup #########
SECRETKEY="${SECRETKEY:-aBitLongSecretKey}"
AUTHENABLED="${AUTHENABLED:-false}"
AUTH_SU_NAME="${AUTH_SU_NAME:-admin}"
AUTH_SU_PASS="${AUTH_SU_PASS:-admin}"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
CONFDIR="$HOME/.ais$NEXT_TIER"
TEST_FSPATH_COUNT=1

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
	echo "Error: TCP port $PORT is not open (check if AIStore is already running)"
	exit 1
fi
TMPF=$(mktemp /tmp/ais$NEXT_TIER.XXXXXXXXX)
touch $TMPF;
OS=$(uname -s)
case $OS in
	Linux) #Linux
		isCommandAvailable "iostat" "-V"
		setfattr -n user.comment -v comment $TMPF
		;;
	Darwin) #macOS
		xattr -w user.comment comment $TMPF
		;;
	*)
		echo "Error: '$OS' is not supported"
		rm $TMPF 2>/dev/null
		exit 1
esac
if [ $? -ne 0 ]; then
	echo "Error: bad kernel configuration: extended attributes are not enabled"
	rm $TMPF 2>/dev/null
	exit 1
fi
rm $TMPF 2>/dev/null

echo Enter number of storage targets:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$servcount' is not a number"; exit 1
fi
echo "Enter number of proxies (gateways):"
read proxycount
if ! [[ "$proxycount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$proxycount' is not a number"; exit 1
elif  [ $proxycount -lt 1 ] ; then
	echo "Error: $proxycount must be at least 1"; exit 1
fi
if [ $proxycount -gt 1 ] ; then
	let DISCOVERYPORT=$PORT+1
	DISCOVERYURL="http://localhost:$DISCOVERYPORT"
	if $USE_HTTPS; then
		DISCOVERYURL="https://localhost:$DISCOVERYPORT"
	fi
fi

START=0
END=$(( $servcount + $proxycount - 1 ))

echo "Number of local cache directories (enter 0 to use preconfigured filesystems):"
read testfspathcnt
if ! [[ "$testfspathcnt" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$testfspathcnt' is not a number"; exit 1
fi
TEST_FSPATH_COUNT=$testfspathcnt

# If not specified, CLDPROVIDER it will be empty and build
# will not include neither AWS nor GCP. As long as CLDPROVIDER
# is not equal to `aws` nor `gcp` it will be assumed to be empty.
CLDPROVIDER=""

echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo  3: None
echo Enter your choice:
read cldprovider
if [ $cldprovider -eq 1 ]; then
	CLDPROVIDER="aws"
elif [ $cldprovider -eq 2 ]; then
	CLDPROVIDER="gcp"
fi

mkdir -p $CONFDIR

# Not really used for local testing but to keep config.sh quiet
GRAPHITE_PORT=2003
GRAPHITE_SERVER="127.0.0.1"
CONFFILE_COLLECTD=$CONFDIR/collectd.conf
CONFFILE_STATSD=$CONFDIR/statsd.conf

#
# generate conf file(s) based on the settings/selections above
#
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
for (( c=$START; c<=$END; c++ ))
do
	CONFDIR="$HOME/.ais$NEXT_TIER$c"
	INSTANCE=$c
	mkdir -p $CONFDIR
	CONFFILE="$CONFDIR/ais.json"
	LOGDIR="$LOGROOT/$c/log"
	source $DIR/config.sh
	((PORT++))
	((PORT_INTRA++))
	((PORT_REPL++))
done

# conf file for authn
CONFDIR="$HOME/.ais$NEXT_TIER"
CONFFILE="$CONFDIR/authn.json"
LOGDIR="$LOGROOT/authn/log"
source $DIR/authn.sh


# -logtostderr=false		 # Logs are written to standard error
# -alsologtostderr=false	 # Logs are written to standard error and files
# -stderrthreshold=ERROR	 # Log errors and above are written to stderr and files
# build
VERSION=`git rev-parse --short HEAD`
BUILD=`date +%FT%T%z`

if [ "$ENABLE_CODE_COVERAGE" == "" ]
then
	EXE=$GOPATH/bin/aisnode
	GOBIN=$GOPATH/bin go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/aisnode.go
else
	echo "Note: code test-coverage enabled!"
	EXE=$GOPATH/bin/ais_coverage.test
	rm $LOGROOT/*.cov
	go test . -c -run=TestCoverage -v -o $EXE -cover
fi
if [ $? -ne 0 ]; then
	exit 1
fi
# build authn
GOBIN=$GOPATH/bin go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" ../authn
if [ $? -ne 0 ]; then
	exit 1
fi

# run proxy and storage targets
for (( c=$START; c<=$END; c++ ))
do
	CONFDIR="$HOME/.ais${NEXT_TIER}$c"
	CONFFILE="$CONFDIR/ais.json"

	PROXY_PARAM="-config=$CONFFILE -role=proxy -ntargets=$servcount $1 $2"
	TARGET_PARAM="-config=$CONFFILE -role=target $1 $2"
	if [ "$ENABLE_CODE_COVERAGE" == "" ]
	then
		CMD=$EXE
	else
		CMD="$EXE -coverageTest -test.coverprofile ais${c}.cov -test.outputdir $LOGROOT"
	fi

	if [ $c -eq 0 ]
	then
		export AIS_PRIMARYPROXY="true"
		set -x
		$CMD $PROXY_PARAM&
		{ set +x; } 2>/dev/null
		unset AIS_PRIMARYPROXY
		# wait for the proxy to start up
		sleep 2
	elif [ $c -lt $proxycount ]
	then
		set -x
		$CMD $PROXY_PARAM&
		{ set +x; } 2>/dev/null
	else
		set -x
		$CMD $TARGET_PARAM&
		{ set +x; } 2>/dev/null
	fi
done

if [[ $AUTHENABLED = "true" ]]; then
	CONFDIR="$HOME/.ais$NEXT_TIER"
	CONFFILE="$CONFDIR/authn.json"
	set -x
	$GOPATH/bin/authn -config=$CONFFILE &
	{ set +x; } 2>/dev/null
fi
sleep 0.1

# enable auto-completion
source $DIR/../../cli/aiscli_autocomplete

echo done
