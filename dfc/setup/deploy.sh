#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3] [-statstime=<DURATION>]
#
# To deploy DFC with code coverage enabled, set ENABLE_CODE_COVERAGE=1.
# After runs, to collect code coverage data:
# 1. run: make kill
#    wait until all DFC processes are stopped, currently this is not automated, on screen, it will
#    show "coverage: x.y% of statements" for each process, this indicates the proper termination and
#    successful creation of coverage data for one process.
# 2. run: make code-coverage
#    this will generate dfc_cov.html file under /tmp/dfc
# 3. view the result
#    open /tmp/dfc/dfc_cov.html in a browser
############################################

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
USE_HTTPS=false
PORT=${PORT:-8080}
PROXYURL="http://localhost:$PORT"
if $USE_HTTPS; then
	PROXYURL="https://localhost:$PORT"
fi
LOGLEVEL="3" # Verbosity: 0 (minimal) to 4 (max)
LOGROOT="/tmp/dfc"
#### Authentication setup #########
SECRETKEY="${SECRETKEY:-aBitLongSecretKey}"
AUTHENABLED="${AUTHENABLED:-false}"
AUTH_SU_NAME="${AUTH_SU_NAME:-admin}"
AUTH_SU_PASS="${AUTH_SU_PASS:-admin}"
NON_ELECTABLE=false
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
CONFDIR="$HOME/.dfc"
TESTFSPATHCOUNT=1

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
	echo "Error: TCP port $PORT is not open (check if DFC is already running)"
	exit 1
fi
TMPF=$(mktemp /tmp/dfc.XXXXXXXXX)
touch $TMPF;
OS=$(uname -s)
case $OS in
	Linux) #Linux
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

echo Enter number of cache targets:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$servcount' is not a number"; exit 1
fi
echo "Enter number of proxies:"
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
TESTFSPATHCOUNT=$testfspathcnt

echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo Enter your choice:
read cldprovider
if [ $cldprovider -eq 1 ]
then
	CLDPROVIDER="aws"
elif [ $cldprovider -eq 2 ]
then
	CLDPROVIDER="gcp"
else
	echo "Error: '$cldprovider' is not a valid input, can be either 1 or 2"; exit 1
fi

mkdir -p $CONFDIR

# Not really used for local testing but to keep config.sh quiet
GRAPHITE_SERVER="127.0.0.1"
CONFFILE_COLLECTD=$CONFDIR/collectd.conf
CONFFILE_STATSD=$CONFDIR/statsd.conf

#
# generate conf file(s) based on the settings/selections above
#
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
for (( c=$START; c<=$END; c++ ))
do
	CONFDIR="$HOME/.dfc$c"
	mkdir -p $CONFDIR
	CONFFILE="$CONFDIR/dfc.json"
	LOGDIR="$LOGROOT/$c/log"
	source $DIR/config.sh
	((PORT++))
done

# conf file for authn
CONFDIR="$HOME/.dfc"
CONFFILE="$CONFDIR/authn.json"
LOGDIR="$LOGROOT/authn/log"
source $DIR/authn.sh


# -logtostderr=false 		# Logs are written to standard error
# -alsologtostderr=false 	# Logs are written to standard error and files
# -stderrthreshold=ERROR 	# Log errors and above are written to stderr and files
# build
BUILD=`git rev-parse --short HEAD`
if [ "$ENABLE_CODE_COVERAGE" == "" ]
then
	EXE=$GOPATH/bin/dfc
	go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go
else
	echo "Note: code test-coverage enabled!"
	EXE=$GOPATH/bin/dfc_coverage.test
	rm $LOGROOT/*.cov
	go test . -c -run=TestCoverage -v -o $EXE -cover
fi
if [ $? -ne 0 ]; then
	exit 1
fi
# build authn
go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" ../authn
if [ $? -ne 0 ]; then
	exit 1
fi

# run proxy and storage targets
for (( c=$START; c<=$END; c++ ))
do
	CONFDIR="$HOME/.dfc$c"
	CONFFILE="$CONFDIR/dfc.json"

	PROXY_PARAM="-config=$CONFFILE -role=proxy -ntargets=$servcount $1 $2"
	TARGET_PARAM="-config=$CONFFILE -role=target $1 $2"
	if [ "$ENABLE_CODE_COVERAGE" == "" ]
	then
		CMD=$EXE
	else
		CMD="$EXE -coverageTest -test.coverprofile dfc$c.cov -test.outputdir $LOGROOT"
	fi

	if [ $c -eq 0 ]
	then
		export DFCPRIMARYPROXY="true"
		set -x
		$CMD $PROXY_PARAM&
		{ set +x; } 2>/dev/null
		unset DFCPRIMARYPROXY
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
	CONFDIR="$HOME/.dfc"
	CONFFILE="$CONFDIR/authn.json"
	set -x
	$GOPATH/bin/authn -config=$CONFFILE &
	{ set +x; } 2>/dev/null
fi
sleep 0.1
echo done
