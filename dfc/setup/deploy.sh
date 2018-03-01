#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3] [-statstime=<DURATION>]
#
############################################

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
PROXYURL="http://localhost:8080"
PASSTHRU=true

# local daemon ports start from $PORT+1
PORT=8079

PROTO="tcp"
LOGLEVEL="3" # Verbosity: 0 (minimal) to 4 (max)
LOGROOT="/tmp/dfc"
###################################
#
# NOTE:
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/dfc/"
CLOUDBUCKETS="cloud"
LOCALBUCKETS="local"
LBCONF="localbuckets"
CONFPATH="$HOME/.dfc"
# CONFPATH="/etc/.dfc"
MAXCONCURRENTDOWNLOAD=64
MAXCONCURRENTUPLOAD=64
MAXPARTSIZE=4294967296
TESTFSPATHCOUNT=1
STATSTIME="10s"
HTTPTIMEOUT="60s"
KEEPALIVETIME="120s"
DONTEVICTIME="30m"
LOWWATERMARK=75
HIGHWATERMARK=90
NOXATTRS=false
LRUENABLED=true
VALIDATECOLDGET=true
H2C=false

PROXYPORT=$(expr $PORT + 1)
if lsof -Pi :$PROXYPORT -sTCP:LISTEN -t >/dev/null; then
	echo "Error: TCP port $PROXYPORT is not open (check if DFC is already running)"
	exit 1
fi

# (prelim and incomplete) test extended attrs
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
		echo "Sorry " $OS " not supported "
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
START=0
END=$servcount

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

mkdir -p $CONFPATH

for (( c=$START; c<=$END; c++ ))
do
	PORT=$(expr $PORT + 1)
	CONFFILE="$CONFPATH/dfc$c.json"
	LOGDIR="$LOGROOT/$c/log"
	cat > $CONFFILE <<EOL
{
	"logdir":			"$LOGDIR",
	"loglevel": 			"${LOGLEVEL}",
	"cloudprovider":		"${CLDPROVIDER}",
	"cloud_buckets":		"${CLOUDBUCKETS}",
	"local_buckets":		"${LOCALBUCKETS}",
	"lb_conf":                	"${LBCONF}",
	"stats_time":			"${STATSTIME}",
	"http_timeout":			"${HTTPTIMEOUT}",
	"keep_alive_time":		"${KEEPALIVETIME}",
	"listen": {
		"proto": 		"${PROTO}",
		"port":			"${PORT}"
	},
	"proxy": {
		"url": 			"${PROXYURL}",
		"passthru": 		${PASSTHRU}
	},
	"s3": {
		"maxconcurrdownld":	${MAXCONCURRENTDOWNLOAD},
		"maxconcurrupld":	${MAXCONCURRENTUPLOAD},
		"maxpartsize":		${MAXPARTSIZE}
	},
	"cksum_config": {
                 "validate_cold_get":             ${VALIDATECOLDGET}
	},
	"lru_config": {
		"lowwm":		${LOWWATERMARK},
		"highwm":		${HIGHWATERMARK},
		"dont_evict_time":      "${DONTEVICTIME}",
        "lru_enabled":  ${LRUENABLED}
	},
	"test_fspaths": {
		"root":			"${TESTFSPATHROOT}",
		"count":		$TESTFSPATHCOUNT,
		"instance":		$c
	},
	"fspaths": {
		"/tmp/dfc":	"",
		"/disk2/dfc":	""
	},
	"no_xattrs":			${NOXATTRS},
    "h2c": ${H2C}
}
EOL
done

# Set the following glog CLI to change the logging defaults:
#
# -logtostderr=false
#	Logs are written to standard error instead of to files.
# -alsologtostderr=false
#	Logs are written to standard error as well as to files.
# -stderrthreshold=ERROR
#	Log events at or above this severity are logged to standard
#	error as well as to files.

# build
go build && go install && GOBIN=$GOPATH/bin go install setup/dfc.go
if [ $? -ne 0 ]; then
	exit 1
fi

# run proxy and storage targets
for (( c=$START; c<=$END; c++ ))
do
	CONFFILE="$CONFPATH/dfc$c.json"
	if [ $c -eq 0 ]
	then
			set -x
			$GOPATH/bin/dfc -config=$CONFFILE -role=proxy -ntargets=$servcount $1 $2 &
			{ set +x; } 2>/dev/null
			# wait for the proxy to start up
			sleep 2
	else
			set -x
			$GOPATH/bin/dfc -config=$CONFFILE -role=target $1 $2 &
			{ set +x; } 2>/dev/null
	fi
done
sleep 2
echo done
