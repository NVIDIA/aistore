#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3] [-statstime=<DURATION>]
#
############################################

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
INSTANCEPREFIX="dfc"
TMPDIR="/tmp/nvidia"
CACHEDIR="cache"
LOGDIR="log"
PROXYURL="http://localhost:8080"
PASSTHRU=true

# local daemon ports start from $PORT+1
PORT=8079

# Starting ID
ID=0

PROTO="tcp"
DIRPATH="/tmp/nvidia/"
# Verbosity: 0 (minimal) to 4 (max)
LOGLEVEL="3"
LOGDIR="/log"
CONFPATH="$HOME/.dfc"
# CONFPATH="/etc/.dfc"
INSTANCEPREFIX="dfc"
MAXCONCURRENTDOWNLOAD=64
MAXCONCURRENTUPLOAD=64
MAXPARTSIZE=4294967296
CACHEDIR="/cache"
ERRORTHRESHOLD=5
STATSTIMESEC=10
HTTPTIMEOUTSEC=60
DONTEVICTIMESEC=600
FSLOWWATERMARK=65
FSHIGHWATERMARK=80

PROXYPORT=$(expr $PORT + 1)
if lsof -Pi :$PROXYPORT -sTCP:LISTEN -t >/dev/null; then
	echo "Error: TCP port $PROXYPORT is not open (check if DFC is already running)"
	exit 1
fi

echo Enter number of cache servers:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$servcount' is not a number"; exit 1
fi
START=0
END=$servcount

echo Enter number of mount points per server:
read mntpointcount
if ! [[ "$mntpointcount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$mntpointcount' is not a number"; exit 1
fi
CACHEPATHCOUNT=$mntpointcount

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
# convert all timers to seconds
let "STATSTIMESEC=$STATSTIMESEC*10**9"
let "HTTPTIMEOUTSEC=$HTTPTIMEOUTSEC*10**9"
let "DONTEVICTIMESEC=$DONTEVICTIMESEC*10**9"

mkdir -p $CONFPATH

for (( c=$START; c<=$END; c++ ))
do
	ID=$(expr $ID + 1)
	PORT=$(expr $PORT + 1)
	CURINSTANCE="$INSTANCEPREFIX$c"
	CONFFILE="$CONFPATH/$CURINSTANCE.json"
	cat > $CONFFILE <<EOL
	{
		"logdir":			"${DIRPATH}${CURINSTANCE}${LOGDIR}",
		"loglevel": 			"${LOGLEVEL}",
		"cloudprovider":		"${CLDPROVIDER}",
		"stats_time":			${STATSTIMESEC},
		"http_timeout":			${HTTPTIMEOUTSEC},
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
		"cache": {
			"cachepath":			"${DIRPATH}${CURINSTANCE}${CACHEDIR}",
			"cachepathcount":		${CACHEPATHCOUNT},
			"errorthreshold":		${ERRORTHRESHOLD},
			"fslowwatermark":		${FSLOWWATERMARK},
			"fshighwatermark":		${FSHIGHWATERMARK},
			"dont_evict_time":		${DONTEVICTIMESEC}
		}
	}
EOL
done

# run proxy and storage targets
for (( c=$START; c<=$END; c++ ))
do
	CURINSTANCE="$INSTANCEPREFIX$c"
	CONFFILE="$CONFPATH/$CURINSTANCE.json"
	if [ $c -eq 0 ]
	then
			set -x
			go run setup/dfc.go -config=$CONFFILE -role=proxy $1 $2 &
			{ set +x; } 2>/dev/null
			# wait for the proxy to start up
			sleep 2
	else
			set -x
			go run setup/dfc.go -config=$CONFFILE -role=target $1 $2 &
			{ set +x; } 2>/dev/null
	fi
done
sleep 2
echo done
