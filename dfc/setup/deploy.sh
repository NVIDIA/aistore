#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3]
#
############################################

INSTANCEPREFIX="dfc"
TMPDIR="/tmp/nvidia"
CACHEDIR="cache"
LOGDIR="log"
PROXYURL="http://localhost:8080"
PASSTHRU=true

# Starting Portnumber
PORT=8079

# Starting ID
ID=0

PROTO="tcp"
CLDPROVIDER="amazon"
DIRPATH="/tmp/nvidia/"
# Verbosity: 0 (minimal) to 4 (max)
LOGLEVEL="3"
LOGDIR="/log"
CONFPATH="/etc/dfconf"
INSTANCEPREFIX="dfc"
MAXCONCURRENTDOWNLOAD=64
MAXCONCURRENTUPLOAD=64
MAXPARTSIZE=4294967296
CACHEDIR="/cache"
ERRORTHRESHOLD=5
FSCHECKFREQ=5
FSLOWWATERMARK=65
FSHIGHWATERMARK=80

echo Enter number of caching servers:
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

for (( c=$START; c<=$END; c++ ))
do
	ID=$(expr $ID + 1)
	PORT=$(expr $PORT + 1)
	CURINSTANCE=$INSTANCEPREFIX$c
	CONFFILE=$CONFPATH$c.json
	cat > $CONFFILE <<EOL
	{
		"id": 				"${ID}",
		"logdir":			"${DIRPATH}${CURINSTANCE}${LOGDIR}",
		"loglevel": 			"${LOGLEVEL}",
		"cloudprovider":		"${CLDPROVIDER}",
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
			"fscheckfreq":			${FSCHECKFREQ},
			"fslowwatermark":		${FSLOWWATERMARK},
			"fshighwatermark":		${FSHIGHWATERMARK}	
		}
	}
EOL
done

# Start Proxy Client and Storage Daemon, First Configuration file is used for Proxy 
# and subsequent one is used for Storage Server(s).
for (( c=$START; c<=$END; c++ ))
do
		CONFFILE=$CONFPATH$c.json
		if [ $c -eq 0 ]
		then
				set -x
				go run setup/dfc.go -configfile=$CONFFILE -role=proxy $1 $2 &
				{ set +x; } 2>/dev/null
				# wait for the proxy to start up
				sleep 2
		else
				set -x
				go run setup/dfc.go -configfile=$CONFFILE -role=server $1 $2 &
				{ set +x; } 2>/dev/null
		fi
done
sleep 2
echo done
