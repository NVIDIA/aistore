#!/bin/bash

############################################
#
# Usage: addlocal.sh [-loglevel=0|1|2|3] [-statstime=<DURATION>]
#
############################################

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
INSTANCEPREFIX="dfc"
PORT=8079
LOGLEVEL="3"
LOGDIR="/log"
CONFPATH="$HOME/.dfc"

echo Enter local index of the new cache server:
read idx
if ! [[ "$idx" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$idx' is not a number"; exit 1
fi

firstidx=1
FIRST="$INSTANCEPREFIX$firstidx"
FIRSTPORT=$(expr $PORT + $firstidx + 1)
NEW="$INSTANCEPREFIX$idx"
NEWPORT=$(expr $PORT + $idx + 1)

if lsof -Pi :$NEWPORT -sTCP:LISTEN -t >/dev/null; then
	echo "Error: TCP port $NEWPORT is not open"
	echo "(run 'lsof' or 'netstat' to see which ports are available)"
	exit 1
fi

cd $CONFPATH
cp "${FIRST}.json" "$NEW.js"
eval sed -i 's/$FIRSTPORT/$NEWPORT/' *.js
eval sed -i 's/$FIRST/$NEW/' *.js

mv "$NEW.js" "$NEW.json"
cd -
CONFFILE="$CONFPATH/$NEW.json"

set -x
go run setup/dfc.go -configfile=$CONFFILE -role=target $1 $2 &
{ set +x; } 2>/dev/null
sleep 1
echo done
