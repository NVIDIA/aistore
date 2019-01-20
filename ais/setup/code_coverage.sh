#!/bin/bash

LOGROOT="/tmp/ais"
TMPFILE=$LOGROOT/ais_cov

echo 'mode: count' > $TMPFILE
tail -q -n +2 $LOGROOT/*.cov >> $TMPFILE
go tool cover -html=$TMPFILE -o $LOGROOT/ais_cov.html
rm $TMPFILE
