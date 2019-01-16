#!/bin/bash

LOGROOT="/tmp/dfc"
TMPFILE=$LOGROOT/dfc_cov

echo 'mode: count' > $TMPFILE
tail -q -n +2 $LOGROOT/*.cov >> $TMPFILE
go tool cover -html=$TMPFILE -o $LOGROOT/dfc_cov.html
rm $TMPFILE
