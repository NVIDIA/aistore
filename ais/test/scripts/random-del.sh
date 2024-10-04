#!/bin/bash

#
# NOTE: internal use (local playground only)
#

aaa=($(find /tmp/ais -type f | grep %ob))
echo "Listed: ${#aaa[@]} objects"

while true
do
  sleep 1
  bbb=${aaa[$RANDOM % ${#aaa[@]}]}
  rm $bbb 2>/dev/null && echo "Deleted: $bbb"
done
