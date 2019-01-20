#!/bin/bash
for i in $(find /tmp -name ais.INFO); do
	echo $i
	cat $i | grep -P 'stats:.*[0-9]+' | awk -F'[, ]' '{print $2,$9,$10}'
	echo
done

