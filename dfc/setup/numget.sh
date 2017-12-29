#!/bin/bash
for i in $(find /tmp -name dfc.INFO); do
	echo $i
	cat $i | grep -P 'stats:.*[0-9]+' | awk -F'[, ]' '{print $2,$8,$9}'
	echo
done

