#!/bin/bash
for i in $(find /tmp -name dfc.INFO); do
	echo $i
	cat $i | grep -P 'stats:.*[0-9]+' | sed 's/[{}]/ /g' | awk '{print $2 "," $6}'
	echo
done

