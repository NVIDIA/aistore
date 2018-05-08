#!/bin/bash
for i in $(find /tmp/dfc -name cloud 2>/dev/null); do
	for j in $(find $i -type f); do
		if [[ $j =~ \/\. ]];
		then
			echo $j
		else
			rm $j
		fi
	done
done
