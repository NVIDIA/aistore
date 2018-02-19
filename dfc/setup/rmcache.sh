#!/bin/bash
for i in $(find /tmp/dfc -name cloud); do
	for j in $(find $i -type f); do
		if [[ $j =~ \/\. ]];
		then
			echo $j
		else
			rm $j
		fi
	done
done
count=$(cat ~/.dfc/dfc1.json | grep count | awk -F'[: ,]' '{print $2}')
if [ $count -eq 0 ]; then
	echo "Cleaned up /tmp/dfc"
	echo "Warning: some or all of the configured fspaths may have retained cached files"
fi
