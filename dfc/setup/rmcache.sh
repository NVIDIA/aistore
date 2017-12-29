#!/bin/bash
for i in $(find /tmp/nvidia -name cache); do
	for j in $(find $i -type f); do
		if [[ $j =~ \/\. ]];
		then
			echo $j
		else
			rm $j
		fi
	done
done
