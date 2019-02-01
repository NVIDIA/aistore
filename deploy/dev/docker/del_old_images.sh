#!/bin/bash

# the script deletes all obsolete images (they have name <none>) replaced by
# a newer ones

for s in `docker images | grep '<none>' | awk '{print $3}'`
do
	docker rmi $s
done
