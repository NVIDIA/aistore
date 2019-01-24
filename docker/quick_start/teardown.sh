#!/bin/bash

container_id=`docker ps | grep aistorage/ais-quick-start | awk '{ print $1 }'`
docker rm --force $container_id
echo "AIStore cluster terminated"