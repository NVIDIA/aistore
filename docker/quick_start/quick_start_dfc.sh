#!/usr/bin/env bash

#TODO: #5
container_id=`docker ps | grep liangdrew/dfc | awk '{ print $1 }'`
docker exec -it $container_id /bin/bash -c "export PORT=${PORT:-8080} && printf '1\n 1\n 1\n 1\n' | ./setup/deploy.sh; /bin/bash"