#!/usr/bin/env bash

container_id=`docker ps | grep aistorage/dfc-quick-start | awk '{ print $1 }'`
docker exec -it $container_id /bin/bash -c "export PORT=${PORT:-8080} && printf '1\n 1\n 1\n 1\n' | ./setup/deploy.sh; /bin/bash"
