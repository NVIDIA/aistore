#!/usr/bin/env bash

# TODO: Use NVIDIA-owned repository
REPOSITORY_NAME=liangdrew/dfc

docker pull $REPOSITORY_NAME
docker run -dit $REPOSITORY_NAME /bin/bash
container_id=`docker ps | grep $REPOSITORY_NAME | awk '{ print $1 }'`
docker exec -it $container_id /bin/bash -c "printf '1\n 1\n 0\n 1\n' | ./setup/deploy.sh; /bin/bash"

