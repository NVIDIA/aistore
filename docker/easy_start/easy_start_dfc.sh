#!/usr/bin/env bash

# TODO: Issue #5
REPOSITORY_NAME=liangdrew/dfc

docker pull $REPOSITORY_NAME
docker run -dit $REPOSITORY_NAME /bin/bash
container_id=`docker ps | grep $REPOSITORY_NAME | awk '{ print $1 }'`
docker exec -it $container_id /bin/bash -c "export PORT=${PORT:-8080} && printf '1\n 1\n 1\n 1\n' | ./setup/deploy.sh; /bin/bash"