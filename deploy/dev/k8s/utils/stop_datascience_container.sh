#!/bin/bash
set -e

# Kill the data science container
if [[ "$(docker ps -q -f name=ais_datascience)" ]]; then
  docker container rm -f ais_datascience  || true
  echo "Would you like to cleanup the local volume? [y/n]"
  read -r delete_vol
  if [[ "$delete_vol" == "y" ]]; then
    export JUPYTER_LOCAL_DIR=${JUPYTER_LOCAL_DIR:-`pwd`/ais_datascience}
    if [[ -d "$JUPYTER_LOCAL_DIR" ]]; then
      rm -rf $JUPYTER_LOCAL_DIR
    else
      echo "Skipping clean up! ${JUPYTER_LOCAL_DIR} doesn't exist!"
    fi
  fi
fi
