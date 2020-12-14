#!/bin/bash
set -e
# TODO: Move user `./stop.sh` providing options, like `./stop.sh aistore`.
echo "Stopping AIS Clusters"
kubectl delete pod -l type=aisproxy
kubectl delete pod -l type=aistarget
