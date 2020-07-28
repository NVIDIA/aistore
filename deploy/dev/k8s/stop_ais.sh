#!/bin/bash
set -e
echo "Stopping AIS Clusters"
kubectl delete pod -l type=aisproxy
kubectl delete pod -l type=aistarget
