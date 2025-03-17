#!/bin/bash

EXTERNAL_IP=$(kubectl get pod ais-proxy-0 -o jsonpath='{.status.hostIP}')
EXTERNAL_PORT=$(kubectl get pod ais-proxy-0 -o jsonpath='{.spec.containers[0].ports[0].hostPort}')
export AIS_ENDPOINT="http://$EXTERNAL_IP:$EXTERNAL_PORT"