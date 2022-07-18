#!/bin/bash

set -e

{ echo n; echo n; } | ./utils/deploy_minikube.sh

# NOTE: 6 x n (4 remote providers + local registry + datascience stack)
{ echo 1; echo 1; echo 1; echo 1; echo n; echo n; echo n; echo n; echo ; echo n; } | ./utils/deploy_ais.sh
