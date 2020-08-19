#!/bin/bash
set -e

{ echo n; } | ./utils/deploy_minikube.sh

{ echo 1; echo 1; echo 1; echo 3; echo n; echo n; echo n;echo n;  } | ./utils/deploy_ais.sh
