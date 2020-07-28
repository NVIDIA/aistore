#!/bin/bash
set -e

#TODO: this is a workaround to work with VPNs
/opt/cisco/anyconnect/bin/vpn disconnect || true

source utils/deploy_minikube.sh
source utils/minikube_registry.sh

./utils/deploy_ais.sh
