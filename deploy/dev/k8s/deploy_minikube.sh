#!/bin/bash

source utils/pre_check.sh

#TODO: this is a workaround, disconnect the vpn
/opt/cisco/anyconnect/bin/vpn disconnect

# deletes any pre-existing minikube deployment
minikube delete

# we use docker as it is simple to use
minikube start --driver=docker

source utils/ais_minikube_setup.sh
source utils/minikube_registry.sh
