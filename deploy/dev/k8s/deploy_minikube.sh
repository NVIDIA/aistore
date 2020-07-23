#!/bin/bash

#TODO: this is a workaround, disconnect the vpn
/opt/cisco/anyconnect/bin/vpn disconnect

# deletes any pre-existing minikube deployment
minikube delete

# we use docker as it is simple to use
minikube start --driver=docker

source ais_minikube_setup.sh
source minikube_registry.sh
