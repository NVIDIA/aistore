#!/bin/bash

#TODO: this is a workaround, disconnect the vpn
/opt/cisco/anyconnect/bin/vpn disconnect

# removing minikube's registry-fwd container if preset
docker kill registry-fwd

# deletes any pre-existing minikube deployment
minikube delete

# we use docker as it is simple to use
minikube start --driver=docker

# this is to enable deployment as we have node selectors
kubectl label nodes minikube nodename=ais

# to allow running kubectl commands from within a pod (for e.g target)
kubectl apply -f minikube_perms.yaml

# making /var/lib/minikube/ais
minikube ssh 'sudo mkdir -p /var/lib/minikube/ais'


# mount binding /tmp to a persistent path
minikube ssh 'sudo mount --bind /var/lib/minikube/ais /tmp'

#creating directory for ais-fs
minikube ssh 'sudo mkdir -p /tmp/ais-k8s'

#enable registry
minikube addons enable registry

#map localhost:5000 to the registry of minikube
docker run --name registry-fwd --rm -d -it --network=host alpine ash -c "apk add socat && socat TCP-LISTEN:5000,reuseaddr,fork TCP:$(minikube ip):5000"
