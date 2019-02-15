# AIS helm chart 

## Overview

This repo includes all the definition of launching a AIS proxy and target on a K8s cluster.


```bash


PREREQUISITES
=============
One (and only one) of the nodes in the K8s cluster must have a label "initial_primary_proxy" with value "yes".  This can be set by command:
kubectl label nodes <A-node-name> initial_primary_proxy=yes

The key (default: initial_primary_proxy) and value (default: yes) can be customized by the values.yml, and label your node accordingly: 
    proxy.initialPrimaryProxyNodeLabel.name
    proxy.initialPrimaryProxyNodeLabel.value


TO INSTALL
==========
Usage: helm install --name=devops-ais .


TO DESTROY
==========
Usage: helm del --purge devops-ais 


```

