## Introduction

[Minikube](https://kubernetes.io/docs/setup/learning-environment/minikube/) allows one to run Kubernetes in a local environment like your laptop for testing out applications in a native Kubernetes envrironment. It can only be used for single-node Kubernetes cluster. It supports a range of drivers like Virtualbox, KVM, Docker etc. Virtualbox, KVM require a hypervisor.  However, if you choose to use docker on a Linux machine there is no need for a hypervisor.

Our [scripts](/deploy/dev/k8s) use Docker as the driver. This document shows how to deploy AIStore on Minikube and use it for some of its features like ETL(transformers) etc. or to develop new features on it.

## Prerequisites

1. Have Linux/MacOS installed on your Laptop/Machine/VirtualMachine.
2. [Install](https://docs.docker.com/engine/install/) Docker.
3. Ensure that your `$USER` is added to the `docker` group and re-login.

```console
$ sudo usermod -aG docker $USER && newgrp docker
$ # relogin if needed
$ sudo service docker start
```

>  **Note:  Running Minikube on a VPN might be [problematic](https://minikube.sigs.k8s.io/docs/handbook/vpn_and_proxy/). For smoother
   operations try running with VPN disabled.**

All commands below are running from the same directory i.e `${AISTORE_ROOT}/deploy/dev/k8s`

## Trying AIStore on Minikube


### Deploy

The script [try.sh](try.sh) starts a basic and limited version (1-proxy and 1-target) of AIStore.

```console
$ ./try.sh
```


### Stopping and cleanup

The script [stop.sh](stop.sh) stops the running Minikube instance.

```console
$ ./stop.sh
```

## Developing AIStore on Minikube


### Deploy

The script [dev.sh](dev.sh) starts a development version of AIStore on Minikube.

```console
$ ./dev.sh
```

It's possible to pass input to the command, so no interaction is required:

```console
$ ./dev.sh <<< $'n\ny\n1\n1\n1\n2\nn\nn\nn\ny'
```

### Stopping and cleanup

The script [stop_ais.sh](stop_ais.sh) stops the AIStore cluster while keeping Minikube still running.

```console
$ ./stop_ais.sh
```

For stopping and deleting the Minikube cluster

```console
$ ./stop.sh
```

## Troubleshooting Minikube

See [known issues](https://minikube.sigs.k8s.io/docs/drivers/docker/#known-issues) with using Docker driver.

## Demo

1. Deploying the cluster

```console
$ ./try.sh
🔥  Deleting "minikube" in docker ...
🔥  Deleting container "minikube" ...
🔥  Removing /home/mj/.minikube/machines/minikube ...
💀  Removed all traces of the "minikube" cluster.
😄  minikube v1.11.0 on Ubuntu 20.04
✨  Using the docker driver based on user configuration
👍  Starting control plane node minikube in cluster minikube
🔥  Creating docker container (CPUs=2, Memory=7900MB) ...
🐳  Preparing Kubernetes v1.18.3 on Docker 19.03.2 ...
    ▪ kubeadm.pod-network-cidr=10.244.0.0/16
🔎  Verifying Kubernetes components...
🌟  Enabled addons: default-storageclass, storage-provisioner
🏄  Done! kubectl is now configured to use "minikube"
clusterrolebinding.rbac.authorization.k8s.io/fabric8-rbac created
secret/aws-credentials created
Starting kubernetes deployment...
Deploying proxy
pod/ais-proxy created
Waiting for the primary proxy to be ready...
error: timed out waiting for the condition on pods/ais-proxy
Deploying target
pod/ais-target created
List of running pods
NAME         READY   STATUS              RESTARTS   AGE   IP            NODE       NOMINATED NODE   READINESS GATES
ais-proxy    0/1     ContainerCreating   0          31s   192.168.0.3   minikube   <none>           <none>
ais-target   0/1     Pending             0          0s    <none>        minikube   <none>           <none>
Done.

Please set the "AIS_ENDPOINT" for use of CLI:
export AIS_ENDPOINT="http://192.168.0.3:8080"
```

2. Exporting the AIS_ENDPOINT

```console
$ export AIS_ENDPOINT="http://192.168.0.3:8080"
```

3. Checking status

```console
$ kubectl get pods
NAME                             READY   STATUS    RESTARTS   AGE
ais-proxy                        1/1     Running   0          80s
ais-target                       1/1     Running   0          49s
$ # ais is running
$ ais create bucket test-bucket
"test-bucket" bucket created
$ cat > sample
This is a sample data
^C
```

4. Putting sample object

```console
$ ais put sample test-bucket/test-obj
PUT "test-obj" into bucket "test-bucket"
```

5. Creating sample spec for transformer

```console
$ cat > spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: transformer-echo
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "hrev://"
    wait_timeout: 15s
spec:
  containers:
    - name: server
      image: aistore/transformer_echo:latest
      imagePullPolicy: Always
      ports:
        - containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
^C
```

6. Initiating ETL

```console
$ ais etl init spec.yaml
veSC9rvQQ
```

7. Transforming an object

```console
$ ais etl --help
NAME:
   ais etl - use ETLs

USAGE:
   ais etl command [command options] [arguments...]

COMMANDS:
   init    initialize ETL with yaml spec
   build   build ETL with provided code, optional dependencies and runtime
   ls      list all ETLs
   logs    retrieve logs produced by ETL
   stop    stop ETL with given id
   object  transform object with given ETL
   bucket  offline transform bucket with given ETL

OPTIONS:
   --help, -h  show help

$ ais etl object --help
NAME:
   ais etl object - transform object with given ETL

USAGE:
   ais etl object [command options] ETL_ID BUCKET_NAME/OBJECT_NAME OUTPUT

OPTIONS:
   --help, -h  show help

$ ais etl object veSC9rvQQ test-bucket/test-obj out.txt
$ cat out.txt
This is a sample data
```
