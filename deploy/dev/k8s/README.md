## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Makefile](#Makefile)
- [Files](#files)
- [Trying AIStore on Minikube](#trying-aistore-on-minikube)
- [Developing AIStore on Minikube](#developing-aistore-on-minikube)
- [Troubleshooting Minikube](#troubleshooting-minikube)

## Introduction

[Minikube](https://minikube.sigs.k8s.io/docs) allows one to run Kubernetes in a local environment like your laptop for testing out applications in a native Kubernetes environment. It can only be used for single-node Kubernetes cluster. It supports a range of drivers like Virtualbox, KVM, Docker, etc. Virtualbox and KVM require a hypervisor.  However, if you choose to use docker on a Linux machine there is no need for a hypervisor.

Our [scripts](/deploy/dev/k8s) use Docker as the driver. This document shows how to deploy AIStore on Minikube and use it for some of its features like ETL(transformers) etc. or to develop new features on it.

## Prerequisites

1. Have Linux/macOS installed on your Laptop/Machine/VirtualMachine.
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

## [Makefile](/deploy/dev/k8s/Makefile)

| Command | Description |
| --- | --- |
| `make try` | start minikube and run a basic version of AIStore inside it ([ref](#trying-aistore-on-minikube)) |
| `make dev` | deploy multi-proxy/target AIStore with other utilities ([ref](#developing-aistore-on-minikube)) |
| `make minimal` | start a minimal AIStore deployment on already running Minikube|
| `redeploy-ais` | skips the Minikube and Docker registry deployment in `make dev` |
| `make stop-ais` | stop running AIStore pods  |
| `make stop`| stop and deletes AIStore pods, minikube cluster |

## Files
| File | Description |
| --- | --- |
| [Dockerfile-aisnode-ubuntu](Dockerfile-aisnode-ubuntu) | build AIStore node docker image on `Ubuntu 22.04` |
| [Dockerfile-aisnode-alphine](Dockerfile-aisnode-alphine) | build AIStore node docker image on `alphine` |
| [Dockerfile-datascience](Dockerfile-datascience) | build docker image for data science - jupyter notebook, pandas, pytorch, etc. |

## Trying AIStore on Minikube


### Deploy

You can start Minikube with a basic version of AIStore (1-proxy and 1-target) running inside of it with the `make try` command.

```console
$ # navigate to this directory 
$ # cd ${AISTORE_ROOT}/deploy/dev/k8s
$
$ make try
$ 
$ # you can also set the CPU and MEMORY allocated to Minikube by
$ CPU=8 MEMORY=16000 make -e try
```


### Stopping and cleanup
To stop AIS nodes running inside of Minikube, deleting all the pods and minikube itself, you can run the `stop` target of the [Makefile](/deploy/dev/k8s/Makefile).

```console
$ make stop
```

## Developing AIStore on Minikube


### Deploy

The `dev` target of [Makefile](/deploy/dev/k8s/Makefile) allows you to customize and run AIStore by accepting inputs from the user through the terminal.

```console
$ make redeploy-ais
```

To update the aisnode images, or to redeploy AIStore, execute the `make ais-redeploy` argument, as shown below. This will skip the Minikube and Docker registry deployment.

```console
$ ./dev.sh redeploy
```

### Data science stack

In addition to deploying an AIStore cluster, the `dev` target of [Makefile](/deploy/dev/k8s/Makefile) also provides an option to run data science stack. Developers can utilize it to run and test simple ML/DL pipelines against the AIStore cluster.

The data science stack comprises of utilities to interact with AIStore cluster, interactive jupyter notebook, `python3.8` environment with commonly used data science libraries, such as `scipy`, `matplotlib`, `numpy`, `pytorch`, etc.

Non-interactive command for deploying AIStore cluster with datas cience stack:

```console
$ JUPYTER_TOKEN="aistoretoken" make dev
```

In the above command, `JUPYTER_TOKEN` is the token used to access the jupyter notebook. By default, the jupyter notebook can be accessed over [http://localhost:8888] using the `JUPYTER_TOKEN`. However, the default port can be overwritten using the `JUPYTER_PORT` variable, as shown below:

```console
$ JUPYTER_PORT=8899 JUPYTER_TOKEN="aistoretoken" make dev
```

To prevent losing progress made on jupyter notebooks, a local directory (by default `./ais_datascience`) is mounted onto the docker container running jupyter notebooks where all the notebooks are persisted. The default local directory can be overwritten using the `JUPYTER_LOCAL_DIR` variable, as shown below:
```console
$ JUPYTER_LOCAL_DIR="/home/ubuntu/ais_datascience" JUPYTER_TOKEN="aistoretoken" make dev
``` 

### Stopping and cleanup

The `stop-ais` target stops the AIStore cluster while keeping Minikube still running.

```console
$ make stop-ais
```

For stopping and deleting the Minikube cluster

```console
$ make stop
```

NOTE: If the default jupyter local directory was overwritten while deploying, ensure `JUPYTER_LOCAL_DIR` is pointing to the correct directory when performing a cleanup.

## Troubleshooting Minikube

See [known issues](https://minikube.sigs.k8s.io/docs/drivers/docker/#known-issues) with using Docker driver.

## Demo

1. Deploying the cluster

```console
$ CPU=4 MEMORY=12000 make -e try 
minikube delete
🔥  Deleting "minikube" in docker ...
🔥  Deleting container "minikube" ...
🔥  Removing /home/abhgaikwad/.minikube/machines/minikube ...
💀  Removed all traces of the "minikube" cluster.
./utils/stop_datascience_container.sh
minikube config set cpus 4
❗  These changes will take effect upon a minikube delete and then a minikube start
minikube config set memory 12000
❗  These changes will take effect upon a minikube delete and then a minikube start
minikube start --driver=docker 
😄  minikube v1.26.0 on Ubuntu 20.04
✨  Using the docker driver based on user configuration
📌  Using Docker driver with root privileges
👍  Starting control plane node minikube in cluster minikube
🚜  Pulling base image ...
🔥  Creating docker container (CPUs=4, Memory=12000MB) ...
🐳  Preparing Kubernetes v1.24.1 on Docker 20.10.17 ...
    ▪ Generating certificates and keys ...
    ▪ Booting up control plane ...
    ▪ Configuring RBAC rules ...
🔎  Verifying Kubernetes components...
    ▪ Using image gcr.io/k8s-minikube/storage-provisioner:v5
🌟  Enabled addons: storage-provisioner, default-storageclass
🏄  Done! kubectl is now configured to use "minikube" cluster and "default" namespace by default
./minimal.sh
Checking kubectl default sa account...
serviceaccount/default created
clusterrolebinding.rbac.authorization.k8s.io/fabric8-rbac created
No resources found in default namespace.
pod/ais-proxy-0 created
Waiting for the primary proxy to be ready...
pod/ais-proxy-0 condition met
Starting target deployment...
pod/ais-target-0 created
Waiting for the targets to be ready...
pod/ais-target-0 condition met
List of running pods
NAME           READY   STATUS    RESTARTS   AGE    IP             NODE       NOMINATED NODE   READINESS GATES
ais-proxy-0    1/1     Running   0          101s   192.168.49.2   minikube   <none>           <none>
ais-target-0   1/1     Running   0          30s    192.168.49.2   minikube   <none>           <none>
Done.


Set the "AIS_ENDPOINT" for use of CLI:
export AIS_ENDPOINT="http://192.168.49.2:8080"

```

2. Exporting the AIS_ENDPOINT

```console
$ export AIS_ENDPOINT="http://192.168.49.2:8080"
```

3. Checking status

```console
$ kubectl get pods
NAME           READY   STATUS    RESTARTS   AGE
ais-proxy-0    1/1     Running   0          7m59s
ais-target-0   1/1     Running   0          6m48s
$ # ais is running
$ ais bucket create test-bucket
"test-bucket" bucket created
$ cat > sample
This is a sample data
```

4. Putting sample object

```console
$ ais object put sample test-bucket/test-obj
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
```

6. Initiating ETL

```console
$ ais etl init spec spec.yaml
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
   ais etl object [command options] ETL_ID BUCKET/OBJECT_NAME OUTPUT

OPTIONS:
   --help, -h  show help

$ ais etl object veSC9rvQQ test-bucket/test-obj out.txt
$ cat out.txt
This is a sample data
```
