## Table of Contents

- [Table of Contents](#table-of-contents)
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Makefile](#makefile)
- [Files](#files)
- [Trying AIStore on minikube](#trying-aistore-on-minikube)
  - [Deploy](#deploy)
  - [Stopping and cleanup](#stopping-and-cleanup)
- [Developing AIStore on minikube](#developing-aistore-on-minikube)
  - [Deploy](#deploy-1)
  - [Local](#local)
  - [Stopping and cleanup](#stopping-and-cleanup-1)
- [Enabling HTTPS in Minikube for AIStore Deployment](#enabling-https-in-minikube-for-aistore-deployment)
- [Troubleshooting minikube](#troubleshooting-minikube)
- [Demo](#demo)

## Introduction

[Minikube](https://minikube.sigs.k8s.io/docs) allows one to run Kubernetes locally (like on your laptop) thus providing for testing out applications in a native Kubernetes environment.

* [Minikube GitHub](https://github.com/kubernetes/minikube)

Minikube can only be used for a single-node Kubernetes cluster. It supports a range of drivers like Virtualbox, KVM, Docker, etc.

Virtualbox and KVM require a hypervisor. However, if you choose to use docker on a Linux machine there is no need for a hypervisor.

Our [scripts](/deploy/dev/k8s) use Docker as the driver. This document shows how to deploy AIStore on minikube and use it for some of its features like ETL(transformers) etc. or to develop new features on it.

## Prerequisites

1. Have Linux/macOS installed on your Laptop/Machine/VirtualMachine.
2. [Install](https://docs.docker.com/engine/install/) Docker.
3. Ensure that your `$USER` is added to the `docker` group.

```console
$ sudo usermod -aG docker $USER && newgrp docker
```
4. Install [minikube](https://minikube.sigs.k8s.io/docs/start/).
5. Install [kubectl](https://kubernetes.io/docs/tasks/tools/).

**Notes:**
- Running minikube on a VPN might be problematic. For smoother operations, try running with VPN disabled. More information can be found [here](https://minikube.sigs.k8s.io/docs/handbook/vpn_and_proxy/).
- Running minikube with the Docker driver on newer macOS versions may encounter problems. Please refer to the [known issues](#known-issues) section for more details.

All commands below are running from the same directory i.e `${AISTORE_ROOT}/deploy/dev/k8s`

## [Makefile](/deploy/dev/k8s/Makefile)

| Command | Description |
| --- | --- |
| `make try` | start minikube and run a basic version of AIStore inside it ([ref](#trying-aistore-on-minikube)) |
| `make dev` | deploy multi-proxy/target AIStore with other utilities ([ref](#developing-aistore-on-minikube)) |
| `make minimal` | start a minimal AIStore deployment (1 proxy and 1 target) on an already running minikube |
| `make redeploy-ais` | skips the minikube and Docker registry deployment in `make dev` |
| `make minimal-local` | start minikube, rebuild the `aisnode` container with local changes and start a minimal deployment |
| `make redeploy-local` | rebuild the `aisnode` container with local changes on an already running minikube |
| `make stop-ais` | stop running AIStore pods  |
| `make stop`| stop and deletes AIStore pods, minikube cluster |

## Files
| File | Description |
| --- | --- |
| [Dockerfile](/deploy/dev/k8s/Dockerfile) | build AIStore node docker image on `Ubuntu 22.04` |

## Trying AIStore on minikube

### Deploy

You can start minikube with a basic version of AIStore (1-proxy and 1-target) running inside of it with the `make try` command.

```console
$ # navigate to this directory
$ # cd ${AISTORE_ROOT}/deploy/dev/k8s
$
$ make try
$
$ # you can also set the CPU and MEMORY allocated to minikube by
$ CPU=8 MEMORY=16000 make -e try
```


### Stopping and cleanup
To stop AIS nodes running inside of minikube, deleting all the pods and minikube itself, you can run the `stop` target of the [Makefile](/deploy/dev/k8s/Makefile).

```console
$ make stop
```

### Enabling HTTPS in Minikube for AIStore Deployment

To enable HTTPS for your AIStore deployment in Minikube, follow these steps:
#### For Minimal Deployment

To set up a minimal AIStore deployment with HTTPS, run the following command:

```bash
$ HTTPS=true make try
```

#### For Development Deployment

For a development deployment, follow these steps:

1. Run the `make dev` command:

```bash
$ make dev
```

2. During the setup process, you will be prompted to enable HTTPS. Respond with "y" to activate HTTPS:

```bash
Enable HTTPS: (y/n)?
y
```

With these steps, your Minikube-based AIStore deployment will be configured for secure HTTPS communication.

#### Testing Considerations for HTTPS

When working with HTTPS-enabled AIStore deployments, here are some important testing considerations:

#### 1. AIStore CLI Configuration

If you're using the AIStore CLI for testing, set the following environment variable to bypass certificate verification and connect to the AIStore cluster:

```bash
$ ais config cli set cluster.skip_verify_crt true
```

#### 2. Using `curl`

When interacting with your AIStore cluster over HTTPS using `curl`, use the `-k` flag to skip certificate validation:

```bash
$ curl -k https://your-ais-cluster-url
```

#### 3. Exporting the Self-Signed Certificate

If you prefer not to skip certificate validation when using `curl`, you can export the self-signed certificate for use:

```bash
$ kubectl get secret ais-tls-certs -o jsonpath='{.data.tls\.crt}' | base64 --decode > tls.crt
```

Now, you can use the exported `tls.crt` as a parameter when using `curl`, like this:

```bash
$ curl --cacert tls.crt https://your-ais-cluster-url
```

## Developing AIStore on minikube


### Deploy

The `dev` target of [Makefile](/deploy/dev/k8s/Makefile) allows you to customize and run AIStore by accepting inputs from the user through the terminal.

```console
$ make redeploy-ais
```

To update the aisnode images, or to redeploy AIStore, execute the `make ais-redeploy` argument, as shown below. This will skip the minikube and Docker registry deployment.

### Local

By default, the make targets will pull the `aisnode-minikube` image from [dockerhub](https://hub.docker.com/u/aistorage/). The `minimal-local` and `redeploy-local` options provide an easy way to test changes on a minimal cluster with a rebuilt local image by pushing the image to the local minikube registry. `make dev` also includes a prompt to rebuild the local image for more complex deployments.  

### Stopping and cleanup

The `stop-ais` target stops the AIStore cluster while keeping minikube still running.

```console
$ make stop-ais
```

For stopping and deleting the minikube cluster

```console
$ make stop
```

NOTE: If the default jupyter local directory was overwritten while deploying, ensure `JUPYTER_LOCAL_DIR` is pointing to the correct directory when performing a cleanup.

## Known Issues

See [known issues](https://minikube.sigs.k8s.io/docs/drivers/docker/#known-issues) with using Docker driver.

**macOS only:** One of the scripts to start AIS uses the `envsubst` command, which is not available by default. Please run the following commands as a workaround:
```
brew install gettext
brew link --force gettext
```
The ingress, and ingress-dns addons are currently only supported on Linux. See [issue](https://github.com/kubernetes/minikube/issues/7332).

## Demo

1. Deploying the cluster

```console
$ CPU=4 MEMORY=12000 make -e try 
minikube delete
🔥  Deleting "minikube" in docker ...
🔥  Deleting container "minikube" ...
🔥  Removing /home/abhgaikwad/.minikube/machines/minikube ...
💀  Removed all traces of the "minikube" cluster.
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
$ ais create ais://test-bucket
"test-bucket" bucket created
$ cat > sample
This is a sample data
```

4. Putting sample object

```console
$ ais put sample ais://test-bucket
PUT "sample" => ais://test-bucket/test-obj
```

5. Creating sample spec for transformer

```console
$ cat > spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: transformer-echo
  annotations:
    communication_type: "hpull://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_echo:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command: ["gunicorn", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"] 
      readinessProbe:
        httpGet:
          path: /health
          port: default
```

6. Initiating ETL

```console
$ ais etl init spec --name test-etl --from-file spec.yaml --comm-type hpull
ETL[test-etl]: job "etl-d3KIiNQ1-e"
```

7. Transforming an object

```console
$ ais etl --help
NAME:
   ais etl - use ETLs

USAGE:
   ais etl command [arguments...] [command options]

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
   ais etl object ETL_NAME BUCKET/OBJECT_NAME OUTPUT [command options]

OPTIONS:
   --help, -h  show help

$ ais etl object veSC9rvQQ test-bucket/test-obj out.txt
$ cat out.txt
This is a sample data
```
