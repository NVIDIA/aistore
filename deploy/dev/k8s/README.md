## Disclaimer

For a more robust version, the development team has switched over to Docker. If you would like to deploy AIS in a containerized environment, consider using Docker. The Docker folder can be found in [`deploy/dev/docker`](/deploy/dev/docker).

## Minikube

Using [Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/) to deploy a Kubernetes cluster will also work for deploying single node clusters. Minikube cannot be used for multi-node deployments due to its limitations. Minikube supports a range of drivers including Virtualbox, KVM, docker etc. Virtualbox, KVM require a hypervisor.  However if you choose to use docker on a Linux machine there is no need for a hypervisor.

*Note:* You could face a problem when using Minikube on [VPN](https://minikube.sigs.k8s.io/docs/handbook/vpn_and_proxy/#vpn)


```console
$ ./deploy_minikube.sh
```

## MiniAis

`deploy_miniais.sh` is a script for deploying a miniature version (1-proxy, 1-target) of AIStore. It can be used to play around with features of
AIStore in a local environment.

### Usage

1. Install  [Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).

2. Run
```console
$ ./deploy_miniais.sh # Intializes the miniature AIStore application running in Minikube
```

3. AIStore can be accessed on `$(minikube ip):8080`. Configuring this on the `ais` [cli](https://github.com/NVIDIA/aistore/blob/master/cmd/cli/README.md), one can start using it.

4. For stopping the cluster
```console
$ ./stop_ais.sh
```

### Demo

1. Deploying the cluster
```
➜  k8s git:(mini-ais) ✗ ./deploy_miniais.sh**

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
Done
Please set the AIS_ENDPOINT for use of cli
export AIS_ENDPOINT="http://192.168.0.3:8080"
```

2. Exporting the AIS_ENDPOINT

```
➜  k8s git:(mini-ais) ✗ export AIS_ENDPOINT="http://192.168.0.3:8080"
```

3. Checking status

```
➜  k8s git:(mini-ais) ✗ kubectl get pods
NAME                             READY   STATUS    RESTARTS   AGE
ais-proxy                        1/1     Running   0          80s
ais-target                       1/1     Running   0          49s
tar2tf-ais-ais-target-minikube   1/1     Running   0          44s
➜  k8s git:(mini-ais) ✗ # ais is running
➜  k8s git:(mini-ais) ✗ ais create bucket test-bucket
"test-bucket" bucket created
➜  k8s git:(mini-ais) ✗ cat > sample
This is a sample data
^C
```

4. Putting sample object

```
➜  k8s git:(mini-ais) ✗ ais put sample test-bucket/test-obj
PUT "test-obj" into bucket "test-bucket"
```

5. Creating sample spec for transformer

```
➜  k8s git:(mini-ais) ✗ cat > spec.yaml
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

6. Initiating a transformer

```
➜  k8s git:(mini-ais) ✗ ais transform init spec.yaml
veSC9rvQQ
```

7. Transforming an object

```
➜  k8s git:(mini-ais) ✗ ais transform --help
NAME:
   ais transform - use transformations

USAGE:
   ais transform command [command options] [arguments...]

COMMANDS:
   init    initialize transformation with yaml spec
   ls      list all transformations
   stop    stop transformation with given id
   object  get transformed object

OPTIONS:
   --help, -h  show help

➜  k8s git:(mini-ais) ✗ ais transform object --help
NAME:
   ais transform object - get transformed object

USAGE:
   ais transform object [command options] TRANSFORM_ID BUCKET_NAME/OBJECT_NAME OUTPUT

OPTIONS:
   --help, -h  show help

➜  k8s git:(mini-ais) ✗ ais transform object veSC9rvQQ test-bucket/test-obj out.txt
➜  k8s git:(mini-ais) ✗ cat out.txt
This is a sample data
```

##  Kubeadm: Deploying an AIStore Cluster

For development purposes, we are going to use [Kubeadm](https://kubernetes.io/docs/reference/setup-tools/kubeadm/kubeadm/) to create a simple two node cluster with any number of proxies and any number of targets. Users can also decide to deploy AIS with a single-node.

<img src="/docs/images/k8s_arch.png" width="50%" >

### Prerequisites

AIStore on Kubernetes utilizes a [local Docker repository](https://docs.docker.com/registry/deploying/). This allows Kubernetes nodes to build the latests AIS version directly from the source without having to fetch from a remote registry. This guide uses your local machine as the local Docker registry host. You will also need Docker-Compose installed. For more information, see [Docker and Docker-Compose installation guide](/docs/docker_main.md)

### Creating a local Docker registry for local networks

1. Create a self signed certificate for your Docker registry. This will allow your Kubernetes nodes on the same network to pull images from the local Docker repository.

    ```console
    $ sudo vim /etc/ssl/openssl.cnf
    ```

    Add the line `subjectAltName=IP:<YOUR_IPv4_ADDRESS>` below the section labeled `[ v3_ca ]`.

2. Create a certificate directory

    ```console
    $ sudo mkdir /certs
    ```

3. Create the certificate

    ```console
    $ sudo openssl req \
      -newkey rsa:4096 -nodes -sha256 -keyout /certs/domain.key \
      -x509 -days 365 -out /certs/domain.crt
    ```

    You'll be prompted to fill out some information for your certificate. You should end up with two files `domain.crt` and `domain.key`.

4. To allow access for Docker to trust these certificates,

    ```console
    $ sudo mkdir -p /etc/docker/certs.d/<YOUR_IPv4_ADDRESS>:5000
    $ sudo cp /certs/domain.crt /etc/docker/certs.d/<YOUR_IPv4_ADDRESS>:5000/
    $ cd /etc/docker/certs.d/<YOUR_IPv4_ADDRESS>:5000/
    $ sudo mv domain.crt ca.crt
    ```

5. Reload the Docker daemon to use the new certificate

    ```console
    $ sudo service docker reload
    ```

6. On your other Kubernetes nodes, copy the `ca.crt` file to `etc/docker/certs.d/<YOUR_IPv4_ADDRESS>:5000`

    ```console
    $ sudo mkdir -p /etc/docker/certs.d/<YOUR_IPv4_ADDRESS>:5000
    ```

    Copy the contents of `ca.crt` to the directory and reload the Docker service with

    ```console
    $ sudo service docker reload
    ```

> If you change or create a new certificate, and you already have an AIS instance running on Kubernetes, running `sudo service docker reload` might not be enough to tell Docker to use your new certificate. You might also need to delete your local Docker registry (`docker ps` and look for the "registry" Docker container) and run `sudo ./deploy_kubernetes` again.

### Setup with Kubeadm

We can use our local machine to host our Kubernetes cluster. For multi-node clusters, Kubeadm requires VMs or other physical machines to host the nodes.

1. [Install Kubeadm](https://kubernetes.io/docs/setup/independent/install-kubeadm/) and [kubelet](https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/) and [kubectl](https://kubernetes.io/docs/reference/kubectl/overview/) tools.

    ```console
    $ sudo apt-get update && sudo apt-get install -y apt-transport-https curl
    $ curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
    $ sudo cat <<EOF >/etc/apt/sources.list.d/kubernetes.list
        deb https://apt.kubernetes.io/ kubernetes-xenial main
        EOF
    $ sudo apt-get update
    $ sudo apt-get install -y kubelet kubeadm kubectl
    $ sudo apt-mark hold kubelet kubeadm kubectl
    ```

2. [Install Docker](https://docs.docker.com/install/)

3. Disable swap memory. Kubeadm requires that swap memory is disabled.

    ```console
    $ sudo swapoff -a
    ```

4. Disable your firewall to allow DNS resolution for the pods in the Kubernetes cluster.

    ```console
    $ sudo ufw disable
    ```

> For multi-node Kubernetes clusters, you need perform the same steps on each machine

#### Deploying a Two-Node Cluster

1. Initialize the Kubernetes cluster with your local machine as the host.

    ```console
    $ sudo kubeadm init --pod-network-cidr=10.244.0.0/16
    ```

    The `--pod-network-cidr` allocates a CIDR for every node. This is a requirement by [flannel](https://github.com/coreos/flannel).

    > Flannel is a virtual network that gives a subnet to each node for use with container([pod](https://kubernetes.io/docs/concepts/workloads/pods/pod)) runtimes.

    > Ensure that Docker and Kubeadm are installed on all machines in the cluster

2. Once Kubeadm finishes initializing, a set of commands should appear on the screen.

    ```console
    $ mkdir -p $HOME/.kube
    $ sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
    $ sudo chown $(id -u):$(id -g) $HOME/.kube/conf
    ```

    This allows non-root users to use `kubectl`.

3. Configure the pod network add-on. This will be used to allow pods to communicate with each other.

    ```console
    $ kubectl apply -f https://raw.githubusercontent.com/coreos/flannel/bc79dd1505b0c8681ece4de4c0d86c5cd2643275/Documentation/kube-flannel.yml
    ```

    Also make sure that `/proc/sys/net/bridge/bridge-nf-call-iptables` is set to `1` by running

    ```console
    $ sudo sysctl net.bridge.bridge-nf-call-iptables=1
    ```

4. You should have received a command with the form,

    ```console
    $ kubeadm join <MASTER_NODE_IP> --token <TOKEN> --discovery-token-ca-cert-hash <HASH>
    ```

    Use this to join the nodes to your cluster.

5. Once that is setup, you should see that your Kubernetes nodes has been created. Check with

    ```console
    $ kubectl get nodes
    ```

    The name of the nodes should be the the name of your machines that they are running on. If the status is `NotReady`, wait for a couple of seconds and check again.

6. Once the status is `Ready`, add labels to your nodes and untaint the master node.

   * Add a label:

        ```console
        # Assigns the node to host the proxy Pod
        $ kubectl label node <YOUR_NODE_NAME_HERE> nodename=ais-proxy

        # Assigns the node to host the target Pods
        $ kubectl label node <YOUR_NODE_NAME_HERE> nodename=ais-target
        ```
        This allows the pods to be assigned to particular nodes.

        > For a single node cluster, assign the nodename to be 'ais'

   * Untaint the node:

        ```console
        $ kubectl taint nodes --all node-role.kubernetes.io/master-
        ```
        This allows for pods to be scheduled on the host/master node.

7. Run the deployment script `sudo ./deploy_kubernetes.sh`.

    > This is still under development. It currently only works with one proxy and any number of targets.
    > If the pod is stuck on `ContainerCreating` status, you can check the logs using `kubectl describe pods <POD_NAME>`

8. After a minute, you should see that the pods are running with `kubectl get pods -o wide`.

    ```console
    $ kubectl get pods -o wide
    NAME                               READY   STATUS    RESTARTS   AGE   IP           NODE           NOMINATED NODE   READINESS GATES
    aisprimaryproxy-77456674db-6fzq5   1/1     Running   0          89s   10.244.0.4   ais-master     <none>           <none>
    aistarget-5b6c698c8-f87mm          1/1     Running   0          39s   10.244.1.1   ais-worker1    <none>           <none>
    ```

9. You can also scale the number of storage targets.

    ```console
    $ kubectl scale --replicas=<REPLICA_COUNT> -f aistarget_deployment.yml
    ```

Setting the number of replicas to `6` will create six targets.


#### Interacting with the Cluster

To interact with the cluster, install the `AIS` CLI tool

```console
$ cd ../../../cmd/cli
$ ./install.sh
```

This will create a binary named `ais` that can be used to interact with the cluster.
Configure the CLI tool to point to the Kubernetes cluster by assigning the `AIS_ENDPOINT` environment variable to the URL of the primary proxy.

Example:

```console
$ export AIS_ENDPOINT=http://10.244.0.4:8080
```

For the list of available commands, see [here](/cmd/cli/README.md).

 * To run tests, SSH into the primary proxy

```console
$ kubectl exec -it <AIS_PRIMARY_PROXY_NAME> -- /bin/bash
```

Once inside the container, go to the `$WORKDIR`

```console
$ cd $WORKDIR
```

From there you can run tests using,

```console
$ CGO_ENABLED=0 BUCKET=yourS3Bucket go test ./tests -v -count=1
```

This runs the entire test suite. To run specific tests, use the [`-run`](https://golang.org/pkg/testing/#hdr-Subtests_and_Sub_benchmarks) tag.

#### Stopping the Cluster

1. Run `./stop_kubernetes.sh` to teardown the deployments.

2. To teardown the Kubernetes cluster:

    ```console
    $ sudo kubeadm reset
    ```

#### Useful Kubernetes commands

1. To list all the running pods

    ```console
    $ kubectl get pods -o wide
    NAME                                READY     STATUS    RESTARTS   AGE      IP           NODE
    aisprimaryproxy-77456674db-6fzq5    1/1       Running   0          89s      10.244.0.4   ais-master
    aistarget-5b6c698c8-f87mm           1/1       Running   0          39s      10.244.1.1   ais-worker1
    ```

2. To view pod logs, run `kubectl logs <pod_name>`

    ```console
    $ kubectl logs aistarget-5b6c698c8-bdgc5
    I 18:57:42.232529 config.go:447 Logdir: "/tmp/ais/log" Proto: tcp Port: 8080 Verbosity: 3
    I 18:57:42.232823 config.go:449 Config: "/aisconfig/ais.json" StatsTime: 10s
    I 18:57:42.232864 daemon.go:194 version: 6d9c095 | build_time: 2019-02-01T18:57:39+0000
    I 18:57:42.233194 utils.go:108 Found only one IPv4: 10.244.0.50, MTU 1450
    W 18:57:42.233208 utils.go:110 IPv4 10.244.0.50 MTU size is small: 1450
    I 18:57:42.233407 httpcommon.go:291 Configured PUBLIC NETWORK address: [10.244.0.50:8080] (out of: )
    I 18:57:42.235359 daemon.go:245 Warning: configuring 1 fspaths for testing
    I 18:57:42.237140 keepalive.go:377 Starting targetkeepalive
    I 18:57:42.237150 iostat_linux.go:91 Starting iostat
    I 18:57:42.237182 fshc.go:88 Starting fshc
    I 18:57:42.237162 atime.go:151 Starting atime
    I 18:57:42.237292 common_stats.go:192 Starting storstats
    I 18:57:42.237447 mem.go:890 Starting gmem2, minfree 1.25GiB, low 3.76GiB, timer 2m0s
    I 18:57:42.237468 mem.go:890 gmem2: free memory 27.07GiB > 80% total
    I 18:57:42.242928 clustermap.go:314 registered smap-listener aistarget-5b6c698c8-bdgc52019-02-01T18:57:39+0000=>public/rebalance
    I 18:57:42.242955 target.go:261 target aistarget-5b6c698c8-bdgc52019-02-01T18:57:39+0000 is ready
    I 18:57:42.256547 mem.go:890 Starting ec, minfree 2.71GiB, low 8.12GiB, timer 2m0s
    I 18:57:42.256565 mem.go:890 ec: free memory 27.07GiB > 80% total
    I 18:57:42.901475 httpcommon.go:980 receive Smap v1 (local v0), ntargets 1, action early-start-have-registrations
    I 18:57:42.901507 target.go:3344 receive Smap: v1, ntargets 1, primary aisprimaryproxy-77456674db-6mvkb2019-02-01T18:56:48+0000, action early-start-have-registrations
    I 18:57:42.901517 target.go:3349 target: aistarget-5b6c698c8-bdgc52019-02-01T18:57:39+0000 <= self
    I 18:57:42.903763 target.go:3288 receive bucket-metadata: version 1, action early-start-have-registrations
    I 18:57:43.910116 httpcommon.go:980 receive Smap v2 (local v1), ntargets 1, action primary-started-up
    I 18:57:43.910145 target.go:3344 receive Smap: v2, ntargets 1, primary aisprimaryproxy-77456674db-6mvkb2019-02-01T18:56:48+0000, action primary-started-up
    I 18:57:43.910156 target.go:3349 target: aistarget-5b6c698c8-bdgc52019-02-01T18:57:39+0000 <= self
    I 18:57:44.244925 target.go:3432 cluster started up
    I 18:57:52.237593 {"up.μs.time":10000264}
    I 18:57:52.241998 sda, 0.00, 8.00, 0.00, 0.03, 0.00, 0.00, 0.00, 0.00, 0.00, 0.25, 0.00, 0.00, 3.50, 0.25, 0.20
    I 18:58:02.237536 {"kalive.μs":858,"up.μs.time":20000268}
    I 18:58:02.241988 sda, 0.00, 8.00, 0.00, 0.03, 0.00, 0.00, 0.00, 0.00, 0.00, 0.25, 0.00, 0.00, 3.50, 0.25, 0.20
    I 18:58:12.237533 {"kalive.μs":788,"up.μs.time":30000267}
    I 18:58:12.241953 sda, 0.00, 8.00, 0.00, 0.04, 0.00, 3.50, 0.00, 30.43, 0.00, 0.50, 0.00, 0.00, 5.75, 0.50, 0.40
    I 18:58:22.237577 {"kalive.μs":800,"up.μs.time":40000290}
    I 18:58:22.242014 sda, 0.00, 9.50, 0.00, 0.08, 0.00, 10.50, 0.00, 52.50, 0.00, 0.63, 0.01, 0.00, 8.42, 0.63, 0.60
    I 18:58:32.237538 {"kalive.μs":829,"up.μs.time":50000281}
    ```

3. To ssh into a pod

    ```console
    $ kubectl exec <pod_name> -i -t /bin/bash
    ```

    Example:
    ```console
    $ kubectl exec aisproxy-84999457d6-8qk6x -i -t /bin/bash
    ```
