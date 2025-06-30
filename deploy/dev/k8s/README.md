# Local/Development K8s Deployment

The local development deployment of AIStore on Kubernetes is a lightweight version that closely mirrors a [production deployment](https://github.com/NVIDIA/ais-k8s), providing a similar architecture and operational model while requiring fewer resources and simplifying setup for testing and prototyping.

This operator-less deployment uses [Kustomize](https://kustomize.io/) with [kapp](https://carvel.dev/kapp/) for deployment orchestration, leveraging kapp's [change groups](https://carvel.dev/kapp/docs/v0.45.0/apply-ordering/) to ensure components start in the correct order (`setup` → `proxy` → `target`). The base contains a complete, minimal (1-proxy, 1-target) deployment that can be used as-is or customized via overlays for modifications (see [bases and overlays](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/#bases-and-overlays)).

## Getting Started

### Prerequisites

This setup requires Linux. Before proceeding, ensure the following dependencies are installed:

- [Docker Engine](https://docs.docker.com/engine/): Required for running KinD clusters.

- [KinD](https://kind.sigs.k8s.io/#installation-and-usage): Used to create and manage the local Kubernetes cluster.

- [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl): CLI for interacting with the Kubernetes cluster.

- [`kapp`](https://carvel.dev/kapp/): Used to deploy and manage AIStore components within the Kubernetes cluster.

> **Note:** All Makefile targets work with Minikube. For Minikube, set `CLUSTER_TYPE=minikube`.

> **Note:** For macOS/Windows, see the [external access](#external-access) section below.

### Create K8s Cluster

Create a local Kubernetes cluster:

```bash
$ make create-cluster
```

### Deploy AIStore

Deploy the base AIStore configuration (1 proxy, 1 target):

```bash
$ make minimal
./utils/deploy.sh base
Target cluster 'https://127.0.0.1:41265' (nodes: kind-control-plane)

Changes

Namespace  Name                              Kind                Age  Op      Op st.  Wait to    Rs  Ri  
(cluster)  ais-rbac                          ClusterRoleBinding  -    create  -       reconcile  -   -  
^          ais                               Namespace           -    create  -       reconcile  -   -  
ais        ais-cluster-config-override       ConfigMap           -    create  -       reconcile  -   -  
^          ais-proxy                         Service             -    create  -       reconcile  -   -  
^          ais-proxy                         StatefulSet         -    create  -       reconcile  -   -  
^          ais-proxy-local-config-template   ConfigMap           -    create  -       reconcile  -   -  
^          ais-target                        Service             -    create  -       reconcile  -   -  
^          ais-target                        StatefulSet         -    create  -       reconcile  -   -  
^          ais-target-local-config-template  ConfigMap           -    create  -       reconcile  -   -  
^          ais                               ServiceAccount      -    create  -       reconcile  -   -  

Op:      10 create, 0 delete, 0 update, 0 noop, 0 exists
Wait to: 10 reconcile, 0 delete, 0 noop
...

Succeeded

To connect to your cluster, run one of the following:
  - export AIS_ENDPOINT=http://172.18.0.2:8080
  - source export_endpoint.sh
```

Once successfully deployed, use the AIStore [CLI](/docs/cli.md) to interact with the AIStore cluster:

```bash
$ export AIS_ENDPOINT=http://172.18.0.2:8080

$ ais show cluster
PROXY			 MEM USED(%)	 MEM AVAIL	 LOAD AVERAGE	 UPTIME	 STATUS
p[xpdr9787zw1nb][P]	 0.09%		 51.92GiB	 [1.2 1.4 1.2]	 5m0s	 online

TARGET		 MEM USED(%)	 MEM AVAIL	 CAP USED(%)	 CAP AVAIL	 LOAD AVERAGE	 REBALANCE	 UPTIME	 STATUS
t[rDCt9090]	 0.10%		 51.92GiB	 51%		 1.748TiB	 [1.2 1.4 1.2]	 -		 5m0s	 online

Summary:
   Proxies:		1
   Targets:		1
   Capacity:		used 475.65GiB (51%), available 447.38GiB
   Cluster Map:		version 4, UUID bbc3Rz7Qe, primary p[xpdr9787zw1nb]
   Software:		3.27.92a48a5 (build: 2025-04-10T00:25:45+0000)
   Deployment:		K8s
   Status:		2 online
   Rebalance:		n/a
   Authentication:	disabled
   Version:		3.27.92a48a5
   Build:		2025-04-10T00:25:45+0000
```

Alternatively, use the [Python SDK](/python/aistore/sdk/README.md) to interact with the AIStore cluster:

```python
from aistore.sdk.client import Client

client = Client("http://172.18.0.2:8080")
```

### Cleanup

To remove the AIStore deployment from the K8s cluster:

```bash
$ make cleanup
```

To delete the local K8s cluster created by `make create-cluster` entirely:

```bash
$ make delete-cluster
```

## Advanced Usage

### Further Customization

For any customization beyond the minimal base deployment, use Kustomize overlays to modify the configuration. The sample overlays shown below in `overlays/samples/` are meant to serve as examples and demonstrate some common customization patterns.

When adding new resources, use the appropriate `kapp.k14s.io/change-group` annotation (`setup`, `proxy`, or `target`) to ensure proper deployment ordering.

#### Multi-Node

The multi-node overlay scales the cluster to run 3 proxies and 3 targets for testing larger deployments:

```bash
$ make multi-node
```

> **Note:** The deployment requires that each node runs at most one proxy and one target. To run a multi-node cluster, ensure the cluster has at least as many nodes as the larger of the proxy or target StatefulSet replica counts. For guidance on running multi-node clusters, see [this guide for KinD](https://kind.sigs.k8s.io/docs/user/quick-start/#multi-node-clusters) and [this guide for Minikube](https://minikube.sigs.k8s.io/docs/tutorials/multi_node/).

#### HTTPS

The HTTPS sample overlay enables TLS using [cert-manager](https://cert-manager.io/) with a proper CA certificate chain.

First, install `cert-manager` in your cluster if not already installed:

```bash
$ kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.18.1/cert-manager.yaml
```

Then, deploy AIStore with HTTPS enabled:

```bash
$ make https
```

When HTTPs is enabled, the AIStore CLI and Python SDK will require a valid certificate to connect to the AIStore cluster.

To skip verification with the AIStore CLI:

```bash
$ ais config cli set cluster.skip_verify_crt true
```

To skip verification with the Python SDK:

```python
from aistore.sdk.client import Client
client = Client("https://172.25.0.5:8080", skip_verify=True)
```

Alternatively, export the cluster's certificate and use it for verification:

```bash
$ kubectl get secret ais-tls-certs -n ais -o jsonpath='{.data.tls\.crt}' | base64 --decode > tls.crt
```

#### Cloud Backends

The cloud sample overlay configures AWS and GCP backends because the default `aistorage/aisnode` image is built with support for both AWS and GCP cloud providers included.

To run a cluster with AWS and GCP cloud backends enabled, first replace the placeholders in `overlays/samples/cloud/kustomization.yaml` with proper literals/paths for AWS and GCP credentials.

Then, run the following command to deploy the cluster with cloud backends enabled:

```bash
$ make cloud
```

Verify the backend configuration is working:

```bash
$ ais bucket ls --all
NAME						 PRESENT
s3://ais		     no

Total: [AWS buckets: 1 (0 present)] ========

NAME			 PRESENT
gs://ais1	 no
gs://ais2	 no

Total: [GCP buckets: 2 (0 present)] ========

```

#### Local Development

The local development overlay uses locally built `aisnode` and `aisinit` images with the `local-development` tag:

```bash
$ make local-dev
```

The target first builds and loads the local images with the `local-development` tag, then deploys using the local development overlay.

#### External Access

The external access overlay demonstrates load balancer access for virtualized environments where direct cluster access is not possible (e.g. Docker Desktop). It creates a shared `ais-proxy-lb` LoadBalancer for all proxies and individual LoadBalancers `ais-target-*` for each target.

Run [`cloud-provider-kind`](https://github.com/kubernetes-sigs/cloud-provider-kind) in the background to provide external IPs, then deploy:

```bash
$ make external-access
```

> **Note:** For Minikube, run [`minikube tunnel`](https://minikube.sigs.k8s.io/docs/commands/tunnel/) in the background instead.

## References

- [Makefile](/deploy/dev/k8s/Makefile)
- [Local/Cluster Configuration](/docs/configuration.md)
- [Environment Variables](/docs/environment-vars.md)
- [Kustomize Documentation](https://kubectl.docs.kubernetes.io/references/kustomize/)
- [`kapp` Documentation](https://carvel.dev/kapp/)