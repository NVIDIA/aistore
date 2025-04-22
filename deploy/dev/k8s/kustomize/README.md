# Local/Development K8s Deployment

The local development deployment of AIStore on Kubernetes is a lightweight version that closely mirrors a [production deployment](https://github.com/NVIDIA/ais-k8s), providing a similar architecture and operational model while requiring fewer resources and simplifying setup for testing and prototyping.

Running a local development deployment of AIStore on Kubernetes requires a local Kubernetes cluster solution such as [Minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download) or [KinD](https://kind.sigs.k8s.io/docs/user/quick-start/); for the purposes of this guide, KinD will be used, but all Makefile targets can be used with either Minikube or KinD. **For Minikube, use the environment variable `CLUSTER_TYPE` and set it to `minikube`.**

## Prerequisites

This setup requires Linux. Before proceeding, ensure the following dependencies are installed:

- [Docker Engine](https://docs.docker.com/engine/): Required for running KinD clusters.

- [KinD](https://kind.sigs.k8s.io/#installation-and-usage): Used to create and manage the local Kubernetes cluster.

- [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl): Kubernetes command-line tool for interacting with the cluster.

## Getting Started

### Create KinD Cluster

To start, run the following to create a minimal KinD cluster:

```bash
$ make create-cluster 
./utils/create_cluster.sh
Creating cluster "kind" ...
 ✓ Ensuring node image (kindest/node:v1.32.2) 🖼
 ✓ Preparing nodes 📦  
 ✓ Writing configuration 📜 
 ✓ Starting control-plane 🕹️ 
 ✓ Installing CNI 🔌 
 ✓ Installing StorageClass 💾 
Set kubectl context to "kind-kind"
You can now use your cluster with:

kubectl cluster-info --context kind-kind

Not sure what to do next? 😅  Check out https://kind.sigs.k8s.io/docs/user/quick-start/
Kubernetes control plane is running at https://127.0.0.1:38003
CoreDNS is running at https://127.0.0.1:38003/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
```

### Deploy Minimal AIStore Cluster

Next, deploy a minimal AIStore cluster (1 proxy and 1 target) on the KinD cluster:

```bash
$ make minimal
./utils/deploy_ais.sh base/common base/proxy base/target
clusterrolebinding.rbac.authorization.k8s.io/ais-rbac created
configmap/ais-cluster-config-override created
configmap/ais-proxy-local-config-template created
service/ais-proxy created
statefulset.apps/ais-proxy created
pod/ais-proxy-0 condition met
configmap/ais-target-local-config-template created
service/ais-target created
statefulset.apps/ais-target created
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...

To connect to the cluster: export AIS_ENDPOINT=http://172.18.0.2:8080
```

Verify that the cluster is up and running:

```bash
$ kubectl get pods
NAME           READY   STATUS    RESTARTS   AGE
ais-proxy-0    1/1     Running   0          90s
ais-target-0   1/1     Running   0          71s
```

### Interacting w/ AIStore

Use the AIStore [CLI](/docs/cli.md) to interact with the AIStore cluster:

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

To clean up and undeploy the minimal AIStore cluster, run:

```bash
$ make minimal-cleanup
./utils/cleanup_ais.sh base/common base/proxy base/target
clusterrolebinding.rbac.authorization.k8s.io "ais-rbac" deleted
configmap "ais-cluster-config-override" deleted
configmap "ais-proxy-local-config-template" deleted
service "ais-proxy" deleted
statefulset.apps "ais-proxy" deleted
configmap "ais-target-local-config-template" deleted
service "ais-target" deleted
statefulset.apps "ais-target" deleted
job.batch/node-cleanup-kind-control-plane created
```

To delete the KinD cluster itself, run:

```bash
$ make delete-cluster
./utils/delete_cluster.sh
Deleting cluster "kind" ...
Deleted nodes: ["kind-control-plane"]
```

## Further Customizations

To further customize the deployment, simply define an overlay.

For example, to deploy a cluster with 2 proxies and 3 targets, create an overlay that modifies the number of replicas for the respective StatefulSets:

```bash
$ mkdir -p overlays/custom-deployment/proxy

$ cat <<EOF > overlays/custom-deployment/proxy/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ais-proxy
spec:
  replicas: 2
EOF

$ cat <<EOF > overlays/custom-deployment/proxy/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
- ../../../base/proxy

patches:
- path: statefulset.yaml
EOF

$ mkdir -p overlays/custom-deployment/target

$ cat <<EOF > overlays/custom-deployment/target/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ais-target
spec:
  replicas: 3
EOF

$ cat <<EOF > overlays/custom-deployment/target/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
- ../../../base/target

patches:
- path: statefulset.yaml
EOF
```

> **Note:** The deployment requires that each node should run at most one proxy and one target. If you plan to run multiple proxies and targets, ensure the cluster has at least as many nodes as the larger of the proxy or target StatefulSet replica counts. For guidance on running multi-node clusters, see [this guide](https://kind.sigs.k8s.io/docs/user/quick-start/#multi-node-clusters) for KinD and [this guide](https://minikube.sigs.k8s.io/docs/tutorials/multi_node/) for Minikube.

To deploy the custom overlay, run:

```bash
$ kubectl apply -k base/common  # Apply base/common since it is not modified in the custom overlay
clusterrolebinding.rbac.authorization.k8s.io/ais-rbac created
configmap/ais-cluster-config-override created

$ kubectl apply -k overlays/custom-deployment/proxy
configmap/ais-proxy-local-config-template created
service/ais-proxy created
statefulset.apps/ais-proxy created

$ kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
pod/ais-proxy-0 condition met

$ kubectl apply -k overlays/custom-deployment/target
configmap/ais-target-local-config-template created
service/ais-target created
statefulset.apps/ais-target created

$ kubectl rollout status statefulset/ais-target
Waiting for 3 pods to be ready...
Waiting for 2 pods to be ready...
Waiting for 1 pods to be ready...
partitioned roll out complete: 3 new pods have been updated...
```

To get the endpoint for the cluster, run:

```bash
$ make get-endpoint
To connect to the cluster, use: AIS_ENDPOINT=http://172.18.0.2:8080 
```

And to clean up and undeploy the custom deployment, run:

```bash
$ kubectl delete -k base/common
clusterrolebinding.rbac.authorization.k8s.io "ais-rbac" deleted
configmap "ais-cluster-config-override" deleted

$ kubectl delete -k overlays/custom-deployment/proxy
configmap "ais-proxy-local-config-template" deleted
service "ais-proxy" deleted
statefulset.apps "ais-proxy" deleted

$ kubectl delete -k overlays/custom-deployment/target
configmap "ais-target-local-config-template" deleted
service "ais-target" deleted
statefulset.apps "ais-target" deleted

$ ./utils/node_cleanup.sh
job.batch/node-cleanup-kind-worker created
job.batch/node-cleanup-kind-worker2 created
job.batch/node-cleanup-kind-worker3 created
```

## Enable HTTPs / Cloud Backends

Sample overlays, located in `overlays/samples`, are provided as templates for enabling additional features such as HTTPs and cloud backends.

These overlays can be used directly or further extended with additional customizations as needed.

### [HTTPs](/docs/https.md)

The `https` sample overlay provides a basic example using a self-signed certificate issued via `cert-manager`'s built-in self-signed issuer for development purposes. 

To run a minimal cluster with HTTPs enabled, run:

```bash
$ make minimal-https
```

> **Note:** `minimal-https` installs [`cert-manager`](https://github.com/cert-manager/cert-manager) in the cluster and also ensures the [`cmctl` CLI](https://cert-manager.io/docs/reference/cmctl/) is installed on the host to properly wait for `cert-manager` to become fully ready before deploying the cluster.

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
$ kubectl get secret ais-tls-certs -o jsonpath='{.data.tls\.crt}' | base64 --decode > tls.crt
```

To clean up and undeploy the minimal cluster with HTTPs enabled, run:

```bash
$ make minimal-https-cleanup
```

### [Cloud Backends](/docs/providers.md)

The cloud sample overlay configures AWS and GCP backends because the default `aistorage/aisnode` image is built with support for both AWS and GCP cloud providers included.

To run a minimal cluster with AWS and GCP cloud backends enabled, populate the `kustomization.yaml` file in `overlays/samples/cloud/target` with AWS and GCP credentials:

- For GCP, create a `creds.json` file in the `overlays/samples/cloud/target` directory containing GCP service account credentials.
- For AWS, update the literal placeholders in the `kustomization.yaml` file with AWS credentials.

Then, run the following command to deploy the minimal cluster with cloud backends enabled:

```bash
$ make minimal-cloud
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

To clean up and undeploy the minimal cluster with cloud backends enabled, run:

```bash
$ make minimal-cloud-cleanup
```

## Deploy w/ Local Changes (Development)

For convenience, all provided Makefile targets for sample deployments (e.g. `minimal`, `minimal-https`, `minimal-cloud`) support environment variable `LOCAL_DEVELOPMENT`, which if set, deploys with local changes to `aisnode` as well as `aisinit`:

```bash
$ LOCAL_DEVELOPMENT=true make minimal
```

## References

- [Makefile](/deploy/dev/k8s/kustomize/Makefile)
- [Local/Cluster Configuration](/docs/configuration.md)
- [Environment Variables](/docs/environment-vars.md)