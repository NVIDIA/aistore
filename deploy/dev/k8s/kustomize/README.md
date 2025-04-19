# Local/Development K8s Deployment

The local, development deployment of AIStore on Kubernetes is a lightweight version that closely mirrors a [production deployment](https://github.com/NVIDIA/ais-k8s), providing a similar architecture and operational model while requiring fewer resources and simplifying setup for testing and prototyping.

Running a local, development deployment of AIStore on Kubernetes requires a local Kubernetes cluster solution such as [Minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download) or [KinD](https://kind.sigs.k8s.io/docs/user/quick-start/); for the purposes of this guide, KinD will be used.

## Prerequisites

This setup requires Linux. Before proceeding, ensure the following dependencies are installed:

- [Docker Engine](https://docs.docker.com/engine/): Required for running KinD clusters.

- [KinD](https://kind.sigs.k8s.io/#installation-and-usage): Used to create and manage the local Kubernetes cluster.

- [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl): Kubernetes command-line tool for interacting with the cluster.

## Getting Started

### Create KinD Cluster

To run a simple, single-node KinD cluster, run:

```bash
$ kind create cluster --name ais-k8s-local
Creating cluster "ais-k8s-local" ...
 ✓ Ensuring node image (kindest/node:v1.32.2) 🖼
 ✓ Preparing nodes 📦  
 ✓ Writing configuration 📜 
 ✓ Starting control-plane 🕹️ 
 ✓ Installing CNI 🔌 
 ✓ Installing StorageClass 💾 
Set kubectl context to "kind-ais-k8s-local"
You can now use your cluster with:

kubectl cluster-info --context kind-ais-k8s-local

Thanks for using kind! 😊
```

Once the cluster is created, check the status of the cluster with:

```bash
$ kubectl cluster-info --context kind-ais-k8s-local
Kubernetes control plane is running at https://127.0.0.1:34805
CoreDNS is running at https://127.0.0.1:34805/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.

$ kubectl get nodes
NAME                          STATUS   ROLES           AGE    VERSION
ais-k8s-local-control-plane   Ready    control-plane   2m5s   v1.32.2

$ kubectl get pods
No resources found in default namespace.
```

### Deploy Minimal AIStore Cluster

Next, deploy a minimal AIStore cluster (1 proxy and 1 target) on the KinD cluster:

```bash
$ make minimal

kubectl apply -k base/common
clusterrolebinding.rbac.authorization.k8s.io/ais-rbac created
configmap/ais-cluster-config-override created
kubectl apply -k base/proxy
configmap/ais-proxy-local-config-template created
service/ais-proxy created
statefulset.apps/ais-proxy created
kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0
pod/ais-proxy-0 condition met
kubectl apply -k base/target
configmap/ais-target-local-config-template created
service/ais-target created
statefulset.apps/ais-target created
kubectl rollout status statefulset/ais-target
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

Verify that the cluster is up and running:

```bash
$ kubectl get pods

NAME           READY   STATUS    RESTARTS   AGE
ais-proxy-0    1/1     Running   0          90s
ais-target-0   1/1     Running   0          71s
```

### Interacting w/ AIStore

To interact with the AIStore cluster, export the `AIS_ENDPOINT` environment variable to the address of the AIStore primary proxy (e.g. `ais-proxy-0`), which is the IP of the host node:

```bash
$ kubectl get pods -o wide
NAME           READY   STATUS    RESTARTS   AGE     IP           NODE                          NOMINATED NODE   READINESS GATES
ais-proxy-0    1/1     Running   0          2m25s   10.244.0.5   ais-k8s-local-control-plane   <none>           <none>
ais-target-0   1/1     Running   0          2m6s    10.244.0.6   ais-k8s-local-control-plane   <none>           <none>
```

Since the primary proxy is running on the control plane node `ais-k8s-local-control-plane`, the IP of the host node can then be found with:

```bash
$ kubectl get nodes -o wide
NAME                          STATUS   ROLES           AGE     VERSION   INTERNAL-IP   EXTERNAL-IP   OS-IMAGE                         KERNEL-VERSION      CONTAINER-RUNTIME
ais-k8s-local-control-plane   Ready    control-plane   6m36s   v1.32.2   172.25.0.5    <none>        Debian GNU/Linux 12 (bookworm)   6.11.0-21-generic   containerd://2.0.2
```

The IP of the host node is `172.25.0.5`, so the `AIS_ENDPOINT` environment variable should be set to `http://172.25.0.5:8080` (`8080` is the public port of the proxy):

```bash
$ export AIS_ENDPOINT=http://172.25.0.5:8080
```

Use the AIStore [CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md) to interact with the AIStore cluster:

```bash
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

Alternatively, use the [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk#ais-python-sdk) to interact with the AIStore cluster:

```python
from aistore.sdk.client import Client

client = Client("http://172.25.0.5:8080")
```

To clean up and undeploy the minimal AIStore cluster, run:

```bash
$ make minimal-cleanup
kubectl delete -k base/common || true
clusterrolebinding.rbac.authorization.k8s.io "ais-rbac" deleted
configmap "ais-cluster-config-override" deleted
kubectl delete -k base/proxy || true
configmap "ais-proxy-local-config-template" deleted
service "ais-proxy" deleted
statefulset.apps "ais-proxy" deleted
kubectl delete -k base/target || true
configmap "ais-target-local-config-template" deleted
service "ais-target" deleted
statefulset.apps "ais-target" deleted
job.batch/deployment-cleanup created
```

## Further Customizations

To further customize the deployment, simply define an overlay.

For example, to deploy a cluster with 2 proxies and 3 targets, create an overlay that modifies the number of replicas for the respective statefulsets:

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

> **Note:** The deployment schedules at most one proxy and one target per node. To run multiple proxies and targets on the same node, ensure the cluster has enough nodes as the maximum of replicas for the proxy and target statefulsets.

To deploy the custom overlay, run:

```bash
$ kubectl apply -k base/common  # Apply base/common since it is not modified in the overlay
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

## Deploy w/ Local Changes (Development)

For changes to `aisnode`, build the `aisnode` container image:

```bash
$ cd ../../../prod/k8s/aisnode_container
$ IMAGE_REPO=my-repo/ais-init IMAGE_TAG=test-tag make build
```

For changes to `ais-init`, build the `ais-init` container image:

```bash
$ cd ../../../prod/k8s/aisnode_container
$ IMAGE_REPO=my-repo/aisnode IMAGE_TAG=test-tag make build
```

Load the built images into the KinD cluster:

```bash
$ kind load docker-image my-repo/aisnode:test-tag
Image: "my-repo/aisnode:test-tag" with ID "sha256:13cb3d735f93f5dfcd43d5af30ebee4b91491e159991d2139ef97f9356e6bee1" not yet present on node "kind-control-plane", loading...

$ kind load docker-image my-repo/ais-init:test-tag
Image: "my-repo/ais-init:test-tag" with ID "sha256:3d1b0b78e9088fcdf608dc31b7eb1f6117633756d3db47c9fb398f0b96cd5792" not yet present on node "kind-control-plane", loading...
```

Then, define an overlay to use the images in the deployment:

```bash
$ mkdir -p overlays/custom-deployment/proxy

$ cat <<EOF > overlays/custom-deployment/proxy/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
- ../../../base/proxy

images:
  - name: aistorage/aisnode
    newName: my-repo/aisnode
    newTag: test-tag
  - name: aistorage/ais-init
    newName: my-repo/ais-init
    newTag: test-tag
EOF

$ mkdir -p overlays/custom-deployment/target

$ cat <<EOF > overlays/custom-deployment/target/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
- ../../../base/target

images:
  - name: aistorage/aisnode
    newName: my-repo/aisnode
    newTag: test-tag
  - name: aistorage/ais-init
    newName: my-repo/ais-init
    newTag: test-tag
EOF
```

Lastly, deploy the cluster:

```bash
$ kubectl apply -k base/common  # Apply base/common since it is not modified in the overlay
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
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

Again, to clean up and undeploy the custom deployment, run:

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
job.batch/node-cleanup-kind-control-plane created
```

## Enable HTTPs / Cloud Backends

Sample overlays, located in `overlays/samples`, are provided as templates for enabling additional features such as HTTPs and cloud backends.

These overlays can be used directly or further extended with additional customizations as needed.

### HTTPs

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

### Cloud Backends

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
