# Choose an AIStore Kubernetes Deployment

AIStore has separate Kubernetes paths for local development and production operator deployments. Choose a path here, then follow its own instructions. For deployments that do not require Kubernetes, refer to [Getting Started](/docs/getting_started.md).

## Choose a Path

- **Develop and test locally:** Use the [local KinD or Minikube guide](/deploy/dev/k8s/README.md). This operator-less deployment uses `deploy/dev/k8s`; it is not a production deployment. Its guide covers the base cluster, access, and ETL-enabled development image.
- **Plan a production deployment:** Start with the [ais-k8s production deployment guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/README.md) and [operator guide](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md). The operator manages the AIS cluster lifecycle. Consult its [redeployment guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/redeployment.md) before changing or deleting an existing deployment; cleanup choices can affect cluster state and user data.

## Check Prerequisites

For local KinD development, the [local Kubernetes guide](/deploy/dev/k8s/README.md) lists Linux, Docker Engine, KinD, `kubectl`, and `kapp`; it also describes the Minikube option. For production, check the [ais-k8s prerequisites](https://github.com/NVIDIA/ais-k8s/blob/main/docs/prerequisites.md) for Kubernetes and `kubectl`, node readiness, Linux hosts, drives, and filesystem requirements. The [operator guide](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md) covers operator installation and webhook certificates. Do not apply local development steps to a production cluster.

Before using the production operator, decide how to provision [target storage volumes](https://github.com/NVIDIA/ais-k8s/blob/main/docs/storage_volumes.md), configure [Kubernetes monitoring](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md), and handle [TLS](https://github.com/NVIDIA/ais-k8s/blob/main/docs/tls.md). Follow the upstream deployment guides for the actual manifests and commands.

If the cluster requires authentication, refer to [AIS token validation](/docs/auth_validation.md) and the operator's [authentication guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/authentication.md) and [auth profile guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/auth_profile.md). For an example using an external issuer, refer to the current [Keycloak deployment guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/deploy_with_keycloak.md). The separate [AuthN service guide](/docs/authn.md) includes its own security-audit caveat; using the operator does not remove that caveat.

AIStore [ETL](/docs/etl.md) requires Kubernetes and an ETL-enabled `aisnode` build or image. The local Kubernetes guide describes its ETL development image; for production, use the [ETL prerequisites and operator image guidance](/docs/etl.md#quick-start) before deploying transformations.
