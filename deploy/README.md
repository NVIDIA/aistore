## Contents

|Folder | Intended for | When and how to use | Documentation |
| --- | --- | --- | --- |
| [deploy/dev/local](/deploy/dev/local) | developers | use it for non-containerized development on your workstation, laptop, VM, etc. | run `make help` to see usage examples and supported options; visit [deploy/scripts](/deploy/scripts) for alternative scripted ways to run cluster locally  |
| **Docker** | --- | --- | --- |
| [deploy/dev/docker](/deploy/dev/docker) | developers | run AIS cluster consisting of one or more `aisnode` containers; use `AIS_ENDPOINT` to test and/or develop with it  | [readme](/deploy/dev/docker/README.md) |
| [deploy/prod/docker/single](/deploy/prod/docker/single) | first-time users and/or small-size (ad-hoc) production deployments | this is a minimal AIS cluster consisting of a single storage target and a single gateway, all in one preconfigured ready-for-usage docker image; may be perfect for small-size immediate deployment and first-time quick runs | [readme](/deploy/prod/docker/single/README.md) |
| **Kubernetes** | --- | --- | --- |
| [deploy/dev/k8s](/deploy/dev/k8s) | AIStore development with native Kubernetes provided by [minikube](https://minikube.sigs.k8s.io/docs) | Short answer: run minikube and then deploy AIS cluster on it. The steps are carefully documented [here](/deploy/dev/k8s/README.md). | [readme](/deploy/dev/k8s/README.md) |
| [deploy/prod/k8s](/deploy/prod/k8s) | production | use Dockerfiles in this folder to build AIS images for for production deployment, for which there's a separate and dedicated [repository](https://github.com/NVIDIA/ais-k8s) containing the corresponding tools, scripts, and documentation  | [AIS/K8s Operator and deployment playbooks](https://github.com/NVIDIA/ais-k8s) |

