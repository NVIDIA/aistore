# AIStore development deployments

The setups in this repository are for development, testing, and evaluation. For production, follow the [ais-k8s deployment guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/README.md).

| Setup | Use case |
| --- | --- |
| [Local playground](/deploy/dev/local/README.md) | Run AIStore from source on a workstation or VM. See `make help` and [deployment scripts](/scripts). |
| [Ansible](/deploy/dev/ansible/README.md) | Develop and test on multiple hosts. |
| [Slurm](/deploy/dev/slurm/README.md) | Run multi-node tests on a Slurm cluster. |
| [Docker](/deploy/dev/docker/README.md) | Develop and test with AIStore containers. |
| [Minimal Compose](/deploy/prod/docker/compose/README.md) | Smoke-test a single proxy and target. |
| [Local Kubernetes](/deploy/dev/k8s/README.md) | Develop and test on KinD or Minikube without the operator. |

The [Kubernetes deployment guide](/docs/kubernetes.md) links to the local setup and the production prerequisites.

`deploy/prod/k8s` contains the container image build recipes used by CI, release publishing, and local Kubernetes development. Production cluster installation and operation are documented in [ais-k8s](https://github.com/NVIDIA/ais-k8s).
