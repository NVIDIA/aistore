# GitLab Runner Setup - Kubernetes (Persistent)

Setup scripts for persistent AIStore deployments in GitLab CI runners, specifically for the **ais-etl** repository (`ais-etl-ci-k8s` tag):

- Installs Docker, Minikube, kubectl, and GitLab Runner
- Sets up Minikube K8s cluster
- Registers Kubernetes executor runner
- Configures CoreDNS for faster DNS updates
- Installs Local Path Provisioner
- Configures `inotify` limits

## Usage

### 1. Install Dependencies

```bash
sudo ./setup.sh [--data-root <absolute_path>]
```

> **Important:** After installation, you must log out and log back in (or run `newgrp docker`) to refresh your group membership and allow rootless Docker access and for the next step to work.

### 2. Start Minikube Cluster and Register Runner

```bash
./start_runner.sh [--token <runner_token>] [--data-root <absolute_path>] [--nodes <number>] [--concurrency <number>] [--tunnel <true|false>]
```

> **Note:** This script will prompt for `sudo` access when needed for specific operations.

### 3. Deploy AIS Cluster

```bash
cd {AISTORE_ROOT}/deploy/dev/k8s
DOCKERHUB_USERNAME=<dockerhub_username> DOCKERHUB_TOKEN=<dockerhub_token> make github-ci
```

> **Note:** The `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are required to pull images authenticated from DockerHub (retrieve them from GitLab CI/CD variables).

> **Note:** To re-deploy the cluster, run `make cleanup` and repeat step three, or refer to [`../ansible/README.md`](../ansible/README.md) for remote re-deployment.

## Options

> **Note:** The `--data-root` parameter is provided because ITSS provided GP VM hosted runners cannot be expanded at root (see runner setup guide on internal GitLab Wiki).

### `setup.sh`

- **`--data-root <absolute_path>`** (optional): Base directory for all data storage

### `start_runner.sh`

- **`--token <runner_token>`** (optional): GitLab runner registration token
- **`--data-root <absolute_path>`** (optional): Base directory for all data storage
- **`--nodes <number>`** (optional): Number of Minikube nodes (default: 1)
- **`--concurrency <number>`** (optional): Runner concurrency (default: 1)
- **`--tunnel <true|false>`** (optional): Start minikube tunnel (default: false)
