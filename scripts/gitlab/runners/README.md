# GitLab Runner Setup

Setup scripts for Docker-based GitLab CI runners for AIStore:

- Installs Docker and GitLab Runner
- Sets up local pull-through registry cache for container images
- Registers the runner with Docker executor and privileged mode
- Configures `inotify` limits

## Usage

### 1. Install Dependencies & Setup Registry

```bash
sudo ./setup.sh [--data-root <absolute_path>]
```

### 2. Register Runner

```bash
./start_runner.sh --token <runner_token> [--data-root <absolute_path>] [--concurrency <number>]
```

> **Note:** This script will prompt for `sudo` access when needed for specific operations.

## Options

> **Note:** The `--data-root` parameter is provided because ITSS provided GP VM hosted runners cannot be expanded at root (see runner setup guide on internal GitLab Wiki).

### `setup.sh`

- **`--data-root <absolute_path>`** (optional): Base directory for all data storage

### `start_runner.sh`

- **`--token <runner_token>`** (required): GitLab runner registration token
- **`--data-root <absolute_path>`** (optional): Base directory for all data storage
- **`--concurrency <number>`** (optional): Runner concurrency (default: 1)

For Kubernetes-based persistent runners (used by `ais-etl`), see the `k8s/` sub-directory. 
