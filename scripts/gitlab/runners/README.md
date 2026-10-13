# GitLab Runner Setup

Setup scripts for Docker-based GitLab CI runners for AIStore:

- Installs Docker and GitLab Runner
- Sets up local pull-through registry cache for container images
- Registers the runner with Docker executor and privileged mode
- Configures `inotify` limits
- Loads the kernel `tls` module (kTLS for the HTTPS CI jobs)

## Usage

### 1. Install Dependencies & Setup Registry

```bash
sudo ./setup.sh [--data-root <absolute_path>] [--registry-username <username>] [--registry-password <password]
```

### 2. Register Runner

```bash
./start_runner.sh --token <runner_token> [--data-root <absolute_path>] [--concurrency <number>]
```

> **Note:** This script will prompt for `sudo` access when needed for specific operations.

### 3. Tag kTLS Runners

The `test:short:https` and `test:long:https` jobs run only on runners with the `ktls` tag.

`setup.sh` runs [check-ktls.sh](../../check-ktls.sh) after it loads the `tls` module and prints whether the runner is eligible for the `ktls` tag. If eligible, add the `ktls` tag to the runner in GitLab.

## Options

> **Note:** The `--data-root` parameter is provided because ITSS provided GP VM hosted runners cannot be expanded at root (see runner setup guide on internal GitLab Wiki).

### `setup.sh`

- **`--data-root <absolute_path>`** (optional): Base directory for all data storage

### `start_runner.sh`

- **`--token <runner_token>`** (required): GitLab runner registration token
- **`--data-root <absolute_path>`** (optional): Base directory for all data storage
- **`--concurrency <number>`** (optional): Runner concurrency (default: 1)

For Kubernetes-based persistent runners (used by `ais-etl`), see the `k8s/` sub-directory. 
