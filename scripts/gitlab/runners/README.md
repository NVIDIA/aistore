## GitLab Runner Setup

For setting up Kubernetes runners with persistent AIStore on Minikube:

- Run `sudo ./setup.sh [--data-root <path>]` to install required dependencies and set up the environment.
- Run `./start_runner.sh [--nodes <number>] [--token <runner_token>] [--data-root <path>] [--concurrency <runner-concurrency>] [--tunnel <true|false>]` to start the Minikube cluster and register the GitLab runner (if a token is provided).
- Go to `{AISTORE_ROOT}/deploy/dev/k8s/kustomize` and deploy AIStore (e.g. `make minimal`).

#### Notes

- To relocate Docker & Runner data, pass `--data-root=<absolute_path>` to **both** scripts.  
- Minikube profile & state live under `/var/local/minikube`.  
- Minikube tunnel logs, if one was started, will be written to `minikube_tunnel.log`.
- `gitlab-runner` will run as a system service with a new user `gitlab-runner`.

### `setup.sh` options

- **`--data-root <absolute_path>`:**  Base directory under which Docker stores its data-root (`<absolute_path>/docker`) and GitLab Runner stores its builds and cache (`<absolute_path>/gitlab-runner`).

### `start_runner.sh` options

- **`--nodes <number>`:** Number of Minikube nodes (default: `1`).
- **`--token <runner_token>`:** GitLab runner registration token (if omitted, no new runner will be registered).
- **`--data-root <absolute_path>`:** Base directory under which GitLab Runner stores its builds and cache (`<absolute_path>/gitlab-runner`).
- **`--concurrency <number>`:**  Sets runner concurrency in `/etc/gitlab-runner/config.toml` before registering (default: `1`).
- **`--tunnel <true|false>`:** Start `minikube tunnel` in the background (default: `false`).
