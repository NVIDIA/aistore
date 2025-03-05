## Gitlab Runner Setup Scripts

- Run `sudo ./setup.sh`
- Run `./start_runner.sh --nodes=<desired>`
  - To register a new runner, provide a gitlab runner token as an argument: `./start_runner.sh --nodes=<desired> --token=<token-from-gitlab>`
  - To simply restart minikube and tunnel, run the script without a token argument.

The minikube profile and settings will be placed in `/var/local/minikube` 

Minikube tunnel logs will be written to `minikube_tunnel.log`

`gitlab-runner` will run as a system service with a new user `gitlab-runner` 