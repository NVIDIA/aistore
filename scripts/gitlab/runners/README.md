## Gitlab Runner Setup Scripts

- Run `./start_runner.sh`
    - Optionally, provide a gitlab runner token as the first argument to register a new runner
- To restart minikube and tunnel, run the script without a token argument.

The minikube profile and settings will be placed in `/var/local/minikube` 

Minikube tunnel logs will be written to `minikube_tunnel.log`

`gitlab-runner` will run as a system service with a new user `gitlab-runner` 