This directory contains playbooks and scripts for deploying a non-containerized AIS cluster across multiple nodes. 

This setup is intended for **testing and development only**; production deployments should use the k8s deployment.

---

### Prerequisites

- Network connectivity from ansible to hosts and between hosts
- GO installed and available in usr/local/go on all hosts
- Root access on each host (for ansible `become`)

---

### Configuration

Modify the host configuration in `inventory.yaml`. Each host is expected to act as a target node, with an equal number of devices and mountpaths specified. `directory` specifies the root directory for the location of the mountpaths. The `primary` hosts section defines the primary host, which will serve as the only proxy for this deployment (and also one target). Each `target` host will use the proxy on the `primary` host to self-register as a target node.

All other variables are defined in the `vars.yaml` file. This includes locations of files on the hosts as well as the primary host IP and port. 

--- 

### Deployment

Run `deploy_multi.sh` to run the full deployment. This will first run the `setup.yaml` playbook to set up the AIS mounthpaths on all targets. Next, the primary proxy and a target will be deployed on the primary host with the `deploy_primary.yaml` playbook. Finally, any additional targets hosts will be deployed with the `deploy_target.yaml` playbook.