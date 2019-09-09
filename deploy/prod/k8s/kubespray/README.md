## LAUNCH NEW KUBERNETES CLUSTER FOR AIS USING KUBESPRAY

Here you can find the steps to launch a new K8S cluster.

### LAUNCH THE CLUSTER

PREREQUISITES:
- Expects ansible to be installed.

1. Clone the Kubespray repo 
```
$ git clone https://github.com/kubernetes-sigs/kubespray.git
```
2. Appy the git patch of some settings changes that are required in the kubespray config values
```
$ less ais.patch
$ git patch ais.patch
```
3. Copy over inventory settings from sample into the specific inventory directory for the new project
```
$ mkdir -p inventory/ais_cluster
$ cp -rf inventory/sample/* inventory/ais_cluster/
```
4. Setup the ansible inventory file
   Copy the template of the inventory provided in this directory and fill in the values.
```
$ cp ${PATH-TO-THIS-DIR}/hosts.ini inventory/ais_cluster/hosts.ini 
```
5. Now you can try running kubespray using the following command:
```
ansible-playbook --become -i inventory/ais_cluster/hosts.ini cluster.yml -e ansible_ssh_private_key_file=<PATH-TO-PEM> --flush-cache
```

It would take about 15~20 mins and the cluster should be up.
Look at the inventory/ais_cluster/artifacts directory to find the kubeconfig to access the k8s cluster
