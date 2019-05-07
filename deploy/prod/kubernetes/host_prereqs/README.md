## AIS k8s Playbooks

Ansible playbooks for AIS k8s cluster. These support a k8s cluster that will be established using kubespray, and leverage the inventory hosts.init as used with kubespray.

These playbooks are intended both for standalone use and for supporting automation.

### Playbooks for Configuring k8s Nodes

1. Playbook `ais_host_config_common.yml` - run *before* a set of hosts is added to a cluster with kubespray; this play installs assorted debug-related packages, but it does not install Docker or similar.
2. Kubespray is run, to form initial cluster or to extend it with additional hosts; amongst many other things this will install Docker.
3. Playbook `ais_node_taints.yml` - run *after* `kubespray` to apply taints to nodes; NOTE: not currently in use
4. Playbook `ais_host_post_kubespray.yml` - tweaks aspects of k8s install that aren't performed by Kubespray
5. Playbook `ais_gpuhost_config.yml` - run *after* `kubespray` for the host(s); this playbook will install CUDA drivers, `nvidia-docker2`, `nvidia-container-runtime` and the nvidia device plugin.

### Playbooks Supporting AIS Installation

The AIS application is installed using Helm, and proxy/target pods will start up on suitably labelled nodes. *Before* a target pod is started on a node, AIS filesystems (as will be enumerated in the helm install values.yaml or over-rides thereof) must be present and mounted. 
1. `ais_datafs_mkfs.yml` - The playbooks *performs mkfs (all existing data lost!) on the requested devices and mounts them on standardized mountpoints (`/ais/<device>`)*. It should only be necessary prior to a first helm install of AIS - other times you most likely want to retain the data in these filesystems for the new AIS instance to inherit.
2. `ais_datafs_mount.yml` - Mount AIS filesystems that were previously mkfs'd and added to /etc/fstab
3. `ais_datafs_umount.yml` - Unmount AIS filesystems, but leave their /etc/fstab entries intact
4. `ais_datafs_umount_purge.yml` - Unmount AIS filesystems *and* remove entries from /etc/fstab.