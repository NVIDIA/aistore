# Deploying AIS on k8s


## Introduction

This document will detail the steps required to deploy AIS on k8s. It will assume a blank slate and detail all steps. If you already have a k8s installation on which you want to deploy AIS many of these steps can be skipped, but some are still essential. Each section will begin with an indication of whether its steps are required or optional, and what assumptions are made.

As a high-performance and (of course) stateful application, AIS has more moving parts and demands than your average standalone containerized application - but they’re not difficult to realize on the k8s platform.


### Conventions in this Document


#### TL; DR

Steps that you are likely/required to perform are highlighted like this. The intention is that you could step only over those highlighted bits of text and use the other text to fill in detail as needed.


## High Level Overview


### Deployment Type

AIS is a high performance storage application for DL applications. It can be deployed as a storage backend, as a caching level between storage backends (including cloud) and clients, or as a combination. In this document we detail deployment as a storage backend.


### AIS Terminology

An AIS deployment consists of a number of _target pods_ and _gateway pods_, all cooperating within an _AIS cluster instance_. The target pods are deployed with persistent storage from the nodes on which they run - one target per node. The gateway pods together combine to offer a k8s service that external DL clients contact (using http) to read/write data - the gateway service redirects the client to the correct target endpoint to service the request.


### Prerequisites



1. [Hard] **Bare-metal Kubernetes with Calico networking**. To achieve the levels of performance required to service DL applications, AIS expects to be deployed on baremetal k8s - i.e., no hypervisor running underneath k8s. For experimentation it is possible to deploy in VMs, but for production use bare-metal is a hard requirement. K8s 1.14.1 or later is expected (tested). We only support Docker container runtime. The Calico CNI plugin has proven to meet the performance requirements of AIS.
2. [Hard] **Privileged AIS container** - the AIS container must be run with privilege within k8s.
3. [Hard] **External DNS/IP assigned **for clients to contact the AIS gateway service; external meaning “outside the k8s cluster, in DC”. We also support DGX GPU clients within the cluster - but assuming there are to be external clients they need a stable IP to contact.
4. [Hard] **Access to ports 51080 and 51081**. The AIS gateway service is an ingress on port 51080, and redirects clients to target pods via hostport 51081 on target nodes.
5. [Hard] **Kubernetes nodes with persistent storage** in local filesystems to be dedicated to AIS. We assume HDD since AIS is intended to achieve high DL performance without requiring SSD/NVME. Our reference configuration has 10 HDD in each of 12 target nodes in a single rack, but smaller or larger deployments are not simpler or more-complex! Single node deployment is possible for experimentation.
6. [Strong]** Identical storage present on each target node** - more of a limitation of existing deployment scripts than a requirement of AIS
7. [Very strong] **Ubuntu Linux, 18.04** - that is all we have tested on.
8. [Very strong] **XFS local filesystems** for persistent storage in AIS. That is what we’ve tested with, and what the provided playbooks will create.
9. [Obvious!] **High-performance networking** both in target node NICs and in switches. Our reference configuration has used 50GigE for storage nodes and 100GigE on DGX GPU clients. We would **strongly recommend** a 100GE link to each storage server as well!
10. [Recommended] **Ansible** - we provide a number of playbooks for host configuration and AIS preparation; if you don’t use them you’ll have to tease apart their steps and perform them manually!
11. [Recommended] **Grafana** - the default deployment will install Grafana, Graphite and Prometheus in the k8s cluster for performance monitoring. Some storage is required to persist the monitoring data.
12. [Recommended] **Dedicated k8s Cluster** - You _can_ deploy within a big established cluster on just a small subset of nodes, but we’ve tested on standalone clusters so recommend deploying as such. For example some playbooks still target the whole cluster by default.


### Planning

It’s helpful to note the planned configuration before embarking on a deployment. Complete a table along the lines of the following (filled with our reference configuration). AIS target and gateway pods are deployed via k8s DaemonSets on nodes selected by node labeling - maximum one each of target/gateway pods per node.


<table>
  <tr>
   <td><strong>Item</strong>
   </td>
   <td><strong>Resources</strong>
   </td>
   <td><strong>Details</strong>
   </td>
  </tr>
  <tr>
   <td><strong>Target nodes</strong>
   </td>
   <td>cpu01 - cpu12
   </td>
   <td>10 x 10TB HDD: sda, sdb, …, sdj all XFS mounted at /ais/sd[a-j] ~1.2PB total
   </td>
  </tr>
  <tr>
   <td><strong>Gateway pods</strong>
   </td>
   <td>cpu01 - cpu12
   </td>
   <td>HA service, easiest to schedule one gateway pod per target pod/node
   </td>
  </tr>
  <tr>
   <td><strong>Initial gateway</strong>
   </td>
   <td>cpu01
   </td>
   <td>Deployment quirk! Must be one of the gateway pod nodes.
   </td>
  </tr>
  <tr>
   <td><strong>Graphite</strong>
   </td>
   <td>cpu01
   </td>
   <td>/data/graphite, 250Gi
   </td>
  </tr>
  <tr>
   <td><strong>Grafana</strong>
   </td>
   <td>cpu01
   </td>
   <td>/data/grafana, 250Gi
   </td>
  </tr>
</table>



## Host Preparation


### Host OS


#### Installation [Recommended]

[Recommended/Tested] Install Ubuntu 18.04 on all planned target nodes (e.g., we use Foreman to perform this step along with a few of the following steps).


#### Ansible Inventory [Recommended]

Make sure that Ansible has passwordless sudo access to all storage node (i.e., ssh keys for login without password, no password required for sudo.

Since we use Kubespray to deploy k8s, our playbooks use Kubespray host group names by default (can over-ride on cmdline). It is convenient to add an inventory group covering all intended target nodes: we call that group `cpu-worker-node` below (please create it - it is used in some playbooks).


#### Playbook 1: Host Configuration [Required]

Once hosts are installed they _must_ be configured for AIS using the provided Ansible playbook. Before running the playbook, check the values in `vars.yaml`. This playbook runs against the whole cluster by default, limit with -e playhosts=... if needed.


```
    $ cd $AISSRC/deploy/prod/k8s/playbooks
    $ ansible-playbook -i $INVENTORY ./ais_host_config_common.yml --become
```


(assuming group target-nodes is in your inventory). This playbook:



*   Disables unattended upgrades in Ubuntu
*   Installs a long list of packages (see `vars.yaml`); many/all of these are not essential for k8s or AIS in a containerized deployment - they’re installed for debug and monitoring
*   Configures ulimits and performs system tuning via `/etc/sysctl.d`
*   Configures a networking MTU of 9000 for all interfaces listed in the `ais_host_mtu` list of `vars.yaml`; the standard entry is for NIC `enp94s0` driver `mlx5_core` - adjust for your site.
*   Selects the performance governor for CPU frequency management
*   Creates an `ais_host_config` systemctl unit to tune things that can’t be done from sysctl.d:
    *   Applies block device tuning as per `vars.yaml` (see `blkdevtune` there)
    *   Perform some Mellanox ethtool tuning for ring params and number of channels; this can be controlled from `vars.yaml `(see `ethtool` section there)


#### Playbook 2: Enable Multiqueue IO and Reboot

To change IO scheduler run this playbook (runs against cpu-worker-node by default):


```
    $ cd $AISSRC/deploy/prod/k8s/playbooks
    $ ansible-playbook -i $INVENTORY ./ais_enable_multiqueue.yml --become 
```


This playbook modifies the GRUB configuration. You must reboot hosts after running it.


#### Playbook 3: NTP

If NTP in your datacenter is required to use a local pool server then list the pool servers in vars.yaml and run this playbook.


#### Playbook 4: Make Data Filesystems

This playbook performs `mkfs` for all filesystems and mounts them - use with care! You are required to list target nodes and their disks on the cmdline (assumes same disk config for all).


    $ `cd $AISSRC/deploy/prod/k8s/playbooks`


```
    $ ansible-playbook -i $INVENTORY ./ais_datafs_mkfs.yaml \
      -e '{"ais_hosts": ["cpu01", "cpu02", "cpu03", "cpu04", "cpu05", \
         "cpu06", "cpu07", "cpu08", "cpu09", "cpu10", "cpu11", "cpu12" ], \
         "ais_devices": ["sda", "sdb", "sdc", "sdd", "sde", "sdf", \
                         "sdg", "sdh", "sdi", "sdj"]}' \
      --become
```


Yes it could do with some ease-of-use improvements, but you should not need it often!

Note that we use XFS filesystems with specific mount options, and the playbooks mount `sd*` at `/ais/sd*` on the target node hosts.


### Kubespray

Before building an initial k8s cluster, or when adding nodes, easiest is to run the above host configuration steps before building/scaling the cluster, but it is not essential.

You can build the k8s cluster with other solutions - Kubespray has simply worked nicely for our needs. 

We clone [https://github.com/kubernetes-sigs/kubespray](https://github.com/kubernetes-sigs/kubespray), copy the sample inventory into a parallel tree, and apply the following edits (you will likely have to tweak values - essential ones are highlighted):


```
    $ diff -r sample/ aiscluster/
    diff -r sample/group_vars/all/docker.yml aiscluster/group_vars/all/docker.yml
    51a52,53
    > docker_version: 18.09
    > 
    diff -r sample/group_vars/k8s-cluster/addons.yml aiscluster/group_vars/k8s-cluster/addons.yml
    7c7
    < helm_enabled: false
    ---
    > helm_enabled: true
    10c10
    < registry_enabled: false
    ---
    > registry_enabled: true
    16c16
    < metrics_server_enabled: false
    ---
    > metrics_server_enabled: true
    diff -r sample/group_vars/k8s-cluster/k8s-cluster.yml aiscluster/group_vars/k8s-cluster/k8s-cluster.yml
    23c23
    < kube_version: v1.14.3
    ---
    > kube_version: v1.14.1
    81c81,82
    < kube_service_addresses: 10.233.0.0/18
    ---
    > #kube_service_addresses: 10.233.0.0/18
    > kube_service_addresses: 192.168.0.0/18
    86c87,88
    < kube_pods_subnet: 10.233.64.0/18
    ---
    > #kube_pods_subnet: 10.233.64.0/18
    > kube_pods_subnet: 192.168.64.0/18
    127c129
    < cluster_name: cluster.local
    ---
    > cluster_name: aiscluster.local
    136a139,140
    > # for fix of https://github.com/kubernetes/dns/issues/292
    > nodelocaldns_version: "1.15.2"
    178c182
    < # kubeconfig_localhost: false
    ---
    > kubeconfig_localhost: true
    180c184
    < # kubectl_localhost: false
    ---
    > kubectl_localhost: true
    diff -r sample/group_vars/k8s-cluster/k8s-net-calico.yml aiscluster/group_vars/k8s-cluster/k8s-net-calico.yml
    9c9
    < # nat_outgoing: true
    ---
    > nat_outgoing: true
    23c23
    < # calico_mtu: 1500
    ---
    > calico_mtu: 8980
```


The only key value there is calico_mtu - this must be at least 20 bytes less than the host NIC MTU (which we set to 9000).

With the new inventory tweaked as above, build the k8s cluster:


```
    ~/kubespray$ ansible-playbook -i inventory/aiscluster/hosts.ini cluster.yml --become
```


Note: the Ansible inventory used <span style="text-decoration:underline;">must</span> be part of the kubespray tree as above (with group_vars etc as copied from sample) - if you point to a standalone inventory outside the tree then Kubespray has all sorts of subtle failures!

We assume hereafter that the k8s cluster is configured and that you’re able to run kubectl, helm etc as needed.


#### Playbook 5: Post-Kubespray Configuration

After creating a k8s cluster (through whatever means) or after adding a node to an existing cluster, if you intend to run an AIS pod on that node then you must run the following:


```
    $ cd $AISSRC/deploy/prod/k8s/playbooks
    $ ansible-playbook -i $INVENTORY ./ais_host_post_kubespray.yml --become 
```


AIS will not operate without this step. The playbook (which targets the whole cluster by default, be more selective with `-e playhosts=...`) adds the non-default option `--allowed-unsafe-sysctls='net.core.somaxconn'` to kubelet  config file `/etc/kubernetes/kubelet.env` and restarts kubelet on play hosts.


## Kubernetes Preparation

At this stage we have an operational k8s cluster with candidate AIS target nodes configured as required and with the relevant local XFS filesystem made ready for AIS to populate. A little more preparation is needed before deploying the AIS application.


### Node Labeling

The target and gateway pods are created in DaemonSets, controlled by k8s node labeling. You need to label:



*   All intended gateway nodes (ie k8s nodes to host gateway pods, one per node) with `ais-proxy-type=electable`
*   Exactly one, it doesn’t matter which, of the gateway nodes must also be labeled `initial_primary_proxy=yes`
*   All intended target nodes must be labeled `ais-target-node=yes`

There’s no need to start in the final intended configuration, for example you can start a cluster with just one proxy (which will have to be that labeled initial primary) and a single target node and label additional nodes as required. You can label nodes before Helm install or after.

There’s no playbook for labeling, so use shell such as:


```
    $ kubectl label node cpu01 initial_primary_proxy=yes
    $ for ((i=1; i<=12; i=i+1 )); do
    	nodename=$(printf "cpu%02d" $i)
    	kubectl label node $nodename ais-proxy-type=electable
    	kubectl label node $nodename ais-target-node=yes
    done
```



### Storage For AIS

We’ve already made XFS filesystems in host preparation above. The k8s deployments of the target pods access those filesystems via k8s hostPath volumes - we’ll simply pass the list of disks for each node on the helm install cmdline (via the wrapper created below).

The playbooks used above mounted `sda` at `/ais/sda`, and so on. Our reference configuration, in which each target node has 10 HDD `sda-sdj` will pass the corresponding 10 mountpoints.


### Storage For Grafana/Graphite

The default AIS helm chart expects to deploy Grafana/Graphite/Prometheus within the cluster - the installation can be suppressed if another solution is already in place. This deployment is automated, but in advance you need to identify some persistent storage for Grafana and Graphite. This can be done with any volume type, but the AIS chart is expecting the simplest hostPath approach in which you just need to identify the node and path for the storage (having created the directory). Avoid using any of the disks on which AIS data is also stored.

For example, assume there’s some disk space available on node cpu01 - in this example we had 2 x 1TB nvme drives which were joined into a single 2TB lvm volume mounted at /data; to prepare for AIS just

cpu01$ mkdir /data/grafana /data/graphite

Note the node name and paths for use in the start wrapper below.


### External Ingress


#### AIS Gateway External IP

First, assign a datacenter IP that clients will contact for AIS services. Only one is required, for the AIS gateway service (redirections to target pods will be via hostPorts of their respective nodes). We assume the IP is already suitably routed.


#### LoadBalancer

We use metalLB as a loadbalancer for external client ingress to the k8s cluster. If other load balancer solutions are available (e.g. provided by cloud vendor) they could be used instead.

The simplest metalLB setup is to use a layer 2 solution in which one k8s node responds to the MAC address associated with the external IP, and if that host goes awol another is promoted to take over. The alternative is to use BGP, which has some complications when also using Calico networking. So the following assumes the layer 2 solution.

Begin by installing metalLB, as per [https://metallb.universe.tf/installation/](https://metallb.universe.tf/installation/) :


```
    $ helm install -n metallb --namespace metallb-system stable/metallb
```


Next,  configure metalLB by editing (in namespace metallb-system) configmap/metallb-config. We have provided a sample layer 2 configuration which can be used with `kubectl apply -f`:


```
    $ cat deploy/prod/k8s/helm/ais/mlbcm.yaml
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: metallb-config
      namespace: metallb-system
    data:
      config: |
        address-pools:
        - name: default
          protocol: layer2
          addresses:
          - 10.132.179.10-10.132.179.20
```


Note: that example and the service created in ais install assume use of namespace metallb-system in installing metalLB.

The address pool range specified will be controlled/allocated by metalLB - it should contain the external IP chosen for the AIS gateway service.

When AIS is installed with Helm, a service will be created requesting the external IP chosen here.


#### Required Open Ports

You may need to open firewall rules to AIS traffic. AIS uses HTTP, but not on the usual http ports. It uses (by default) port 51080 for the gateway service, and 51081 for target pods. It is possible to configure different host port numbers to pod port numbers - but it’s less confusing to keep them the same.

Port 51080 (or other value if you over-ride below) must be open on all k8s nodes that run proxy pods (are labeled `ais-proxy-type=electable`). The externalIP will only resolve to one of the nodes at a time, but it can move around over time.

Port 51081 must be open on all k8s nodes that run target pods (are labeled `ais-target-node=yes`). Client requests to the gateway service are redirected to hostPort 51081 on the target node (and from there into the target pod it hosts).


### AIS Containers

Containers are currently hosted on quay.io in private repos.


<table>
  <tr>
   <td><strong>Repo</strong>
   </td>
   <td><strong>Version</strong>
   </td>
   <td><strong>Description</strong>
   </td>
  </tr>
  <tr>
   <td><code>quay.io/nvidia/aisnode</code>
   </td>
   <td><code>2.5</code>
   </td>
   <td>AIS container for target and proxy nodes
   </td>
  </tr>
  <tr>
   <td><code>quay.io/nvidia/ais-kubectl</code>
   </td>
   <td><code>stable</code>
   </td>
   <td>Used briefly in an initContainer to label pods, hardly ever changes
   </td>
  </tr>
</table>



### AIS Administrative CLI

Today we install the cli tool manually - pending is a container to deliver these more easily.

The CLI is named `ais`; it must be run on a Linux system.


## Deploy AIS using Helm


### Helm Install Wrapper

Deployment is automated (with all the above in place) via Helm install. The `values.yaml` in the chart has a number of generic defaults or values that can’t be known ahead of time (like disk names etc) and so it is usually a requirement to supply those values on the helm install cmdline. It’s generally easier to capture those site-unique settings in a wrapper script to helm install - saves repeating the cmdline runes every time you want to deploy. A future version of this wrapper should consume an rc file or similar, but for now you copy the sample and customize:


```
    $ cd deploy/prod/k8s/helm/ais
    $ cp run_ais_sample.sh start_ais
    $ vi start_ais
```


The values to confirm/change are highlighted in the script and discussed in comments. All of them we have dealt with and noted earlier in this document:



*   The cluster name; the default is demo, and it’s easier to leave it at that! Services, daemonsets, pods etc will all include this name
    *   Note: you must deploy in the default namespace at this time (work in progress)
    *   Note: you can only deploy one AIS cluster within a k8s cluster (work in progress)
*   Container image locations and versions; associated pull secrets, if needed
*   Target node mount paths for AIS disks
*   Where monitoring data will be stored
*   Optional cpu requests/limits
*   Kubernetes cluster CIDR, as used in Kubespray (`kube_pods_subnet` in `group_vars/k8s-cluster/k8s-cluster.yml` in the Kubespray inventory); required if there are to be AIS storage clients outside the k8s cluster
*   AIS gateway external IP (as per metalLB section above); if you leave this empty then metalLB will allocate and you can discover the IP address in use using `kubectl`.
*   Hostport on target nodes that will be forwarded to target pods; the default is 51081 and there’s no reason to change it


### Deploy AIS


#### Update Dependencies

The `start_ais` script will pull these dependencies in if they are absent, but thereafter will not update dependencies - you choose when you want to update.

To update the Grafana and Graphite dependencies, run


```
    $ cd deploy/prod/k8s/helm/ais/charts
    $ helm dependency update
```


You only need to do that once initially, then again later if you want to update those applications as part of an install.


#### Helm Install

Now run the wrapper script we created above to start the AIS cluster:

$ cd deploy/prod/k8s/helm/ais

$ ./start_ais


### Confirm Success

The first ever startup will take a little longer as all nodes retrieve the container image. Thereafter startup takes around 30s for all pods to be created and to form an AIS cluster. At that time, a


```
$ kubectl get pods --selector=app=ais
```


will show target and proxy pods as Ready. The AIS CLI will show cluster status:

	`$ env AIS_URL=http://<ext-ip>:51080 ais status`

If you configured external access, then the following should show an endpoint (and not <none>):

	`$ kubectl get ep demo-ais-gw`


## http Endpoint For Clients to Access

External clients use `http://<external-IP-or-dns-name>:51080` for AIS access.



## Grafana Access

This moves around on a nodePort service under the current AIS install - so the port changes if you helm delete and reinstall (not a common operation). To retrieve the current port in use:


```
    $ kubectl get service/demo-grafana
    NAME           TYPE       CLUSTER-IP      EXTERNAL-IP   PORT(S)        AGE
    demo-grafana   NodePort   10.233.13.178   <none>        80:30564/TCP   8m37s
```


In this example, port 30564 (on any node) can be contacted for Grafana.

If this is the first time Grafana has been installed, the generated password can be retrieved using:


```
    $ kubectl get secret --namespace default demo-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo'
```


Alternatively, changed the password using:


```
$ kubectl exec -it demo-grafana -- grafana-cli admin reset-admin-password --homepath /usr/share/grafana "newpassword"


<!-- Docs to Markdown version 1.0β18 -->
