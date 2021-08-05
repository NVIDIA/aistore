---
layout: post
title: SYSFILES
permalink: sysfiles
redirect_from:
 - sysfiles.md/
 - /docs/sysfiles.md/
---

## System Files

In addition to user data, AIStore stores, maintains, and utilizes itself a relatively small number of system files that serve a variety of different purposes.
This section tries to enumerate those *system files* and briefly describe their respective usage.

First, there's the *node configuration* file usually derived from a single configuration template and populated either via simple scripts or - for production - [Helm Charts](https://helm.sh) and [Argo CD](https://argoproj.github.io/argo-cd).
*Templated defaults* that must be modified or filled-in on a per-clustered node bases (such as IP addresses and disk paths) are all listed in the corresponding templates - examples including:

* Local Playground: a single [configuration template](/deploy/dev/local/aisnode_config.sh) and [the script](/deploy/dev/local/deploy.sh) we use to populate it when we run the cluster locally on our development machines;
* Production K8s deployment: [a set of configuration templates](https://github.com/NVIDIA/ais-k8s/tree/master/helm/ais/charts/templates) and the `values.yaml` file that can be located in the parent directory and that comprises all the values that must be set, modified, or tuned-up for a specific Kubernetes deployment.

The second category of *system files and directories* includes:

| Name | Type | Node | Brief Description | Description |
| ---- | ---- | ---- | ----------------- | ----------- |
| `.ais.bmd` | file | gateway | Buckets Metadata | Names and properties of all buckets, including replicated [remote buckets](providers.md#cloud-object-storage) and [remote AIStore](providers.md#remote-ais-cluster) buckets |
| `.ais.smap` | file | gateway and target | Cluster Map | Description of whole cluster which includes IDs and IPs of all the nodes. |
| `.ais.rmd` | file | storage target | Rebalancing State | Used internally to make sure that cluster-wide rebalancing runs to completion in presence of all possible events including cluster membership changes and cluster restarts. |
| `.ais.markers/` | dir | storage target | Persistent state markers | Used for many purposes like determining node restart or rebalance/resilver abort. The role of the markers is to survive potential node's process crash (eg. due to power outage or mistake). |
| `.ais.proxy_id` | file | gateway | Gateway node id | Used during node startup to detect a node ID if not [specified with `-daemon_id`](/docs/command_line.md). Note: storage targets also try to detect a node ID, but by looking for the extended attribute `user.ais.daemon_id` on its filesystem. |

Thirdly, there are also AIS components and tools, such as [AIS authentication server](https://github.com/NVIDIA/aistore/tree/master/cmd/authn) and [AIS CLI](https://github.com/NVIDIA/aistore/tree/master/cmd/cli). Authentication server, if enabled, creates a sub-directory `.authn` that contains:

| Name | Description |
| --- | --- |
| `authn.json` | AuthN server configuration |
| `authn.db` | Registered clusters, a token blacklist, user roles, user credentials and permissions |

And on the machine where you run AIS CLI expect to see the following two files (by default, under  `~/.config/ais/`):

| Name | Description |
| --- | --- |
| `config.json` | Configuration file (if doesn't exist, the config gets created and populated with default values upon the first CLI run) |
| `auth.token` | The *token file* is created iff `AuthN` (see above) is running and CLI user logged-in (via `ais auth login` command). The `auth.token` is then used to make the requests to the cluster and manage users and their permissions. When a logged-in user signs out, the *token file* gets removed. |

Finally, there's also `ais.db` that each AIS node may store locally to maintain component-specific runtime information in the form of key-value records. The components in-question include [dSort](https://github.com/NVIDIA/aistore/tree/master/dsort) and [Downloader](https://github.com/NVIDIA/aistore/tree/master/downloader) and the example of the stored information would be running downloading jobs and their errors (if any).
