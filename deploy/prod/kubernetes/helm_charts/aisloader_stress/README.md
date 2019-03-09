## Stress a k8s AIS cluster using aisloader

A DaemonSet is deployed to run aisloader on multiple nodes of the k8s cluster. It is assumed that
the cluster includes both AIS proxy/target nodes and other non-AIS nodes to act as client nodes
(e.g., the client nodes might be GPU nodes). This chart should be installed *after* the ais
chart, and the helm release name of the ais release passed as param as in example below (so
that aisloader knows which proxy to contact).

Pods will be created on nodes that match the nodeSelector labelling in values.yaml, the default
being aisloader=yes. Each container so created will run an aisloader instance with parameters
as per the included ConfigMap which is parametrized from values.yaml.

Examples:
```bash    
    helm install --set ais_release=mydemo .

    helm install --name=loader \
      --set ais_release=demo \
      --set aisloaderArg.bucket.default=big_8M_bucket_%s \
      --set aisloaderArg.pctput.default=0 \
      --set aisloaderArg.duration.default=60m \
      --set aisloaderArg.minsize.default=8M \
      --set aisloaderArg.maxsize.default=8M \
      --set aisloaderArg.numworkers.default=50 .
```