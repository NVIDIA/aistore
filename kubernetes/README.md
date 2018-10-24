## Disclaimer
Note DFC for Kubernetes is not currently being developed or maintained by Nvidia. These files are outdated and were once used to deploy using kubernetes, but the development team has switched over to Docker. If you would like to deploy dfc in a containerized environment, consider using Docker. The Docker folder can be found in `dfcpub/docker`. If you would still like to use kubernetes, consider playing around with these files in this directory to get it working.

## DFC in Kubernetes
This document assumes that you already have k8s cluster setup with the kubectl configured to point to your cluster.

If you don't have k8s cluster you can try setting up a minikube locally by following [Minikube installation](https://kubernetes.io/docs/getting-started-guides/minikube/)

#### Deploying DFC
1. Run  `deploy_docker.sh` script with k8s option to launch one DFC proxy, multiple DFC targets and one DFC client POD.
```
$ ./deploy_docker.sh -e k8s -a <aws.env file path>
```
The current networking limitation requires that DFC client is also launched as container in kubernetes cluster. You can SSH into the client POD to interact with the cluster.

2. To scale up/down the number target pods
```
$ kubectl scale --replicas=<total_number_of_targets> -f dfctarget_deployment.yml
```
`total_number_of_targets` - The number of targets you need in total after the rescaling process.
For example if your cluster already has 4 targets running. To add 2 more targets provide the value as 6. To scale down by two provide the value as 2.

3. To interact with the cluster
 * SSH into client
 ```
 $ kubectl exec -it <DFC_client_pod_name> -- /bin/bash
  Example : kubectl exec -it dfcclient-5797bd5474-6j88v -- /bin/bash
 ```
 * To get cluster config
```
curl -X GET -H 'Content-Type: application/json' -d '{"what": "config"}' http://10.0.2.47:8081/v1/daemon
```
 * To run test, update main_test.go to point the proxy to http://<proxy_ip>:8081
      ```
      go test -v -run=down -numfiles=<number_of_files> -bucket=<bucket_name>   
      ```
Please note that the proxy inside the cluster is running at port 8081

#### Useful Kubernetes commands
1. To list all the running pods
```
$ kubectl get pods -o wide

  NAME                         READY     STATUS    RESTARTS   AGE       IP              NODE
  dfcproxy-84999457d6-8qk6x    1/1       Running   0          39m       10.233.80.166   aws-k8s-dfc-worker0
  dfctarget-84699fd945-4whqr   1/1       Running   0          39m       10.233.80.167   aws-k8s-dfc-worker0
  dfctarget-84699fd945-jbcsx   1/1       Running   0          39m       10.233.73.102   aws-k8s-dfc-worker1
  dfctarget-84699fd945-zqs7m   1/1       Running   0          39m       10.233.80.54    aws-k8s-dfc-worker2
```
2. To view pod logs
```
$ kubectl logs <pod_name>

  Example: $ kubectl logs dfcproxy-84999457d6-8qk6x

    I0224 23:26:30.978207      21 config.go:121] Logdir: "/tmp/dfc/log" Proto: tcp Port: 8080 Verbosity: 3
    I0224 23:26:30.978459      21 config.go:123] Config: "/etc/dfc/dfc.json" Role: proxy StatsTime: 10s
    I0224 23:26:30.978627      21 stats.go:153] Starting proxystats
    I0224 23:26:30.979312      21 keeper.go:33] Starting keepalive
    I0224 23:26:30.979795      21 proxy.go:71] Proxy dfcproxy-84999457d6-8qk6x is ready
    I0224 23:26:34.980963      21 proxy.go:659] Smap (v0) and lbmap (v1) are now in sync with the targets
    I0224 23:26:35.590317      21 proxy.go:491] Registered target ID dfctarget-84699fd945-zqs7m (count 1)
    I0224 23:26:36.546552      21 proxy.go:491] Registered target ID dfctarget-84699fd945-4whqr (count 2)
    I0224 23:26:36.546856      21 proxy.go:589] synchronizeMaps is already running
    I0224 23:26:36.591174      21 proxy.go:491] Registered target ID dfctarget-84699fd945-jbcsx (count 3)
    I0224 23:26:36.591420      21 proxy.go:589] synchronizeMaps is already running
    I0224 23:26:40.978881      21 stats.go:199] proxystats: {Numget:0 Numput:0 Numpost:3 Numdelete:0 Numerr:0 Numlist:0}
    I0224 23:26:42.591117      21 proxy.go:689] http://10.233.80.167:8080/v1/daemon/synclb: &{Mutex:{state:0 sema:0} LBmap:map[] Version:1 syncversion:1}
    I0224 23:26:42.594048      21 proxy.go:689] http://10.233.73.102:8080/v1/daemon/synclb: &{Mutex:{state:0 sema:0} LBmap:map[] Version:1 syncversion:1}
    I0224 23:26:42.597307      21 proxy.go:689] http://10.233.80.54:8080/v1/daemon/synclb: &{Mutex:{state:0 sema:0} LBmap:map[] Version:1 syncversion:1}
    I0224 23:26:42.600658      21 proxy.go:671] rebalance: http://10.233.80.167:8080/v1/daemon/rebalance
    I0224 23:26:42.602740      21 proxy.go:671] rebalance: http://10.233.73.102:8080/v1/daemon/rebalance
    I0224 23:26:42.605111      21 proxy.go:671] rebalance: http://10.233.80.54:8080/v1/daemon/rebalance
    I0224 23:26:42.607661      21 proxy.go:659] Smap (v3) and lbmap (v1) are now in sync with the targets
    I0224 23:28:30.982358      21 keeper.go:87] KeepAlive: all good
```
3. To ssh into a pod
```
$ kubectl exec <pod_name> -i -t /bin/bash

    Example: $ kubectl exec dfcproxy-84999457d6-8qk6x -i -t /bin/bash
```

