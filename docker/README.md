## Getting Started: Docker

DFC can be run as a cluster of Docker containers. There are two supported modes of operation: development (-dev) and production (-prod).

in the development mode, all docker containers mount the same host's DFC source directory, and then execute from this single source. Upon restart (of the DFC cluster), all changes made in the host will, therefore, take an immediate effect.

For introduction to Docker, please watch [Docker 101 youtube](https://www.youtube.com/watch?v=V9IJj4MzZBc)

This README documents the steps to install and run DFC

#### Install Docker
1. Download and install the docker:
```
$ sudo wget -qO- https://get.docker.com/ | sh
```
2. Add your current user to the docker group (but only if you are not the root)
```
$ sudo usermod -aG docker $(whoami)
```
3. Enable and start docker service
```
$ sudo systemctl enable docker.service
$ sudo systemctl start docker.service
```
4. Verify that docker service is running:
```
sudo systemctl status docker.service
```

#### Install Docker compose
* Install python-pip and use pip to install docker-compose
##### Debian
```
$ sudo apt-get install -y python-pip
$ sudo pip install docker-compose
```

#### Starting DFC
1. If you have already installed go and configured $GOPATH execute the below command to download DFC source code and all its dependencies.
```
$ go get -u -v github.com/NVIDIA/dfcpub/dfc
```
2. Create a file called "aws.env" with AWS credentials in the format specified below (make sure that the format is exactly as defined):
```
$ vi aws.env
   AWS_ACCESS_KEY_ID=<Access_key>
   AWS_SECRET_ACCESS_KEY=<Secret_key>
   AWS_DEFAULT_REGION=<Default region>
```
To run DFC docker containers, you will need to pass this aws.env file via -a <aws.env pathname> CLI.
Example:
```
./deploy_docker.sh -a ~/.dfc/aws.env
```

3. As stated above, DFC can be launched in two modes (dev | prod), and supports Ubuntu container images.
```
$ ./deploy_docker.sh -e dev -a <aws.env file path>
```

Please note that if you are running the service for the first time, the image build process will take some time; subsequent runs will use the cached images and be much faster.

5. Scale up/down number of targets
```
 $ ./deploy_docker.sh -s <total_number_of_targets>
```
`total_number_of_targets` - The number of targets you need in total after the rescaling process.
For example if your cluster already has 4 targets running. To add 2 more targets provide the value as 6. To scale down by two provide the value as 2.

#### Helpful docker commands
1. List all the running containers
```
$ sudo docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                    NAMES
375ce054e232        docker_dfctarget    "/bin/sh -c 'export …"   5 seconds ago       Up 4 seconds                                 docker_dfctarget_3
81beaeb36f65        docker_dfctarget    "/bin/sh -c 'export …"   5 seconds ago       Up 4 seconds                                 docker_dfctarget_4
4ce206632f97        docker_dfctarget    "/bin/sh -c 'export …"   5 seconds ago       Up 4 seconds                                 docker_dfctarget_2
05a94765123f        docker_dfcproxy     "/bin/sh -c 'export …"   5 seconds ago       Up 4 seconds        0.0.0.0:8080->8080/tcp   docker_dfcproxy_1
2616242ad1e4        docker_dfctarget    "/bin/sh -c 'export …"   5 seconds ago       Up 4 seconds                                 docker_dfctarget_1
```
2. To view docker logs
```
$ sudo docker logs <container_name>

  Example:$ sudo docker logs dfc_proxy_1

    I0206 22:58:15.530964      21 config.go:121] Logdir: "/var/log" Proto: tcp Port: 8080 Verbosity: 3
    I0206 22:58:15.531240      21 config.go:123] Config: "/etc/dfc/dfc.json" Role: proxy StatsTime: 10s
    I0206 22:58:15.531759      21 proxy.go:78] Proxy 23875:8080 is ready
    I0206 22:58:15.531915      21 stats.go:135] Starting proxystats
    I0206 22:58:15.531930      21 keeper.go:33] Starting keepalive
    I0206 22:58:15.661649      21 proxy.go:491] Registered target ID 40655:8080 (count 1)
    I0206 22:58:15.661937      21 proxy.go:590] synchronizeMaps is already running
    I0206 22:58:17.447533      21 proxy.go:491] Registered target ID 45620:8080 (count 2)
    I0206 22:58:17.447871      21 proxy.go:590] synchronizeMaps is already running
    I0206 22:58:17.975370      21 proxy.go:491] Registered target ID 59333:8080 (count 3)
    I0206 22:58:17.975632      21 proxy.go:590] synchronizeMaps is already running
    I0206 22:58:18.518839      21 proxy.go:491] Registered target ID 43781:8080 (count 4)
    I0206 22:58:18.519116      21 proxy.go:590] synchronizeMaps is already running
```
You can obtain the container name by running command `sudo docker ps`
3. To ssh into a container
```
$ sudo docker exec -it <container_name> /bin/bash
Example: $ sudo docker exec -it dfc_target_1 /bin/bash
```
In production mode, the logs are expected to be in `/var/log/dfc/`.By deafult (Devlopment mode) the logs are under `tmp/dfc/log`

5. List docker images
```
$ sudo docker images

  REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE
  docker_dfcproxy     latest              31697fe843db        20 hours ago        1.21GB
  docker_dfctarget    latest              31697fe843db        20 hours ago        1.21GB
```


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

