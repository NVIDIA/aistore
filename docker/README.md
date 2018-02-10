## Getting Started - Docker

DFC can be run as a cluster of Docker containers. You can run DFC in two modes:DEV and PROD. DEV mode is ideal if you are a developer and want a quick way to setup, debug and execute DFC from source. PROD mode is deal for production deployment environment.

For introduction to docker, [watch]( https://www.youtube.com/watch?v=V9IJj4MzZBc)

#### Install Docker
1. Download install the docker installation script
```
$ sudo wget -qO- https://get.docker.com/ | sh
```
2. Add your current user to docker group if you are not the root user
```
$ sudo usermod -aG docker $(whoami)
```
3. Enable and start docker service
```
$ sudo systemctl enable docker.service
$ sudo systemctl start docker.service
```
4. Verify if docker service is running using the status command
```
sudo systemctl status docker.service
```

#### Install Docker compose
* Install python-pip and use pip to install docker-compose
##### RPM
```
$ sudo yum install epel-release
$ sudo yum install -y python-pip
$ sudo pip install docker-compose
```
##### Debian
```
$ sudo apt-get install -y python-pip
$ sudo pip install docker-compose
```

#### Starting dfc
1. If you have already installed go and configured $GOPATH execute the below command to download DFC source code and all its dependencies. 
```
$ go get -u -v github.com/NVIDIA/dfcpub/dfc
```
2. Create a aws.env file in with aws credentials in the below format. Make sure that the format is exactly as defined below.
```
$ vi aws.env
   AWS_ACCESS_KEY_ID=<Access_key>
   AWS_SECRET_ACCESS_KEY=<Secret_key>
   AWS_DEFAULT_REGION=<Default region>
```
You will need to pass the path of aws.env with option -a <aws.env path> to deploy_docker.sh script.
Example:
```
./deploy_docker.sh -a /tmp/aws.env
```
3. cd into the docker directory
```
$ cd $GOPATH/src/github.com/NVIDIA/dfcpub/docker
```
4. DFC can be launched in two modes and supports ubuntu and centos container images
  * **dev** - This mounts the synced github source code into the containers. Dev mode is ideal for local development and debug use case. All changes made the made in /dfc folder source code will be reflected on the container. But, you will need to shut down the cluster and restart it for the changes to reflect in the DFC service.
```
$ ./deploy_docker.sh -e dev -a <aws.env file path>
```

 * **prod** - This mode pulls the code directly from github master and runs the service.
 ```
 $ ./deploy_docker.sh -e prod -a <aws.env file path>
 ```
 * Select the container OS by passing -o parameter to deploy_docker.sh script with argument centos or ubuntu.
 ```
 $ ./deploy_docker.sh -e dev -o centos -a <aws.env file path>
 or
 $ ./deploy_docker.sh -e dev -o ubuntu -a <aws.env file path>
 ```
Please note that if you are running the service for the first time image build process will take some time; the subsequent runs will use the cached images and be faster.

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

