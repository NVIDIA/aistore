## Getting started easily with DFC using Docker

`./easy_start_dfc.sh`

The above command will pull in the latest DFC docker image from Docker Hub, start the container, and start a deployment with one proxy and one target.

### Configuration

By default, the proxy will be deployed on port 8080 and the target on port 8081.

To change the ports DFC will be deployed on, set the `PORT` environment variable.
For example: `$ PORT=8082 ./easy_start_dfc.sh`
 
The proxy will be then deployed on port `$PORT` and the target on port `$PORT+1`.