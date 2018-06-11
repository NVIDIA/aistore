## Getting started easily with DFC using Docker

1. `docker pull liangdrew/dfc`
2. `docker run -di liangdrew/dfc`
3. `./easy_start_dfc.sh`

[`easy_start_dfc.sh`](easy_start_dfc.sh) will deploy DFC with one proxy, one target, one local cache directory, and will be configured to use AWS as the cloud provider.

### Configuration

By default, the proxy will be deployed on port 8080 and the target on port 8081.

To change the ports DFC will be deployed on, set the `PORT` environment variable.
For example: `$ PORT=8082 ./easy_start_dfc.sh`
 
The proxy will be then deployed on port `$PORT` and the target on port `$PORT+1`.