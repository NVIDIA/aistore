You need to increase the maximum number of open file descriptors in all docker images running AIStore. 

Copy/replace [`limits.conf`](/deploy/conf/limits.conf) to/with `/etc/security/limits.conf`.

For more information, read [performance/maximum-number-of-open-files](https://github.com/NVIDIA/aistore/blob/master/docs/performance.md#maximum-number-of-open-files).