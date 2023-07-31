`loadgen.sh` is a simple script for generating disk load for benchmark tests

## Usage

Running `loadgen.sh` by itself lists all mount points.

```console
$ ./loadgen.sh
``` 

Mount points are passed to `loadgen.sh` to generate load. The load is randomly distributed across the passed in mount points. Note that a mount point can be passed in more than once to give it a higher weight.

Example with mount point passed in twice.

```console
$ ./loadgen.sh /ais/sda /ais/sdb /ais/sdb
```

Example with the `/` mount point

```console
$ sudo -E /bin/bash ./loadgen.sh /ais/sda / /ais/sdc
```

`loadgen.sh` also accepts a number environment variables

| Environment Variable Name | Description | Default Value |
| --- | --- | --- |
| seconds | duration in seconds | 50 |
| workers | number of workers | 1 |
| iobatch | number of io operations a worker performs on a mount point before switching | 1000 |
| pct_read | percentage of io operations that are read requests | 75 |

