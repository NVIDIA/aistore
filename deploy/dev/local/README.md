This document is complementary to [Getting Started](/docs/getting_started.md) providing yet another example of
running non-containerized AIS locally.

Intended audience includes developers and first-time users, including those who'd never used Go before.

## `$GOPATH` and `$PATH`

There's a truckload of [tutorials](https://tip.golang.org/doc/tutorial/getting-started) and markdowns but the gist of it is very simple: you need to have `$GOPATH`.

Secondly, `$GOPATH/bin` must be in your `$PATH`. Something like:

```console
$ export PATH=$GOPATH/bin:$PATH
```

The intuition behind that is very simple: `$GOPATH` defines location for:

* Go sources
* Go packages
* Go binaries

Yes, all of the above, and respectively: `$GOPATH/src`, `$GOPATH/pkg`, and `$GOPATH/bin`.

And so, since you'd likely want to run binaries produced out of Go sources, you need to add the path, etc.

## Step 1: add two data drives into local configuration

```diff
--- a/deploy/dev/local/aisnode_config.sh
+++ b/deploy/dev/local/aisnode_config.sh
@@ -167,7 +167,8 @@ cat > $AIS_LOCAL_CONF_FILE <<EOL
                "port_intra_data":    "${PORT_INTRA_DATA:-10080}"
        },
        "fspaths": {
-               $AIS_FS_PATHS
+               "/sda/ais": {},
+               "/sdb/ais": {}
        },
        "test_fspaths": {
                "root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
```

## Step 2: two data drives is just about enough to run a single AIS target

Note that we are still running everything locally.

Therefore: remove the previously generated configuration (if any) and redeploy from scratch a minimal cluster consisting of a single gateway and a single target:

```console
$ deploy/scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --mountpath-cnt 0 --deployment local --remote-alias remais --gcp --aws

# or, same:
$ make kill clean cli deploy <<< $'1\n1\n0\ny\ny\nn\nn\n'
```

The result will looks something like:

```console
# make kill clean cli deploy <<< $'1\n1\n0\ny\ny\nn\nn\n'
Warning: missing CLI (ais) executable for proper graceful shutdown
Cleaning... done.
Enter number of storage targets:
Enter number of proxies (gateways):
Number of local mountpaths (enter 0 for preconfigured filesystems):
Select backend providers:
Amazon S3: (y/n) ?
Google Cloud Storage: (y/n) ?
Azure: (y/n) ?
HDFS: (y/n) ?
Create loopback devices (note that it may take some time): (y/n) ?
Building aisnode: version=100676b29 providers= aws gcp tags= aws gcp debug mono
done.
+ /root/gocode/bin/aisnode -config=/root/.ais0/ais.json -local_config=/root/.ais0/ais_local.json -role=proxy -ntargets=1
+ /root/gocode/bin/aisnode -config=/root/.ais1/ais.json -local_config=/root/.ais1/ais_local.json -role=target
Listening on port: 8080
```

## Step 3: run aisloader for 2 minutes (8 workers, 1KB size, 100% write, no cleanup)

```console
$ make aisloader

$ aisloader -bucket=ais://abc -duration 2m -numworkers=8 -minsize=1K -maxsize=1K -pctput=100 --cleanup=false
```

## Step 4: run iostat (or use any of the multiple [documented](/docs/prometheus.md) ways to monitor AIS performance)

```console
$ iostat -dxm 10 sda sdb
```

> The drives `/dev/sda` and `/dev/sdb` (and the mountpoints `/sda` and `/sdb`) are used here purely for illustrative purposes. And the two mountpaths, `/sda/ais` and `/sdb/ais`, could, of course, also be named differently.

