Here's a quick sequence with detailed comments inline.

## From HTTP to HTTPS

This assumes that X.509 certificate already exists and the (HTTP-based) cluster is up and running. All we need to do at this point is switch it to HTTPS.

```console
# step 1: reconfigure cluster to use HTTPS
$ ais config cluster net.http.use_https true

# step 2: is optional, and is only required if the cert will fail validation
$ ais config cluster net.http.skip_verify true

# step 3: shutdown
$ ais cluster shutdown

# step 4: remove cluster map - all copies at all possible locations, for example:
$ find ~/.ais* -type f -name ".ais.smap" | xargs rm

# step 5: restart
$ make kill cli deploy <<< $'6\n6\n4\ny\ny\nn\nn\n'

# step 6: optionally, run aisloader
$ AIS_ENDPOINT=https://localhost:8080 aisloader -bucket=ais://nnn -cleanup=false -numworkers=8 -pctput=0 -randomproxy

# step 7: optionally, reconfigure CLI to skip X.509 verification:
$ ais config cli set cluster.skip_verify_crt true

# step 8: run CLI
$ AIS_ENDPOINT=https://127.0.0.1:8080 ais show cluster

$ AIS_ENDPOINT=https://127.0.0.1:8080 ais archive gen-shards "ais://abc/shard-{001..999}.tar.lz4"
Shards created: 999/999 [==============================================================] 100 %

$ export AIS_ENDPOINT=https://localhost:8080

$ ais ls ais://abc --summary
NAME           PRESENT         OBJECTS         SIZE (apparent, objects, remote)        USAGE(%)
ais://abc      yes             999 0           5.86MiB 5.20MiB 0B                      0%
...
...
```

Goes without saying that `localhost` etc. are used here (and elsewhere) for purely illustrative purposes.

Instead of `localhost`, `127.0.0.1`, port `8080`, and the `make` command above one must use their respective correct endpoints and proper deployment operations.

## From HTTPS back to HTTP

```console
# step 1: disable HTTPS
$ AIS_ENDPOINT=https://127.0.0.1:8080 ais config cluster net.http.use_https false

# step 2: shutdown (notice that we are still using HTTPS endpoint)
$ AIS_ENDPOINT=https://127.0.0.1:8080 ais cluster shutdown -y

# step 3: remove cluster maps
$ find ~/.ais* -type f -name ".ais.smap" | xargs rm

# step 4: restart
$ make kill cli deploy <<< $'6\n6\n4\ny\ny\nn\nn\n'

# step 5: and use
$ ais show cluster
```
