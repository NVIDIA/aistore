xmeta is a low-level utility to format (or extract into plain text)
assorted AIS control structures.

```console
$ go run cmd/xmeta/xmeta.go
Build:
        go install xmeta.go
Examples:
        xmeta -h                                                - show usage
        xmeta -x -in=~/.ais0/.ais.smap                          - extract Smap to STDOUT
        xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt       - extract Smap to /tmp/smap.txt
        xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap             - format plain-text /tmp/smap.txt
        xmeta -x -in=~/.ais0/.ais.bmd                           - extract BMD to STDOUT
        xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt         - extract BMD to /tmp/bmd.txt
        xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd               - format plain-text /tmp/bmd.txt
```
