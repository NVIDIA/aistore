# Generate files prior to running, e.g.:
```bash
$ mkdir /tmp/w; for f in {10000..99999}; do echo "$RANDOM -- $RANDOM" > /tmp/w/$f.txt; done
```

# Run:
```bash
$ go test -bench=. -benchtime=20s -benchmem
```
