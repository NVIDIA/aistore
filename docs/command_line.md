### Command-Line arguments

AIS proxy and AIS target (executables) both support the following command-line arguments where those that are *mandatory* are marked with `***`:

```
  -role string
        the role of this AIS daemon: proxy | target (***)
  -config string
        config filename: local file that stores configuration of this daemon (***)
  -alsologtostderr
        log to standard error as well as files
  -config_custom string
        "key1=value1,key2=value2" formatted string to override selected entries in config
  -dryobjsize string
        dry-run: in-memory random content (default 8MB)
  -log_backtrace_at value
        when logging hits line file:N, emit a stack trace
  -logtostderr
        log to standard error instead of files
  -nodiskio
        dry-run: if true, no disk operations for GET and PUT
  -ntargets int
        number of storage targets to expect at startup (hint, proxy-only)
  -transient
        false: apply command-line args to the configuration and save the latter to disk
        true: keep it transient (for this run only)
  -stderrthreshold value
        logs at or above this threshold go to stderr
  -vmodule value
        comma-separated list of pattern=N settings for file-filtered logging
```

> Use `--help` for the most recently updated set of command-line options, for instance:

```console
$ $GOPATH/bin/aisnode --help
```
