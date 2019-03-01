### Command-Line arguments

AIS proxy and AIS target (executables) both support the following command-line arguments where those that are *mandatory* are marked with `***`:

```shell
  -alsologtostderr
        log to standard error as well as files
  -config string
        config filename: the fully qualified name of the local daemon configuration (***)
  -confjson string
        JSON formatted "{name: value, ...}" string to override selected configuration knob(s)
  -dryobjsize string
        dry-run: in-memory random content (default 8MB)
  -log_backtrace_at value
        when logging hits line file:N, emit a stack trace
  -log_dir string
        log directory - location for the AIS logs (***)
  -loglevel string
        log verbosity level (2 - minimal, 3 - default, 4 - super-verbose)
  -logtostderr
        log to standard error instead of files
  -nodiskio
        dry-run: if true, no disk operations for GET and PUT
  -nonetio
        dry-run: if true, no network operations for GET and PUT
  -ntargets int
        number of storage targets to expect at startup (hint, proxy-only)
  -persist
        true: apply command-line args to the configuration and save the latter to disk
        false: keep it transient (for this run only)
  -proxyurl string
        primary proxy/gateway URL - allows to override local config
  -role string
        the role of this AIS daemon: proxy | target (***)
  -statstime duration
        stats reporting (logging) interval
  -stderrthreshold value
        logs at or above this threshold go to stderr
  -vmodule value
        comma-separated list of pattern=N settings for file-filtered logging
```
