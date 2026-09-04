# Delve Debugging (experimental)

> **Experimental**: local-playground only. Not intended for `deploy/dev/docker`, `deploy/dev/k8s`, or any shared/production deployment.

[Delve](https://github.com/go-delve/delve) (`dlv`) support lets you attach a Go debugger to a locally deployed `aisnode` proxy/target.

## Usage

```console
$ AIS_USE_DLV=true make deploy
```

* `dlv` is auto-installed (`go install github.com/go-delve/delve/cmd/dlv@latest`) if not already on your `$PATH`.
* Forces `MODE=debug` (disables compiler optimizations/inlining) unless `MODE` is already set, for accurate stepping and variable inspection.
* Bumps `timeout.startup_time`/`timeout.join_startup_time` to `30m`/`60m` (config defaults: `1m`/`3m`), unless `AIS_STARTUP_TIME`/`AIS_JOIN_STARTUP_TIME` are already set. A daemon paused at a breakpoint stops responding to keepalive/join, which can otherwise trip these timeouts and make a peer self-terminate with `cluster startup is taking unusually long time...` in its log.
* Every daemon runs headless under `dlv exec`, one TCP port per daemon starting at `DLV_PORT` (default `56268`, incremented per daemon in deploy order). Daemons start running immediately (`--continue`) - `dlv` only pauses execution once you set a breakpoint after attaching.
* Attach to a specific daemon at any time (its port is printed at deploy time) with:

  ```console
  $ dlv connect 127.0.0.1:<port>
  ```

* `make kill` (or `deploy/dev/local/kill.sh`) also stops any `dlv` processes started this way.
* Before disconnecting (`exit`), run `continue` first - detaching a client while the process is paused (e.g. mid-command in a non-interactive/piped `dlv connect` session) can leave it ptrace-stopped, which looks like a hang since the daemon stops serving requests until resumed.

## Debugging with Delve

Once attached (`dlv connect 127.0.0.1:<port>`), you're in the `(dlv)` CLI REPL, live-attached to a running daemon. A few commands to get started:

```console
(dlv) break ais/tgtcp.go:1212       # set a breakpoint at file:line
(dlv) break pkgname.FuncName        # or by function name, e.g. ais.(*proxy).metasyncHandler
(dlv) continue                      # resume execution (alias: c) - runs until a breakpoint hits
(dlv) next                          # step over (alias: n)
(dlv) step                          # step into (alias: s)
(dlv) stepout                       # step out of the current function
(dlv) print someVar                 # inspect a variable (alias: p)
(dlv) locals                        # print all local variables in the current frame
(dlv) args                          # print function arguments
(dlv) bt                            # backtrace of the current goroutine (alias: stack)
(dlv) goroutines                    # list all goroutines
(dlv) goroutine <id>                # switch to a specific goroutine, e.g. before bt/locals
(dlv) breakpoints                   # list active breakpoints
(dlv) clear <id>                    # remove a breakpoint
(dlv) continue                      # always resume before disconnecting - see note above
(dlv) exit
```

Since the daemon starts under `--continue` (running immediately), a `break` you set only fires the next time that code path executes - e.g. set it, then trigger the request/operation via the CLI (`ais ...`) or another client, and `dlv` will halt when it's hit.

For the full command reference, see the [Delve CLI documentation](https://github.com/go-delve/delve/tree/master/Documentation/cli) and the [Delve docs index](https://github.com/go-delve/delve/tree/master/Documentation) (IDE integrations, Go-version support matrix, etc.).

## Known limitations

This is a thin wrapper around `dlv exec` and inherits its constraints; it hasn't been exercised against every local-playground scenario, in particular:

* **Stale daemons/on-disk cluster state**: redeploying on top of already-running daemons - e.g. running `make clean` without `make kill` first, or reusing preconfigured (non-`/tmp`) mountpaths that `make clean` intentionally never touches - leaves stale in-memory or on-disk cluster metadata (Smap/VMD) around. A fresh deploy can then collide with it and crash with a cluster-integrity/split-brain error that has nothing to do with `dlv` itself. To catch this early, `AIS_USE_DLV=true` deploys now fail fast with an actionable message if any `aisnode`/`dlv` process is still running - always `make kill` (and `make clean` if reusing test mountpaths) before redeploying. If you're on preconfigured mountpaths and still hit a split-brain error, the stale metadata lives on disk at each mountpath (not `/tmp`) and needs clearing manually.
* **Stale or edited configs**: if you hand-edit `~/.ais*/ais.json` / `ais_local.json` between runs, `aisnode` may fail to start the same way it would without `dlv` - `dlv exec` does not change config validation behavior, but startup failures are easy to miss since dlv's own output is interleaved with `aisnode`'s. If a daemon looks stuck (no "listening on" log line), check its output directly.
* Unauthenticated, unencrypted debug listener (fine on localhost only - never expose these ports beyond your machine).
