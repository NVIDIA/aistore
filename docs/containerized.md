# AIS in Containerized Environments

In production, AIStore clusters often run in containerized, sometimes constrained environments.

When AIS runs inside a constrained container, it:

- detects containerized execution at startup
- determines whether the environment uses cgroup v2 or cgroup v1
- applies a container-aware effective CPU count
- adjusts `GOMAXPROCS` accordingly
- reports container-scoped memory rather than host memory
- reports CPU utilization as a smoothed moving average
- tracks CPU throttling separately when cgroup v2 provides it

In practice, this means AIS runtime behavior and operator-facing metrics align more closely with the limits the container runtime actually enforces.

**Table of Contents**

- [Startup behavior](#startup-behavior)
- [CPU reporting](#cpu-reporting)
- [Interpreting CPU Signals](#interpreting-cpu-signals)
- [Memory reporting](#memory-reporting)
- [Kubernetes note](#kubernetes-note)
- [When to use `ForceContainerCPUMem`](#when-to-use-forcecontainercpumem)
- [How to validate the behavior](#how-to-validate-the-behavior)
- [Fallback behavior](#fallback-behavior)
- [Current scope and limitations](#current-scope-and-limitations)

## Startup behavior

AIS initializes CPU and memory accounting in two stages:

- a minimal package init that always has a safe default
- a runtime `sys.Init()` phase that performs container detection, resolves the cgroup version once, applies container-aware CPU count, and adjusts `GOMAXPROCS`

The container-detection step is best-effort. AIS checks for common markers such as `/.dockerenv` and well-known cgroup tokens including `docker`, `containerd`, `kubepods`, `kube`, `lxc`, `libpod`, and `podman`.

At startup, AIS logs the effective runtime context.

### Example startup log

```console
I 18:55:47.766809 daemon:323 Version 4.4.rc2.bbac8be09, build 2026-04-06T18:49:38+0000, CPUs(40, runtime=80), container:cgroup-v2
I 18:55:47.788269 daemon:265 Node t[QtxHjmTxD], Version 4.4.rc2.bbac8be09, build 2026-04-06T18:49:38+0000, CPUs(40, runtime=80), container:cgroup-v2
```

In this example:

* `runtime=80` is what the Go runtime sees on the host
* `CPUs(40, ...)` is the effective CPU count AIS uses after applying cgroup-aware accounting
* `container:cgroup-v2` means AIS detected a containerized cgroup-v2 environment

This is expected when a pod or container is allowed to use only a subset of the host’s CPU capacity.

## CPU reporting

AIS reports CPU utilization as a **smoothed moving average** rather than a one-shot instantaneous sample. The goal is to provide a more stable and more useful operational signal.

AIS distinguishes between:

* **CPU utilization**: how busy the node is
* **CPU throttling**: how much CPU time the runtime is denying to the container

These are related, but not the same. A node may show moderate utilization while still being throttled. In that case, the problem is not simply “CPU is busy” but “the container is not being allowed to use more CPU.”

### `ais show cluster`

The `ais show cluster` output includes, in particular:

* **SYS CPU(%)** by default
* **LOAD AVERAGE** only with `--verbose`
* **THROTTLED(%)** only when at least one node in the displayed proxy or target section reports non-zero cgroup-v2 throttling

`SYS CPU(%)` is the smoothed node CPU utilization reported by AIS.

`THROTTLED(%)` appears only in environments where AIS can observe CPU throttling via cgroup v2. It indicates how much CPU time the container is losing due to runtime throttling.

### Example: AIS/Kubernetes deployment

The following example shows what a healthy AIS 4.4 deployment may look like inside Kubernetes when nodes run under cgroup v2.

```console
$ ais show cluster

PROXY                    MEM USED(%)     MEM AVAIL       SYS CPU(%)      UPTIME          K8s POD         STATUS
p[br4ghom6tlsn5][P]      18.75%          1.18TiB         21%             643h3m30s       ais-proxy-15    online
p[ydggldkytams7]         0.97%           1.17TiB         9%              642h59m30s      ais-proxy-0     online
p[j4cchhwdmi5ft]         1.02%           1.17TiB         11%             642h59m50s      ais-proxy-1     online
p[tuimpcvr16mnu]         0.98%           1.19TiB         8%              643h0m10s       ais-proxy-2     online
p[ptzgio2qkk02z]         0.91%           1.20TiB         7%              643h0m20s       ais-proxy-3     online
...
...

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)     CAP AVAIL       SYS CPU(%)   THROTTLED(%)    UPTIME          K8s POD         STATUS
t[dysqeHCy]      0.14%           2.30TiB      62%             27.108TiB       34%          0%              685h39m20s      ais-target-0    online
t[bwLnMici]      0.14%           2.36TiB      64%             26.863TiB       41%          0%              685h38m0s       ais-target-1    online
t[tQbKmamu]      0.15%           2.35TiB      63%             27.118TiB       38%          1%              685h37m10s      ais-target-2    online
t[nOtbUodZ]      0.15%           2.32TiB      62%             27.090TiB       47%          0%              685h35m40s      ais-target-3    online
t[fqpzfbfx]      0.16%           2.34TiB      63%             26.721TiB       52%          2%              685h34m10s      ais-target-4    online
t[abRtepsE]      0.14%           2.30TiB      62%             27.587TiB       36%          0%              685h32m40s      ais-target-5    online
...
...

Cluster:
   CLI Endpoint:        https://mnk.abc.oci.aistore.nvidia.com:51080
   Proxies:             16 (all electable)
   Targets:             16 (total disks: 191)
   Capacity:            used 751.37TiB (64%), available 429.74TiB
   Cluster Map:         version 10946, UUID B7nP8-i7RQ, primary p[br4ghom6tlsn5]
   Software:            4.4.rc2.bbac8be09
   Backend:             AWS
   Deployment:          K8s
   Status:              32 online
   Authentication:      enabled
```

A few things are worth noting:

* `SYS CPU(%)` is the node CPU utilization reported by AIS, not the older load-average view.
* `THROTTLED(%)` appears only when at least one node in the displayed section reports non-zero cgroup-v2 throttling.
   - Small non-zero throttling values are not unusual in busy containerized environments.
* Memory and CPU totals shown by AIS should reflect the container’s effective limits rather than the host’s full physical capacity.

> The numbers above are illustrative, but the format and interpretation match AIS 4.4 behavior.

### Example: verbose view

Use `ais show cluster --verbose` to add `LOAD AVERAGE` to the default 4.4 cluster view. This is useful when you want the traditional 1-, 5-, and 15-minute load numbers alongside the newer AIS CPU metrics.

```console
$ ais show cluster --verbose

PROXY                 MEM USED(%)     MEM AVAIL       SYS CPU(%)      LOAD AVERAGE         UPTIME          K8s POD         STATUS
p[br4ghom6tlsn5][P]   18.75%          1.18TiB         21%             [6.2 6.9 7.2]        643h3m30s       ais-proxy-15    online
p[ydggldkytams7]      0.97%           1.17TiB         9%              [12.4 11.5 11.4]     642h59m30s      ais-proxy-0     online
p[j4cchhwdmi5ft]      1.02%           1.17TiB         11%             [8.8 7.6 7.4]        642h59m50s      ais-proxy-1     online
p[tuimpcvr16mnu]      0.98%           1.19TiB         8%              [6.5 6.9 6.9]        643h0m10s       ais-proxy-2     online
p[ptzgio2qkk02z]      0.91%           1.20TiB         7%              [7.2 7.7 7.8]        643h0m20s       ais-proxy-3     online
...
...

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)     CAP AVAIL       SYS CPU(%)   THROTTLED(%)   LOAD AVERAGE      UPTIME          K8s POD         STATUS
t[dysqeHCy]      0.14%           2.30TiB      62%             27.108TiB       34%          0%             [12.4 11.5 11.4]  685h39m20s      ais-target-0    online
t[bwLnMici]      0.14%           2.36TiB      64%             26.863TiB       41%          0%             [8.8 7.9 7.6]     685h38m0s       ais-target-1    online
t[tQbKmamu]      0.15%           2.35TiB      63%             27.118TiB       38%          1%             [7.2 7.7 7.8]     685h37m10s      ais-target-2    online
t[nOtbUodZ]      0.15%           2.32TiB      62%             27.090TiB       47%          0%             [7.4 10.0 10.9]   685h35m40s      ais-target-3    online
t[fqpzfbfx]      0.16%           2.34TiB      63%             26.721TiB       52%          2%             [8.0 9.5 9.8]     685h34m10s      ais-target-4    online
t[abRtepsE]      0.14%           2.30TiB      62%             27.587TiB       36%          0%             [6.1 6.6 6.9]     685h32m40s      ais-target-5    online
...
...

```

## Interpreting CPU Signals

AIS 4.4 exposes several CPU-related signals, but they answer different questions.

`SYS CPU(%)` is the primary AIS CPU signal. It is computed from recent CPU usage and reported as a smoothed moving average. `THROTTLED(%)` is separate: it indicates CPU time the runtime is denying to the container in cgroup-v2 environments. `LOAD AVERAGE` remains available with `--verbose`, but it is secondary and should be treated as additional context rather than the main CPU metric.

| What you see | Likely interpretation | What to check next |
|---|---|---|
| **High `LOAD AVERAGE`, moderate `SYS CPU(%)`** | The node may be under pressure, but not necessarily from sustained CPU execution. Think queueing, contention, or blocked work rather than “CPU fully busy.” | Check whether `THROTTLED(%)` is non-zero. In containerized environments, CPU starvation can show up even when `SYS CPU(%)` is only moderate. |
| **High `SYS CPU(%)`, moderate `LOAD AVERAGE`** | CPUs are genuinely busy right now. This is the clearest sign of active CPU work. | Treat `SYS CPU(%)` as the primary signal. Look for sustained values, workload spikes, or competing foreground/background activity. |
| **High `SYS CPU(%)` and high `THROTTLED(%)`** | The node is both busy and being denied CPU by the runtime. | This is usually the strongest signal of CPU pressure in cgroup-v2 environments. Consider increasing CPU limits or reducing contention. |
| **Low or moderate `SYS CPU(%)`, non-zero `THROTTLED(%)`** | The container may still be short on CPU availability. Throttling indicates lack of CPU entitlement, not just high usage. | Investigate container CPU limits and runtime throttling before assuming the node is healthy. |
| **Low `SYS CPU(%)`, low `THROTTLED(%)`, high `LOAD AVERAGE`** | Pressure may be coming from outside the immediate CPU-utilization path. | Treat load average as context, not proof of CPU saturation. Check for queued work, transient backlog, or other resource contention. |

## Memory reporting

Memory reporting also changes based on the detected runtime environment.

On bare metal, AIS reads host memory information from `/proc/meminfo`.

In containerized Linux environments:

* for **cgroup v2**, AIS uses `memory.max`, `memory.current`, and `memory.stat`
* for **cgroup v1**, AIS uses the corresponding v1 memory control files

If the container has no explicit memory limit, AIS falls back to host memory reporting.

This means that memory shown by AIS inside a constrained container should reflect the container’s configured limit rather than the host’s total RAM.

AIS also treats some auxiliary memory details as best-effort so that missing or unreadable secondary files do not cause runtime failure.

## Kubernetes note

On Kubernetes, cgroup v2 control files often live under a pod-specific subtree rather than directly under `/sys/fs/cgroup/`.

The exact path depends on the container runtime and cgroup namespace configuration — CRI-O, containerd, systemd-nspawn, and `cgroupns=private` setups all produce different nested layouts. For example, plain Docker typically mounts the cgroup root directly at `/sys/fs/cgroup/`, while a Kubernetes pod might see its files under something like `/sys/fs/cgroup/kubepods.slice/.../crio-<hash>.scope/`.

AIS handles this by reading the process's own cgroup membership from `/proc/self/cgroup` at startup and resolving the actual file paths from there, rather than assuming a single hardcoded location. This means the same binary works whether it runs in a flat Docker container, a nested K8s pod, or any other cgroup v2 layout — no configuration required.

## When to use `ForceContainerCPUMem`

The `ForceContainerCPUMem` [feature flag](/docs/feature_flags.md) can override failed container auto-detection.

In the unlikely event that auto-detection fails, enable `ForceContainerCPUMem` and restart the node or the entire cluster.

This forces cgroup-based CPU and memory accounting and is intended for unusual deployments.

Use it when:

* AIS is running in a container or pod
* startup logs do not show "container" in the first 10-12 lines
* reported CPU or memory clearly matches the host instead of the container

## How to validate the behavior

A simple way to validate container-aware CPU and memory reporting is to compare the same test or deployment on the host and inside a constrained container.

AIS includes a `sys` package unit test that you can run inside a constrained container to verify container-aware CPU and memory accounting:

```console
docker run --rm \
  --cpus=1.5 \
  --memory=512m \
  -v "$PWD":/src -w /src \
  -v "$HOME/go/pkg/mod":/go/pkg/mod \
  -v "$HOME/.cache/go-build":/root/.cache/go-build \
  golang:1.26 \
  go test ./sys -run . -v -count=1 2>&1
```

In that setup:

* CPU count and memory totals should reflect the container’s limits rather than the host’s capacity
* changing `--memory=512m` to `--memory=4G` should change the observed memory totals accordingly
* the same workload should produce different CPU and memory observations on host versus inside the container

For a running AIS deployment, practical checks are:

1. inspect startup logs for `container:cgroup-v2` or `container:cgroup-v1`
2. compare `CPUs(..., runtime=...)` in the startup line
3. run `ais show cluster`
4. confirm that `SYS CPU(%)` is shown by default
5. look for `THROTTLED(%)` in constrained cgroup-v2 environments

## Fallback behavior

AIS prefers the most appropriate source for the current environment, but it also degrades gracefully.

In general:

* CPU reporting tries to preserve a usable percentage whenever possible
* memory reporting prefers host stats over failing hard when container-specific files are unavailable
* cgroup probing is done once at startup, not repeatedly during steady-state runtime

This keeps the behavior predictable and avoids repeated source-selection churn while the process is running.

## Current scope and limitations

Starting with v4.4, AIStore fully supports **cgroup v2** and continues to support **cgroup v1**.

> Note that cgroup v1 support is considered deprecated and may be removed in a future release.

Future work may include additional CPU pressure signals and support for non-Linux platforms (such as macOS where Linux cgroups are not available).
