# Package `hk` (housekeeper)

Housekeeper runs a single background goroutine that executes registered
callbacks at their respective, per-callback intervals. Throughout the
codebase, `hk` is the standard way to schedule recurring or deferred
maintenance: expiring idle xactions, pruning registries and caches,
releasing memory, closing idle connections, and similar.

A dedicated goroutine and timer per maintenance task would add unnecessary
scheduling and memory overhead: a busy AIS target may carry thousands of
scheduled actions at any point in time. Housekeeper multiplexes all of them
over one goroutine, one timer, and one min-heap of deadlines.

## Usage

```go
hk.Reg(name+hk.NameSuffix, callback, interval)
```

- The callback `func(now int64) time.Duration` returns the interval until
  its next invocation, or `hk.UnregInterval` to unregister itself.
- Registration with `interval == 0` invokes the callback immediately in
  the housekeeping goroutine and uses its return value as the first actual
  interval. Returning `UnregInterval` from this immediate call is illegal.
- Names must be unique; duplicate registration is dropped and logged as an
  error. Use `NameSuffix` for consistency.
- `hk.Unreg(name)` removes a registered action.
- `hk.UnregIf(name, cb)` likewise removes the action but tolerates
  non-presence; `cb` serves only as the non-nil marker for that behavior
  and is not compared with the registered callback.

Callbacks execute in the housekeeping goroutine and must be brief. A
callback that takes longer than a second is logged. Anything heavier
belongs in its own goroutine, with `hk` used only to trigger it.

## Implementation notes

**Mailbox and doorbell.** `Reg`, `Unreg`, and `UnregIf` append to an
unbounded, mutex-protected pending queue and ring a one-slot doorbell
channel. Two properties follow:

1. Senders never wait for housekeeping queue capacity. In particular, a
   caller holding higher-level locks, such as xaction renewal, cannot be
   parked by housekeeping backpressure.
2. Callbacks may themselves call `Reg`, `Unreg`, or `UnregIf`. The
   operation is queued and takes effect on the next pass of the
   housekeeping loop, without deadlocking the goroutine on its own
   channel.

If the queue is not draining, its length is logged at each power-of-two
length starting at the warning threshold.

**Scheduling.** Actions live in a binary min-heap keyed by absolute
monotonic wake time, with a name-to-index map maintained by specialized
heap primitives. Lookup is O(1), while registration and removal are
O(log n), regardless of the number of scheduled actions. When the timer
fires, the loop runs all due actions, not only the top one. Timer resets
are elided when the nearest wake-up target has not changed.

**Self-defense.** A callback returning a zero or negative interval is
clamped to one second and asserted in debug builds. An erroneous callback
must not be able to spin the housekeeping loop.

## Design rationale

Earlier implementations used a bounded operation channel and linear name
lookup. Under high registration churn, queue backpressure could escape the
housekeeper and stall callers holding unrelated higher-level locks.

The mailbox/doorbell and indexed heap keep housekeeping pressure local:
producers remain independent of consumer throughput, while scheduling cost
remains logarithmic as the action population grows.
