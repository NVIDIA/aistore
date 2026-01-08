## Developer Notes

Resilver is implemented as a multi-worker traversal over all mountpaths on a target. Each worker — called a *jogger* — walks filesystem trees and processes objects it encounters.

All joggers operate independently. There is no central coordinator, no shared queues, and no global ordering. Coordination emerges from deterministic algorithms and object-level locking.

This design ensures:

* full coverage (all objects are eventually visited),
* scalability (I/O parallelism),
* correctness for chunked objects,
* and robustness under preemption.

This document is structured as follows:

- [Multi-Jogger Traversal](#multi-jogger-traversal)
- [Preemption Model](#preemption-model)
- [Primary Copy](#primary-copy)
- [Object Repair Flow](#object-repair-flow)
- [Copy Cleanup and Restoration](#copy-cleanup-and-restoration)
- [Locking Strategy](#locking-strategy)
- [Statistics and Accounting](#statistics-and-accounting)
- [Testing and Validation](#testing-and-validation)
- [See also](#see-also)

---

### Multi-Jogger Traversal

Every mountpath is walked. This is essential.

Chunk manifests and chunks may reside on different mountpaths. Restricting traversal to only newly attached or detached mountpaths would miss objects whose metadata lives elsewhere. The multi-jogger design ensures that every manifest is discovered regardless of where it is stored.

Joggers may encounter the same logical object via different physical files. Correctness does not rely on which jogger sees an object first.

---

### Preemption Model

Resilver is explicitly preemptible.

Any mountpath event aborts the current resilver. The abort is cooperative: joggers notice the abort condition and exit. A new resilver starts immediately with the updated mountpath configuration.

This prevents wasted work and avoids subtle inconsistencies that would arise from completing a run based on outdated volume topology.

---

### Primary Copy

When the main replica is missing but one or more copies exist, multiple joggers may encounter different copies.

To prevent duplicate restoration, resilver deterministically selects a single **primary copy**. The selection rule is simple: the lexicographically smallest FQN among all existing copies wins.

Only the jogger processing that primary copy performs restoration. All others skip their copies. This requires no locks and no coordination between joggers — the decision is purely local and deterministic.

---

### Object Repair Flow

For each object encountered, resilvering follows this logic:

1. Determine the object’s HRW mountpath under the current configuration.
2. If the object is already correct, proceed to copy maintenance if applicable.
3. If the object is misplaced:

   * copy it to the HRW mountpath,
   * load the new main replica,
   * update metadata.
4. For mirrored objects:

   * remove stale copy metadata,
   * create missing copies until the configured count is reached or no eligible mountpaths remain.

Main replica restoration always happens before copy restoration.

---

### Copy Cleanup and Restoration

Resilver first removes metadata entries for copies on unavailable or disabled mountpaths. This step is necessary to avoid counting copies that no longer exist or cannot be accessed.

After cleanup, resilvering counts remaining valid copies and compares against the bucket's mirror configuration. Missing copies are created on eligible mountpaths selected using least-utilization heuristics.

Copy restoration is always performed by the same jogger that restored (or verified) the main replica, ensuring consistency and avoiding races.

---

### Locking Strategy

Resilver must coexist with normal I/O.

Objects are locked during relocation to prevent concurrent modification. However, aggressively blocking on locks would stall resilver under load.

The locking strategy is intentionally tiered:

* A non-blocking try-lock is attempted first.
* Blocking retries are reserved for important objects:
  * main replicas at HRW locations,
  * mirrored objects requiring copy maintenance.
* Non-HRW copies in non-mirrored buckets are skipped if busy.

This balances progress with minimal disruption.

---

### Statistics and Accounting

Resilver statistics intentionally track **repairs**, not traversal.

Reported statistics increments only when a main replica is restored to its HRW mountpath. Copy creation does not increment this counter. This makes progress meaningful even when mirroring is disabled and avoids overstating work.

---

### Testing and Validation

Resilver is [tested](https://github.com/NVIDIA/aistore/tree/main/ais/test) under:

* concurrent mountpath operations
* repeated preemption
* erasure-coded, mirrored and chunked buckets
* partial failures and restarts

Stress tests verify convergence rather than step-by-step behavior. The system is considered correct if, after mountpaths stabilize, all objects end up correctly placed with the configured redundancy.

### See also

* [Space Cleanup](https://github.com/NVIDIA/aistore/blob/main/space/README.md)

