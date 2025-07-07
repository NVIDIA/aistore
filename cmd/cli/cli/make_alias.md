## When to Use `makeAlias`

When reusing a command across different CLI namespaces, decide whether `makeAlias` is appropriate based on the following criteria:

1. Does the command name make sense in the new namespace context?
2. Are the flag requirements different in this context?
3. Would users expect different behavior or capabilities?
4. Does the help text reference command paths that need updating?

If **any** answer is "yes", consider using `makeAlias`. If **all** are "no", direct command reuse is likely the better choice.

### 1. Contextual Naming
**Scenario:** Different namespaces may require different command names.

**Example:** `showCmdBucket` reused as "show" in the bucket namespace, "bucket" in the show namespace:
- `ais bucket show` (direct: `showCmdBucket` with `Name: "show"`)
- `ais show bucket` (alias: `makeAlias(&showCmdBucket, "bucket", "", nil, nil)`)

### 2. Flag Modifications
**Scenario:** Commands may need different flag sets depending on context.

**Example:** `bucketObjCmdCopy` used for bulk vs. single-object operations:
- `ais bucket copy` (direct: with bulk operation flags)
- `ais object copy` (alias: remove bulk flags, add object-specific flags)

### 3. Help Text Context
**Scenario:** Help output should reflect the aliased command path.

**Example:** `archBucketCmd` aliased to the object namespace:
- `ais archive bucket` (direct: help shows "ais archive bucket" examples)
- `ais object archive` (alias: help should show "ais object archive" examples)

### 4. Semantic Clarity
**Scenario:** Users may have different expectations based on namespace semantics.

**Example:** Storage operations vs. informational queries:
- `ais storage backup` (action: "backup the storage")
- `ais show backup` (query: "show backup status")

---

## When *Not* to Use `makeAlias`

### 1. Command Name is Already Appropriate
**Scenario:** The same command name fits both namespaces.

**Example:** `showCmdStgSummary` as "summary" in both:
- `ais storage summary` (direct: `showCmdStgSummary` with `Name: "summary"`)
- `ais show storage summary` (direct: same command, name is contextually correct)

### 2. Flags are Identical
**Scenario:** The command doesn't require any flag changes.

**Example:** Read-only `show` commands typically share the same flags across contexts.

### 3. Namespace Hierarchy is Logical
**Scenario:** Existing command structure aligns with user expectations.

**Example:** `ais show storage` offers multiple subviews (disk, mountpath, capacity, summary), each fitting cleanly within that hierarchy.

---

## Consistency Principle

Prioritize user expectations and mental models when designing CLI command structure. Code reuse is valuable, but user clarity always takes precedence.
