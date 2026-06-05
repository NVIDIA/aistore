// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"slices"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Static descriptor table (xact.Table) - one entry per xaction kind.
//
// The table is the source of truth for static xaction properties that would
// otherwise be scattered across the codebase: scope, startability, metadata
// effects, capacity refresh, idling behavior, mutual coexistence with global
// rebalance and node-local resilver, and generic status/wait reporting mode.
//
// Per-property semantics are documented inline on the Descriptor struct fields below.
//
// Key properties:
//   - Startable means the kind can be started via the generic StartXaction path.
//     Non-startable kinds may still be real xactions started through other paths.
//   - ConflictRebRes and AbortByReb define limited coexistence with rebalance
//     and resilver. They usually travel together; exceptions are deliberate and
//     documented next to their descriptor entries.
//   - Idles marks demand-driven xactions that may remain alive between requests.
//   - ICMode declares whether the kind supports the generic IC status/wait path.
//     ICNone does not mean "no status"; callers must use snaps-based or
//     action-specific status/wait.
//
// See xact/README.md for the full coexistence model, xaction lifecycle,
// IC vs snaps, status/wait models, and the theory of operation behind this table.

const (
	ScopeG  = iota + 1 // cluster
	ScopeB             // bucket
	ScopeGB            // (one bucket) | (all buckets)
	ScopeT             // target
)

// ICMode declares whether and how an xaction kind reports status to IC.
// When non-zero, targets notify IC members, and the status/wait API may query
// an IC proxy instead of polling all targets. When zero, the generic IC status
// path is not available; callers must use the snaps-based API or an
// action-specific status path.
//
// The flags are independent and may be combined:
// * ICUponTerm - target notifies IC on terminal state (finished/aborted);
// * ICUponProgress - target notifies IC periodically with progress
type ICMode uint8

const (
	ICUponTerm     ICMode = 1 << iota // -> core.UponTerm
	ICUponProgress                    // -> core.UponProgress
)

const ICNone ICMode = 0

type (
	Descriptor struct {
		DisplayName string          // as implied
		Access      apc.AccessAttrs // default access permissions; ais/proxy does most of the checking wo/ relying on these defaults
		Scope       int             // ScopeG (global), etc. - the enum above
		Startable   bool            // true if user can start this xaction (e.g., via `api.StartXaction`)
		Metasync    bool            // true if this xaction changes (and metasyncs) cluster metadata
		RefreshCap  bool            // refresh capacity stats upon completion

		// see xreg for "limited coexistence"
		Rebalance      bool // moves data between nodes
		Resilver       bool // moves data between mountpaths
		ConflictRebRes bool // starting this job would conflict with rebalance or resilver that's currently in progress
		AbortByReb     bool // gets aborted upon rebalance (coincides with ConflictRebRes with very few exceptions)

		// xaction has an intermediate `idle` state whereby it "idles" between requests
		// (see related: xact/demand.go)
		Idles bool

		// xaction returns extended xaction-specific stats
		// (see related: `Snap.Ext` in core/xaction.go)
		ExtendedStats bool

		// suppress verbose per-state log records and keep only hk.OldAgeXshort (1m)
		// in registry history
		QuietBrief bool

		// IC reporting mode; see ICMode comment above
		ICMode ICMode
	}
)

////////////////
// Descriptor //
////////////////

var Table = map[string]Descriptor{
	// bucket-less xactions that will typically have a 'cluster' scope (with resilver being a notable exception)
	apc.ActElection:  {DisplayName: "elect-primary", Scope: ScopeG, Startable: false},
	apc.ActRebalance: {Scope: ScopeG, Startable: true, Metasync: true, Rebalance: true, ICMode: ICUponTerm},

	apc.ActETLInline: {Scope: ScopeG, Startable: false, AbortByReb: true, ICMode: ICUponTerm},

	// (one bucket) | (all buckets)
	apc.ActLRU:          {DisplayName: "lru-eviction", Scope: ScopeGB, Startable: true, ICMode: ICUponTerm},
	apc.ActStoreCleanup: {DisplayName: "cleanup", Scope: ScopeGB, Startable: true, ConflictRebRes: true, ICMode: ICUponTerm},

	apc.ActSummaryBck: {
		DisplayName: "summary",
		Scope:       ScopeGB,
		Access:      apc.AceObjLIST | apc.AceBckHEAD,
		Startable:   false,
		Metasync:    false,
		// ICMode: ICNone - synchronous; proxy aggregates per-target results and returns to client
	},
	apc.ActSummaryShard: {
		DisplayName: "shard-summary",
		Scope:       ScopeB,
		Access:      apc.AceObjLIST | apc.AceBckHEAD,
		Startable:   false,
		Metasync:    false,
	},

	// single target (node)
	apc.ActResilver: {Scope: ScopeT, Startable: true, Resilver: true}, // ICMode: ICNone - ScopeT, single-target, no aggregation
	apc.ActRechunk:  {Scope: ScopeB, Startable: true, RefreshCap: true, ConflictRebRes: true, AbortByReb: true, ICMode: ICUponTerm},

	// IndexShard is a best-effort build: stale entries are detected via LOM checksum
	// and fall back to tar.Next() scan. A partial index remains useful, and resumed
	// builds atomically skip already-indexed LOMs (lom.md.flags&Indexed + index object).
	apc.ActIndexShard: {Scope: ScopeB, Startable: true, RefreshCap: false, ConflictRebRes: true, AbortByReb: false, ICMode: ICUponTerm},

	// on-demand EC and n-way replication
	// (non-startable, triggered by PUT => erasure-coded or mirrored bucket)
	apc.ActECGet:     {Scope: ScopeB, Startable: false, Idles: true, ExtendedStats: true},
	apc.ActECPut:     {Scope: ScopeB, Startable: false, RefreshCap: true, Idles: true, ExtendedStats: true},
	apc.ActECRespond: {Scope: ScopeB, Startable: false, Idles: true},
	apc.ActPutCopies: {Scope: ScopeB, Startable: false, RefreshCap: true, Idles: true},

	//
	// on-demand multi-object
	//
	apc.ActArchive: {Scope: ScopeB, Access: apc.AccessRW, Startable: false, RefreshCap: true, Idles: true},

	// TODO: support ICUponProgress
	apc.ActCopyObjects: {
		DisplayName:    "copy-objects",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // apc.AceCreateBucket is checked as well but only if ais://dst doesn't exist
		Startable:      false,
		RefreshCap:     true,
		Idles:          true,
		ConflictRebRes: true,
		AbortByReb:     true,
	},

	// TODO: support ICUponProgress
	apc.ActETLObjects: {
		DisplayName:    "etl-objects",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // ditto
		Startable:      false,
		RefreshCap:     true,
		Idles:          true,
		ConflictRebRes: true,
		AbortByReb:     true,
	},

	apc.ActBlobDl: {Access: apc.AccessRW, Scope: ScopeB, Startable: true, AbortByReb: true, RefreshCap: true, ICMode: ICUponTerm},

	// target pushes periodic progress (tgtdl.go); also finalizes on term
	apc.ActDownload: {Access: apc.AccessRW, Scope: ScopeG, Startable: false, Idles: true, AbortByReb: true, ICMode: ICUponTerm | ICUponProgress},

	// in its own class
	apc.ActDsort: {
		DisplayName:    "dsort",
		Scope:          ScopeB,
		Access:         apc.AccessRW,
		Startable:      false,
		RefreshCap:     true,
		ExtendedStats:  true,
		ConflictRebRes: true,
		AbortByReb:     true,
		// ICMode: ICNone - dsort has its own manager/notification machinery (see ext/dsort)
	},

	// multi-object
	apc.ActPromote: {
		DisplayName: "promote-files",
		Scope:       ScopeB,
		Access:      apc.AcePromote,
		Startable:   false,
		RefreshCap:  true,
		ICMode:      ICUponTerm,
	},
	apc.ActEvictObjects: {
		DisplayName: "evict-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
		ICMode:      ICUponTerm,
	},
	apc.ActEvictRemoteBck: {
		DisplayName: "evict-remote-bucket",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
		// TODO: migrate to ICUponTerm; currently:
		// - keep-md is multi-object evict;
		// - full destroy is synchronous BMD update + per-target DestroyBucket, not IC-tracked
	},
	apc.ActDeleteObjects: {
		DisplayName: "delete-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
		ICMode:      ICUponTerm,
	},

	// TODO: support ICUponProgress
	apc.ActPrefetchObjects: {
		DisplayName: "prefetch-objects",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		RefreshCap:  true,
		ICMode:      ICUponTerm,
	},

	// entire bucket (storage svcs)
	apc.ActECEncode: {
		DisplayName:    "ec-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW,
		Startable:      true,
		Metasync:       true,
		RefreshCap:     true,
		ConflictRebRes: true,
		AbortByReb:     true,
		ICMode:         ICUponTerm,
	},
	apc.ActMakeNCopies: {
		DisplayName: "mirror",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		Metasync:    true,
		RefreshCap:  true,
		ICMode:      ICUponTerm,
	},
	apc.ActMoveBck: {
		DisplayName:    "rename-bucket",
		Scope:          ScopeB,
		Access:         apc.AceMoveBucket,
		Startable:      false, // executing this one cannot be done via `api.StartXaction`
		Metasync:       true,
		Rebalance:      true,
		ConflictRebRes: true,
		AbortByReb:     true,
		ICMode:         ICUponTerm,
	},
	apc.ActCopyBck: {
		DisplayName:    "copy-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // apc.AceCreateBucket ditto
		Startable:      false,        // ditto
		Metasync:       true,
		RefreshCap:     true,
		ConflictRebRes: true,
		AbortByReb:     true,
		ICMode:         ICUponTerm,
	},
	apc.ActETLBck: {
		DisplayName:    "etl-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // ditto
		Startable:      false,        // ditto
		Metasync:       true,
		RefreshCap:     true,
		ConflictRebRes: true,
		AbortByReb:     true,
		ICMode:         ICUponTerm,
	},

	// in re IC: list-objects clients stream pages directly; 'show job' uses snaps; zero WaitForXactionIC callers
	apc.ActList: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Idles: true, QuietBrief: true, ICMode: ICNone},

	// x-moss; IC: ICNone - proxy-coordinated, target-direct status
	apc.ActGetBatch: {Scope: ScopeGB, Startable: false, Metasync: false, ConflictRebRes: true, AbortByReb: true, Idles: true, QuietBrief: true},

	apc.ActCreateNBI: {Scope: ScopeB, Startable: false, Metasync: false, ConflictRebRes: true, AbortByReb: true, Idles: false, ICMode: ICUponTerm},

	// metadata-cache management, internal usage
	apc.ActLoadLomCache: {DisplayName: "warm-up-metadata", Scope: ScopeB, Startable: true},
}

func GetDescriptor(kindOrName string) (string, Descriptor, error) {
	kind, dtor := getDtor(kindOrName)
	if dtor == nil {
		return "", Descriptor{}, cos.NewErrNotFoundFmt(nil, "xaction kind (or name) %q", kindOrName)
	}
	return kind, *dtor, nil
}

func GetKindName(kindOrName string) (kind, name string) {
	if kindOrName == "" {
		return
	}
	var dtor *Descriptor
	kind, dtor = getDtor(kindOrName)
	if dtor == nil {
		return
	}
	name = dtor.DisplayName
	if name == "" {
		name = kind
	}
	return
}

func GetSimilar(kindOrName string) (simKind, simName string) {
	for kind, dtor := range Table {
		if kind == kindOrName || dtor.DisplayName == kindOrName {
			return kind, dtor.DisplayName
		}
		// e.g., "prefetch" vs "prefetch-listrange"
		for _, s := range []string{kind, dtor.DisplayName} {
			if strings.HasPrefix(s, kindOrName) && len(s) > len(kindOrName) {
				if s[len(kindOrName)] == '-' {
					if simKind != "" {
						return "", "" // ambiguity
					}
					simKind, simName = kind, cos.Left(dtor.DisplayName, kind)
					break
				}
			}
		}
	}
	return
}

func IdlesBeforeFinishing(kindOrName string) bool {
	_, dtor := getDtor(kindOrName)
	debug.Assert(dtor != nil)
	return dtor.Idles
}

func ListDisplayNames(onlyStartable bool) (names []string) {
	names = make([]string, 0, len(Table))
	for kind, dtor := range Table {
		if onlyStartable && !dtor.Startable {
			continue
		}
		name := cos.Ternary(dtor.DisplayName != "", dtor.DisplayName, kind)
		debug.Assert(!slices.Contains(names, name), names, " vs ", name)
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func IsSameScope(kindOrName string, scs ...int) bool {
	_, dtor := getDtor(kindOrName)
	if dtor == nil {
		return false
	}
	scope, scope2 := scs[0], 0
	if len(scs) > 1 {
		scope2 = scs[1]
	}
	return dtor.Scope == scope || dtor.Scope == scope2
}

func getDtor(kindOrName string) (string, *Descriptor) {
	if dtor, ok := Table[kindOrName]; ok {
		return kindOrName, &dtor
	}
	for kind, dtor := range Table {
		if dtor.DisplayName == kindOrName {
			return kind, &dtor
		}
	}
	return "", nil
}
