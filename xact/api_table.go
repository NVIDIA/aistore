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

const (
	ScopeG  = iota + 1 // cluster
	ScopeB             // bucket
	ScopeGB            // (one bucket) | (all buckets)
	ScopeT             // target
)

type (
	Descriptor struct {
		DisplayName string          // as implied
		Access      apc.AccessAttrs // access permissions (see: apc.Access*)
		Scope       int             // ScopeG (global), etc. - the enum above
		Startable   bool            // true if user can start this xaction (e.g., via `api.StartXaction`)
		Metasync    bool            // true if this xaction changes (and metasyncs) cluster metadata
		RefreshCap  bool            // refresh capacity stats upon completion

		// see xreg for "limited coexistence"
		Rebalance      bool // moves data between nodes
		Resilver       bool // moves data between mountpaths
		ConflictRebRes bool // conflicts with rebalance/resilver
		AbortRebRes    bool // gets aborted upon rebalance/resilver - currently, all `ext`-ensions

		// xaction has an intermediate `idle` state whereby it "idles" between requests
		// (see related: xact/demand.go)
		Idles bool

		// xaction returns extended xaction-specific stats
		// (see related: `Snap.Ext` in core/xaction.go)
		ExtendedStats bool

		// suppress verbose per-state log records and keep only hk.OldAgeXshort (1m)
		// in registry history
		QuietBrief bool
	}
)

////////////////
// Descriptor //
////////////////

// `xact.Table` is a static, public, and global Kind=>[Xaction Descriptor] map that contains
// xaction kinds and static properties, such as `Startable`, `Owned`, etc.
// In particular, "startability" is narrowly defined as ability to start xaction
// via `api.StartXaction`
// (whereby copying bucket, for instance, requires a separate `api.CopyBucket`, etc.)
var Table = map[string]Descriptor{
	// bucket-less xactions that will typically have a 'cluster' scope (with resilver being a notable exception)
	apc.ActElection:  {DisplayName: "elect-primary", Scope: ScopeG, Startable: false},
	apc.ActRebalance: {Scope: ScopeG, Startable: true, Metasync: true, Rebalance: true},

	apc.ActETLInline: {Scope: ScopeG, Startable: false, AbortRebRes: true},

	// (one bucket) | (all buckets)
	apc.ActLRU:          {DisplayName: "lru-eviction", Scope: ScopeGB, Startable: true},
	apc.ActStoreCleanup: {DisplayName: "cleanup", Scope: ScopeGB, Startable: true},
	apc.ActSummaryBck: {
		DisplayName: "summary",
		Scope:       ScopeGB,
		Access:      apc.AceObjLIST | apc.AceBckHEAD,
		Startable:   false,
		Metasync:    false,
	},

	// single target (node)
	apc.ActResilver: {Scope: ScopeT, Startable: true, Resilver: true},
	apc.ActRechunk:  {Scope: ScopeB, Startable: true, RefreshCap: true, ConflictRebRes: true},

	// on-demand EC and n-way replication
	// (non-startable, triggered by PUT => erasure-coded or mirrored bucket)
	apc.ActECGet:     {Scope: ScopeB, Startable: false, Idles: true, ExtendedStats: true},
	apc.ActECPut:     {Scope: ScopeB, Startable: false, RefreshCap: true, Idles: true, ExtendedStats: true},
	apc.ActECRespond: {Scope: ScopeB, Startable: false, Idles: true},
	apc.ActPutCopies: {Scope: ScopeB, Startable: false, RefreshCap: true, Idles: true},

	//
	// on-demand multi-object (consider setting ConflictRebRes = true)
	//
	apc.ActArchive: {Scope: ScopeB, Access: apc.AccessRW, Startable: false, RefreshCap: true, Idles: true},
	apc.ActCopyObjects: {
		DisplayName: "copy-objects",
		Scope:       ScopeB,
		Access:      apc.AccessRW, // apc.AceCreateBucket is checked as well but only if ais://dst doesn't exist
		Startable:   false,
		RefreshCap:  true,
		Idles:       true,
	},
	apc.ActETLObjects: {
		DisplayName: "etl-objects",
		Scope:       ScopeB,
		Access:      apc.AccessRW, // ditto
		Startable:   false,
		RefreshCap:  true,
		Idles:       true,
		AbortRebRes: true,
	},

	apc.ActBlobDl: {Access: apc.AccessRW, Scope: ScopeB, Startable: true, AbortRebRes: true, RefreshCap: true},

	apc.ActDownload: {Access: apc.AccessRW, Scope: ScopeG, Startable: false, Idles: true, AbortRebRes: true},

	// in its own class
	apc.ActDsort: {
		DisplayName:    "dsort",
		Scope:          ScopeB,
		Access:         apc.AccessRW,
		Startable:      false,
		RefreshCap:     true,
		ConflictRebRes: true,
		ExtendedStats:  true,
		AbortRebRes:    true,
	},

	// multi-object
	apc.ActPromote: {
		DisplayName: "promote-files",
		Scope:       ScopeB,
		Access:      apc.AcePromote,
		Startable:   false,
		RefreshCap:  true,
	},
	apc.ActEvictObjects: {
		DisplayName: "evict-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
	},
	apc.ActEvictRemoteBck: {
		DisplayName: "evict-remote-bucket",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
	},
	apc.ActDeleteObjects: {
		DisplayName: "delete-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
	},
	apc.ActPrefetchObjects: {
		DisplayName: "prefetch-objects",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		RefreshCap:  true,
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
	},
	apc.ActMakeNCopies: {
		DisplayName: "mirror",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		Metasync:    true,
		RefreshCap:  true,
	},
	apc.ActMoveBck: {
		DisplayName:    "rename-bucket",
		Scope:          ScopeB,
		Access:         apc.AceMoveBucket,
		Startable:      false, // executing this one cannot be done via `api.StartXaction`
		Metasync:       true,
		Rebalance:      true,
		ConflictRebRes: true,
	},
	apc.ActCopyBck: {
		DisplayName:    "copy-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // apc.AceCreateBucket ditto
		Startable:      false,        // ditto
		Metasync:       true,
		RefreshCap:     true,
		ConflictRebRes: true,
	},
	apc.ActETLBck: {
		DisplayName: "etl-bucket",
		Scope:       ScopeB,
		Access:      apc.AccessRW, // ditto
		Startable:   false,        // ditto
		Metasync:    true,
		RefreshCap:  true,
		AbortRebRes: true,
	},

	apc.ActList: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Idles: true, QuietBrief: true},

	apc.ActGetBatch: {Scope: ScopeGB, Startable: false, Metasync: false, ConflictRebRes: true, Idles: true}, // apc.Moss

	// cache management, internal usage
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
