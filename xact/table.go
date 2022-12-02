// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
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
		Access      apc.AccessAttrs // Access required by xctn (see: apc.Access*)
		Scope       int             // ScopeG (global), etc. - the enum above
		Startable   bool            // determines if this xaction can be started via API
		Metasync    bool            // true: changes and metasyncs cluster-wide meta
		Owned       bool            // (for definition, see ais/ic.go)
		RefreshCap  bool            // refresh capacity stats upon completion
		Mountpath   bool            // is a mountpath-traversing ("jogger") xaction

		// see xreg for "limited coexistence"
		Rebalance  bool // moves data between nodes
		Resilver   bool // moves data between mountpaths
		MassiveBck bool // massive data copying (transforming, encoding) operation on a bucket
	}
)

// Table is a static Kind=>[Xaction Descriptor] map that contains
// static properties of a given xaction type (aka `kind`), such as:
// `Startable`, `Owned`, etc.
var Table = map[string]Descriptor{
	// bucket-less xactions that will typically have a 'cluster' scope (with resilver being a notable exception)
	apc.ActElection:  {DisplayName: "elect-primary", Scope: ScopeG, Startable: false},
	apc.ActRebalance: {Scope: ScopeG, Startable: true, Metasync: true, Owned: false, Mountpath: true, Rebalance: true},
	apc.ActDownload:  {Scope: ScopeG, Startable: false, Mountpath: true},
	apc.ActETLInline: {Scope: ScopeG, Startable: false, Mountpath: false},

	// (one bucket) | (all buckets)
	apc.ActLRU:          {DisplayName: "lru-eviction", Scope: ScopeGB, Startable: true, Mountpath: true},
	apc.ActStoreCleanup: {DisplayName: "cleanup", Scope: ScopeGB, Startable: true, Mountpath: true},
	apc.ActSummaryBck: {
		DisplayName: "summary",
		Scope:       ScopeGB,
		Access:      apc.AceObjLIST | apc.AceBckHEAD,
		Startable:   false,
		Metasync:    false,
		Owned:       true,
		Mountpath:   true,
	},

	// one target
	apc.ActResilver: {Scope: ScopeT, Startable: true, Mountpath: true, Resilver: true},

	// xactions that run on (or in) a given bucket
	apc.ActECGet:     {Scope: ScopeB, Startable: false},
	apc.ActECPut:     {Scope: ScopeB, Startable: false, Mountpath: true, RefreshCap: true},
	apc.ActECRespond: {Scope: ScopeB, Startable: false},
	apc.ActMakeNCopies: {
		DisplayName: "mirror",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
	},
	apc.ActPutCopies:   {Scope: ScopeB, Startable: false, Mountpath: true, RefreshCap: true},
	apc.ActArchive:     {Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActCopyObjects: {DisplayName: "copy-objects", Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActETLObjects:  {DisplayName: "etl-objects", Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActMoveBck: {
		DisplayName: "rename-bucket",
		Scope:       ScopeB,
		Access:      apc.AceMoveBucket,
		Startable:   false,
		Metasync:    true,
		Owned:       false,
		Mountpath:   true,
		Rebalance:   true,
		MassiveBck:  true,
	},
	apc.ActCopyBck: {
		DisplayName: "copy-bucket",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   false,
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
		MassiveBck:  true,
	},
	apc.ActETLBck: {
		DisplayName: "etl-bucket",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   false,
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
		MassiveBck:  true,
	},
	apc.ActECEncode: {
		DisplayName: "ec-bucket",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
		MassiveBck:  true,
	},
	apc.ActEvictObjects: {
		DisplayName: "evict-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
		Mountpath:   true,
	},
	apc.ActDeleteObjects: {
		DisplayName: "delete-objects",
		Scope:       ScopeB,
		Access:      apc.AceObjDELETE,
		Startable:   false,
		RefreshCap:  true,
		Mountpath:   true,
	},
	apc.ActPrefetchObjects: {DisplayName: "prefetch-objects", Scope: ScopeB, Access: apc.AccessRW, RefreshCap: true, Startable: true},
	apc.ActPromote:         {DisplayName: "promote-files", Scope: ScopeB, Access: apc.AcePromote, Startable: false, RefreshCap: true},
	apc.ActList:            {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Owned: true},

	// cache management, internal usage
	apc.ActLoadLomCache:   {DisplayName: "warm-up-metadata", Scope: ScopeB, Startable: true, Mountpath: true},
	apc.ActInvalListCache: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false},
}

func IsMountpath(kind string) bool { return Table[kind].Mountpath }

func IsValidKind(kind string) bool {
	_, ok := Table[kind]
	return ok
}

func GetDescriptor(kindOrName string) (string, Descriptor, error) {
	kind, dtor := getDtor(kindOrName)
	if dtor == nil {
		return "", Descriptor{}, fmt.Errorf("not found xaction %q", kindOrName)
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

func ListDisplayNames(onlyStartable bool) (names []string) {
	names = make([]string, 0, len(Table))
	for kind, dtor := range Table {
		if onlyStartable && !dtor.Startable {
			continue
		}
		if dtor.DisplayName != "" {
			names = append(names, dtor.DisplayName)
		} else {
			names = append(names, kind)
		}
	}
	sort.Strings(names)
	return
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
