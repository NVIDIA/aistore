// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"github.com/NVIDIA/aistore/api/apc"
)

const (
	ScopeG   = "global"
	ScopeT   = "target"
	ScopeBck = "bucket"
	ScopeO   = "other"
)

type (
	Descriptor struct {
		Scope      string          // ScopeG (global), etc. - the enum above
		Access     apc.AccessAttrs // Access required by xctn (see: apc.Access*)
		Startable  bool            // determines if this xaction can be started via API
		Metasync   bool            // true: changes and metasyncs cluster-wide meta
		Owned      bool            // true: JTX-owned
		RefreshCap bool            // true: refresh capacity stats upon completion
		Mountpath  bool            // true: mountpath-traversing (jogger-based) xaction
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
	apc.ActLRU:          {Scope: ScopeG, Startable: true, Mountpath: true},
	apc.ActStoreCleanup: {Scope: ScopeG, Startable: true, Mountpath: true},
	apc.ActElection:     {Scope: ScopeG, Startable: false},
	apc.ActResilver:     {Scope: ScopeT, Startable: true, Mountpath: true, Resilver: true},
	apc.ActRebalance:    {Scope: ScopeG, Startable: true, Metasync: true, Owned: false, Mountpath: true, Rebalance: true},
	apc.ActDownload:     {Scope: ScopeG, Startable: false, Mountpath: true},
	apc.ActETLInline:    {Scope: ScopeG, Startable: false, Mountpath: false},

	// xactions that run on a given bucket or buckets
	apc.ActECGet:           {Scope: ScopeBck, Startable: false},
	apc.ActECPut:           {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	apc.ActECRespond:       {Scope: ScopeBck, Startable: false},
	apc.ActMakeNCopies:     {Scope: ScopeBck, Access: apc.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	apc.ActPutCopies:       {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	apc.ActArchive:         {Scope: ScopeBck, Startable: false, RefreshCap: true},
	apc.ActCopyObjects:     {Scope: ScopeBck, Startable: false, RefreshCap: true},
	apc.ActETLObjects:      {Scope: ScopeBck, Startable: false, RefreshCap: true},
	apc.ActMoveBck:         {Scope: ScopeBck, Access: apc.AceMoveBucket, Startable: false, Metasync: true, Owned: false, Mountpath: true, Rebalance: true, MassiveBck: true},
	apc.ActCopyBck:         {Scope: ScopeBck, Access: apc.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true, MassiveBck: true},
	apc.ActETLBck:          {Scope: ScopeBck, Access: apc.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true, MassiveBck: true},
	apc.ActECEncode:        {Scope: ScopeBck, Access: apc.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true, MassiveBck: true},
	apc.ActEvictObjects:    {Scope: ScopeBck, Access: apc.AceObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	apc.ActDeleteObjects:   {Scope: ScopeBck, Access: apc.AceObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	apc.ActLoadLomCache:    {Scope: ScopeBck, Startable: true, Mountpath: true},
	apc.ActPrefetchObjects: {Scope: ScopeBck, Access: apc.AccessRW, RefreshCap: true, Startable: true},
	apc.ActPromote:         {Scope: ScopeBck, Access: apc.AcePromote, Startable: false, RefreshCap: true},
	apc.ActList:            {Scope: ScopeBck, Access: apc.AceObjLIST, Startable: false, Metasync: false, Owned: true},
	apc.ActInvalListCache:  {Scope: ScopeBck, Access: apc.AceObjLIST, Startable: false},

	// other
	apc.ActSummaryBck: {Scope: ScopeO, Access: apc.AceObjLIST | apc.AceBckHEAD, Startable: false, Metasync: false, Owned: true, Mountpath: true},
}

func IsValidKind(kind string) bool { _, ok := Table[kind]; return ok }
func IsBckScope(kind string) bool  { return Table[kind].Scope == ScopeBck }
func IsMountpath(kind string) bool { return Table[kind].Mountpath }
