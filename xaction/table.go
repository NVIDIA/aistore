// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import "github.com/NVIDIA/aistore/cmn"

const (
	ScopeG   = "global"
	ScopeT   = "target"
	ScopeBck = "bucket"
	ScopeO   = "other"
)

type (
	Record struct {
		Scope      string          // ScopeG (global), etc. - the enum above
		Access     cmn.AccessAttrs // Access required by xact (see: cmn.Access*)
		Startable  bool            // determines if this xaction can be started via API
		Metasync   bool            // true: changes and metasyncs cluster-wide meta
		Owned      bool            // true: JTX-owned
		RefreshCap bool            // true: refresh capacity stats upon completion
		Mountpath  bool            // true: mountpath-traversing (jogger-based) xaction
	}
)

// Table is a static Kind=>[Xaction Record] map that contains
// static properties of a given xaction type (aka `kind`), such as:
// `Startable`, `Owned`, etc.
var Table = map[string]Record{
	// bucket-less xactions that will typically have a 'cluster' scope (with resilver being a notable exception)
	cmn.ActLRU:          {Scope: ScopeG, Startable: true, Mountpath: true},
	cmn.ActStoreCleanup: {Scope: ScopeG, Startable: true, Mountpath: true},
	cmn.ActElection:     {Scope: ScopeG, Startable: false},
	cmn.ActResilver:     {Scope: ScopeT, Startable: true, Mountpath: true},
	cmn.ActRebalance:    {Scope: ScopeG, Startable: true, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActDownload:     {Scope: ScopeG, Startable: false, Mountpath: true},

	// xactions that run on a given bucket or buckets
	cmn.ActECGet:           {Scope: ScopeBck, Startable: false},
	cmn.ActECPut:           {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	cmn.ActECRespond:       {Scope: ScopeBck, Startable: false},
	cmn.ActMakeNCopies:     {Scope: ScopeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActPutCopies:       {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	cmn.ActArchive:         {Scope: ScopeBck, Startable: false, RefreshCap: true},
	cmn.ActCopyObjects:     {Scope: ScopeBck, Startable: false, RefreshCap: true},
	cmn.ActETLObjects:      {Scope: ScopeBck, Startable: false, RefreshCap: true},
	cmn.ActMoveBck:         {Scope: ScopeBck, Access: cmn.AccessMoveBucket, Startable: false, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActCopyBck:         {Scope: ScopeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActETLBck:          {Scope: ScopeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActECEncode:        {Scope: ScopeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActEvictObjects:    {Scope: ScopeBck, Access: cmn.AccessObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	cmn.ActDeleteObjects:   {Scope: ScopeBck, Access: cmn.AccessObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	cmn.ActLoadLomCache:    {Scope: ScopeBck, Startable: true, Mountpath: true},
	cmn.ActPrefetchObjects: {Scope: ScopeBck, Access: cmn.AccessRW, RefreshCap: true, Startable: true},
	cmn.ActPromote:         {Scope: ScopeBck, Access: cmn.AccessPROMOTE, Startable: false, RefreshCap: true},
	cmn.ActQueryObjects:    {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActList:            {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActInvalListCache:  {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false},

	// other
	cmn.ActSummary: {Scope: ScopeO, Access: cmn.AccessObjLIST | cmn.AccessBckHEAD, Startable: false, Metasync: false, Owned: true, Mountpath: true},
}

func IsValidKind(kind string) bool { _, ok := Table[kind]; return ok }
func IsBckScope(kind string) bool  { return Table[kind].Scope == ScopeBck }
func IsMountpath(kind string) bool { return Table[kind].Mountpath }
