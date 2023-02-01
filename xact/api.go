// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	ScopeG  = iota + 1 // cluster
	ScopeB             // bucket
	ScopeGB            // (one bucket) | (all buckets)
	ScopeT             // target
)

const (
	LeftID  = "["
	RightID = "]"
)

type (
	// either xaction ID or Kind must be specified
	// is getting passed via ActMsg.Value w/ MorphMarshal extraction
	ArgsMsg struct {
		ID   string // xaction UUID
		Kind string // xaction kind _or_ name (see `xact.Table`)

		// optional parameters to further narrow down or filter-out xactions in question
		DaemonID    string        // node that runs this xaction
		Bck         cmn.Bck       // bucket
		Buckets     []cmn.Bck     // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		Timeout     time.Duration // max time to wait and other "non-filters"
		Force       bool          // force
		OnlyRunning bool          // look only for running xactions
	}

	// simplified JSON-tagged version of the above
	QueryMsg struct {
		OnlyRunning *bool     `json:"show_active"`
		Bck         cmn.Bck   `json:"bck"`
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		DaemonID    string    `json:"node,omitempty"`
		Buckets     []cmn.Bck `json:"buckets,omitempty"`
	}

	// further simplified, non-JSON, internal AIS use
	Flt struct {
		Bck         *cluster.Bck
		OnlyRunning *bool
		ID          string
		Kind        string
	}
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

// `Table` is a static, public, and global Kind=>[Xaction Descriptor] map that contains
// xaction kinds and static properties, such as `Startable`, `Owned`, etc.
// In particular, "startability" is narrowly defined as ability to start xaction
// via `api.StartXaction`
// (whereby copying bucket, for instance, requires a separate `api.CopyBucket`, etc.)
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
	apc.ActPutCopies: {Scope: ScopeB, Startable: false, Mountpath: true, RefreshCap: true},

	// multi-object
	apc.ActArchive:     {Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActCopyObjects: {DisplayName: "copy-objects", Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActETLObjects:  {DisplayName: "etl-objects", Scope: ScopeB, Startable: false, RefreshCap: true},
	apc.ActPromote:     {DisplayName: "promote-files", Scope: ScopeB, Access: apc.AcePromote, Startable: false, RefreshCap: true},
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
	apc.ActPrefetchObjects: {
		DisplayName: "prefetch-objects",
		Scope:       ScopeB,
		Access:      apc.AccessRW,
		Startable:   true,
		RefreshCap:  true,
	},

	// entire bucket (storage svcs)
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
	apc.ActMoveBck: {
		DisplayName: "rename-bucket",
		Scope:       ScopeB,
		Access:      apc.AceMoveBucket,
		Startable:   false, // executing this one cannot be done via `api.StartXaction`
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
		Startable:   false, // ditto
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
		Startable:   false, // ditto
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
		MassiveBck:  true,
	},

	apc.ActList: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Owned: true},

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

/////////////
// ArgsMsg //
/////////////

func (args *ArgsMsg) String() (s string) {
	if args.ID == "" {
		s = fmt.Sprintf("x-%s", args.Kind)
	} else {
		s = fmt.Sprintf("x-%s[%s]", args.Kind, args.ID)
	}
	if !args.Bck.IsEmpty() {
		s += "-" + args.Bck.String()
	}
	if args.Timeout > 0 {
		s += "-" + args.Timeout.String()
	}
	if args.DaemonID != "" {
		s += "-node[" + args.DaemonID + "]"
	}
	return
}

//////////////
// QueryMsg //
//////////////

func (msg *QueryMsg) String() (s string) {
	if msg.ID == "" {
		s = fmt.Sprintf("x-%s", msg.Kind)
	} else {
		s = fmt.Sprintf("x-%s[%s]", msg.Kind, msg.ID)
	}
	if !msg.Bck.IsEmpty() {
		s += "-" + msg.Bck.String()
	}
	if msg.DaemonID != "" {
		s += "-node[" + msg.DaemonID + "]"
	}
	if msg.OnlyRunning != nil && *msg.OnlyRunning {
		s += "-only-running"
	}
	return
}

/////////
// Flt //
/////////

func (flt *Flt) String() string {
	msg := QueryMsg{OnlyRunning: flt.OnlyRunning, Bck: flt.Bck.Clone(), ID: flt.ID, Kind: flt.Kind}
	return msg.String()
}

func (flt Flt) Matches(xctn cluster.Xact) (yes bool) {
	debug.Assert(IsValidKind(xctn.Kind()), xctn.String())
	// running?
	if flt.OnlyRunning != nil {
		if *flt.OnlyRunning != xctn.Running() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		debug.Assert(cos.IsValidUUID(flt.ID) || IsValidRebID(flt.ID), flt.ID)
		if yes = xctn.ID() == flt.ID; yes {
			debug.Assert(xctn.Kind() == flt.Kind, xctn.String()+" vs same ID "+flt.String())
		}
		return
	}
	// kind?
	if flt.Kind != "" {
		debug.Assert(IsValidKind(flt.Kind), flt.Kind)
		if xctn.Kind() != flt.Kind {
			return false
		}
	}
	// bucket?
	if Table[xctn.Kind()].Scope != ScopeB {
		return true // non single-bucket x
	}
	if flt.Bck == nil {
		return true // the filter's not filtering out
	}

	if xctn.Bck() == nil {
		return false // ambiguity (cannot really compare)
	}
	return xctn.Bck().Equal(flt.Bck, true, true)
}
