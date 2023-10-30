// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const UUIDSepa = ","

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

// global waiting tunables
// (used in: `api.WaitForXactionIC` and `api.WaitForXactionNode`)
const (
	DefWaitTimeShort = time.Minute        // zero `ArgsMsg.Timeout` defaults to
	DefWaitTimeLong  = 7 * 24 * time.Hour // when `ArgsMsg.Timeout` is negative
	MaxProbingFreq   = 30 * time.Second   // as the name implies
	MinPollTime      = 2 * time.Second    // ditto
	MaxPollTime      = 2 * time.Minute    // can grow up to

	NumConsecutiveIdle = 3 // number of consecutive 'idle' states (to exclude false-positive "is idle")
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

	// primarily: `api.QueryXactionSnaps`
	MultiSnap map[string][]*cluster.Snap // by target ID (tid)
)

type (
	Descriptor struct {
		DisplayName string          // as implied
		Access      apc.AccessAttrs // access permissions (see: apc.Access*)
		Scope       int             // ScopeG (global), etc. - the enum above
		Startable   bool            // true if user can start this xaction (e.g., via `api.StartXaction`)
		Metasync    bool            // true if this xaction changes (and metasyncs) cluster metadata
		Owned       bool            // (for definition, see ais/ic.go)
		RefreshCap  bool            // refresh capacity stats upon completion
		Mountpath   bool            // is a mountpath-traversing ("jogger") xaction

		// see xreg for "limited coexistence"
		Rebalance      bool // moves data between nodes
		Resilver       bool // moves data between mountpaths
		ConflictRebRes bool // conflicts with rebalance/resilver
		AbortRebRes    bool // gets aborted upon rebalance/resilver - currently, all `ext`-ensions

		// xaction has an intermediate `idle` state whereby it "idles" between requests
		// (see related: xact/demand.go)
		Idles bool

		// xaction returns extended xaction-specific stats
		// (see related: `Snap.Ext` in cluster/xaction.go)
		ExtendedStats bool
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
	apc.ActRebalance: {Scope: ScopeG, Startable: true, Metasync: true, Owned: false, Mountpath: true, Rebalance: true},

	apc.ActETLInline: {Scope: ScopeG, Startable: false, Mountpath: false, AbortRebRes: true},

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

	// single target (node)
	apc.ActResilver: {Scope: ScopeT, Startable: true, Mountpath: true, Resilver: true},

	// on-demand EC and n-way replication
	// (non-startable, triggered by PUT => erasure-coded or mirrored bucket)
	apc.ActECGet:     {Scope: ScopeB, Startable: false, Idles: true, ExtendedStats: true},
	apc.ActECPut:     {Scope: ScopeB, Startable: false, Mountpath: true, RefreshCap: true, Idles: true, ExtendedStats: true},
	apc.ActECRespond: {Scope: ScopeB, Startable: false, Idles: true},
	apc.ActPutCopies: {Scope: ScopeB, Startable: false, Mountpath: true, RefreshCap: true, Idles: true},

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

	apc.ActDownload: {Access: apc.AccessRW, Scope: ScopeG, Startable: false, Mountpath: true, Idles: true, AbortRebRes: true},

	// in its own class
	apc.ActDsort: {
		DisplayName:    "dsort",
		Scope:          ScopeB,
		Access:         apc.AccessRW,
		Startable:      false,
		RefreshCap:     true,
		Mountpath:      true,
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
		DisplayName:    "ec-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW,
		Startable:      true,
		Metasync:       true,
		Owned:          false,
		RefreshCap:     true,
		Mountpath:      true,
		ConflictRebRes: true,
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
		DisplayName:    "rename-bucket",
		Scope:          ScopeB,
		Access:         apc.AceMoveBucket,
		Startable:      false, // executing this one cannot be done via `api.StartXaction`
		Metasync:       true,
		Owned:          false,
		Mountpath:      true,
		Rebalance:      true,
		ConflictRebRes: true,
	},
	apc.ActCopyBck: {
		DisplayName:    "copy-bucket",
		Scope:          ScopeB,
		Access:         apc.AccessRW, // apc.AceCreateBucket ditto
		Startable:      false,        // ditto
		Metasync:       true,
		Owned:          false,
		RefreshCap:     true,
		Mountpath:      true,
		ConflictRebRes: true,
	},
	apc.ActETLBck: {
		DisplayName: "etl-bucket",
		Scope:       ScopeB,
		Access:      apc.AccessRW, // ditto
		Startable:   false,        // ditto
		Metasync:    true,
		Owned:       false,
		RefreshCap:  true,
		Mountpath:   true,
		AbortRebRes: true,
	},

	apc.ActList: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Owned: true, Idles: true},

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

///////////////
// MultiSnap //
///////////////

// NOTE: when xaction UUID is not specified: require the same kind _and_
// a single running uuid (otherwise, IsAborted() et al. can only produce ambiguous results)
func (xs MultiSnap) checkEmptyID(xid string) error {
	var kind, uuid string
	if xid != "" {
		debug.Assert(IsValidUUID(xid), xid)
		return nil
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if kind == "" {
				kind = xsnap.Kind
			} else if kind != xsnap.Kind {
				return fmt.Errorf("invalid multi-snap Kind: %q vs %q", kind, xsnap.Kind)
			}
			if xsnap.Running() {
				if uuid == "" {
					uuid = xsnap.ID
				} else if uuid != xsnap.ID {
					return fmt.Errorf("invalid multi-snap UUID: %q vs %q", uuid, xsnap.ID)
				}
			}
		}
	}
	return nil
}

func (xs MultiSnap) GetUUIDs() []string {
	uuids := make(cos.StrSet, 2)
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			uuids[xsnap.ID] = struct{}{}
		}
	}
	return uuids.ToSlice()
}

func (xs MultiSnap) RunningTarget(xid string) (string /*tid*/, *cluster.Snap, error) {
	if err := xs.checkEmptyID(xid); err != nil {
		return "", nil, err
	}
	for tid, snaps := range xs {
		for _, xsnap := range snaps {
			if (xid == xsnap.ID || xid == "") && xsnap.Running() {
				return tid, xsnap, nil
			}
		}
	}
	return "", nil, nil
}

func (xs MultiSnap) IsAborted(xid string) (bool, error) {
	if err := xs.checkEmptyID(xid); err != nil {
		return false, err
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if (xid == xsnap.ID || xid == "") && xsnap.IsAborted() {
				return true, nil
			}
		}
	}
	return false, nil
}

// (all targets, all xactions)
func (xs MultiSnap) IsIdle(xid string) (found, idle bool) {
	if xid != "" {
		debug.Assert(IsValidUUID(xid), xid)
		return xs._idle(xid)
	}
	uuids := xs.GetUUIDs()
	idle = true
	for _, xid = range uuids {
		f, i := xs._idle(xid)
		found = found || f
		idle = idle && i
	}
	return
}

// (all targets, given xaction)
func (xs MultiSnap) _idle(xid string) (found, idle bool) {
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xid == xsnap.ID {
				found = true
				// (one target, one xaction)
				if xsnap.Started() && !xsnap.IsAborted() && !xsnap.IsIdle() {
					return true, false
				}
			}
		}
	}
	idle = true // (read: not-idle not found)
	return
}

func (xs MultiSnap) ObjCounts(xid string) (locObjs, outObjs, inObjs int64) {
	if xid == "" {
		uuids := xs.GetUUIDs()
		debug.Assert(len(uuids) == 1, uuids)
		xid = uuids[0]
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xid == xsnap.ID {
				locObjs += xsnap.Stats.Objs
				outObjs += xsnap.Stats.OutObjs
				inObjs += xsnap.Stats.InObjs
			}
		}
	}
	return
}

func (xs MultiSnap) ByteCounts(xid string) (locBytes, outBytes, inBytes int64) {
	if xid == "" {
		uuids := xs.GetUUIDs()
		debug.Assert(len(uuids) == 1, uuids)
		xid = uuids[0]
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xid == xsnap.ID {
				locBytes += xsnap.Stats.Bytes
				outBytes += xsnap.Stats.OutBytes
				inBytes += xsnap.Stats.InBytes
			}
		}
	}
	return
}

func (xs MultiSnap) TotalRunningTime(xid string) (time.Duration, error) {
	debug.Assert(IsValidUUID(xid), xid)
	var (
		start, end     time.Time
		found, running bool
	)
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xid == xsnap.ID {
				found = true
				running = running || xsnap.Running()
				if !xsnap.StartTime.IsZero() {
					if start.IsZero() || xsnap.StartTime.Before(start) {
						start = xsnap.StartTime
					}
				}
				if !xsnap.EndTime.IsZero() && xsnap.EndTime.After(end) {
					end = xsnap.EndTime
				}
			}
		}
	}
	if !found {
		return 0, errors.New("xaction [" + xid + "] not found")
	}
	if running {
		end = time.Now()
	}
	return end.Sub(start), nil
}
