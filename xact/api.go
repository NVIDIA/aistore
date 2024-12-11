// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
)

const (
	ScopeG  = iota + 1 // cluster
	ScopeB             // bucket
	ScopeGB            // (one bucket) | (all buckets)
	ScopeT             // target
)

const (
	SepaID = ","

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

	// number of consecutive 'idle' xaction states, with possible numeric
	// values translating as follows:
	// 1: fully rely on xact.IsIdle() logic with no extra checks whatsoever
	// 2: one additional IsIdle() call after MinPollTime
	// 3: two additional IsIdle() calls spaced at MinPollTime interval, and so on.
	NumConsecutiveIdle = 2
)

// ArgsMsg.Flags
const (
	XrmZeroSize = 1 << iota // usage: x-cleanup (apc.ActStoreCleanup) to remove zero size objects
)

type (
	// either xaction ID or Kind must be specified
	// is getting passed via ActMsg.Value w/ MorphMarshal extraction
	ArgsMsg struct {
		ID          string        // xaction UUID
		Kind        string        // xaction kind _or_ name (see `xact.Table`)
		DaemonID    string        // node that runs this xaction
		Bck         cmn.Bck       // bucket
		Buckets     []cmn.Bck     // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		Timeout     time.Duration // max time to wait
		Flags       uint32        `json:"flags,omitempty"` // enum (XrmZeroSize, ...) bitwise
		Force       bool          // force
		OnlyRunning bool          // only for running xactions
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
	MultiSnap map[string][]*core.Snap // by target ID (tid)
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

	apc.ActList: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false, Metasync: false, Idles: true},

	// cache management, internal usage
	apc.ActLoadLomCache:   {DisplayName: "warm-up-metadata", Scope: ScopeB, Startable: true},
	apc.ActInvalListCache: {Scope: ScopeB, Access: apc.AceObjLIST, Startable: false},
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
					simKind, simName = kind, dtor.DisplayName
					break
				}
			}
		}
	}
	return
}

func Cname(kind, uuid string) string { return kind + LeftID + uuid + RightID }

func ParseCname(cname string) (xactKind, xactID string, _ error) {
	const efmt = "invalid name %q"
	l := len(cname)
	if l == 0 || cname[l-1] != RightID[0] {
		return "", "", fmt.Errorf(efmt, cname)
	}
	i := strings.IndexByte(cname, LeftID[0])
	if i < 0 {
		return "", "", fmt.Errorf(efmt, cname)
	}
	xactKind, xactID = cname[:i], cname[i+1:l-1]
	return xactKind, xactID, nil
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

//
// validators (helpers)
//

func IsValidKind(kind string) bool {
	_, ok := Table[kind]
	return ok
}

func CheckValidKind(kind string) (err error) {
	if _, ok := Table[kind]; !ok {
		err = fmt.Errorf("invalid xaction (job) kind %q", kind)
	}
	return err
}

func IsValidUUID(id string) bool { return cos.IsValidUUID(id) || IsValidRebID(id) }

func CheckValidUUID(id string) (err error) {
	if !cos.IsValidUUID(id) && !IsValidRebID(id) {
		err = fmt.Errorf("invalid xaction (job) UUID %q", id)
	}
	return err
}

/////////////
// ArgsMsg //
/////////////

func (args *ArgsMsg) String() string {
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString("xa-")
	sb.WriteString(args.Kind)
	sb.WriteByte('[')
	if args.ID != "" {
		sb.WriteString(args.ID)
	}
	sb.WriteByte(']')
	if !args.Bck.IsEmpty() {
		sb.WriteByte('-')
		sb.WriteString(args.Bck.String())
	}
	if args.Timeout > 0 {
		sb.WriteByte('-')
		sb.WriteString(args.Timeout.String())
	}
	if args.DaemonID != "" {
		sb.WriteString("-node[")
		sb.WriteString(args.DaemonID)
		sb.WriteByte(']')
	}
	if args.Flags > 0 {
		sb.WriteString("-0x")
		sb.WriteString(strconv.FormatUint(uint64(args.Flags), 16))
	}
	return sb.String()
}

//////////////
// QueryMsg //
//////////////

func (msg *QueryMsg) String() (s string) {
	if msg.ID == "" {
		s = "x-" + msg.Kind
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

func (xs MultiSnap) RunningTarget(xid string) (string /*tid*/, *core.Snap, error) {
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

// (all targets, all xactions)
func (xs MultiSnap) IsIdle(xid string) (aborted, running, notstarted bool) {
	if xid != "" {
		debug.Assert(IsValidUUID(xid), xid)
		return xs._get(xid)
	}
	uuids := xs.GetUUIDs()
	for _, xid = range uuids {
		a, r, ns := xs._get(xid)
		aborted = aborted || a
		notstarted = notstarted || ns
		running = running || r
	}
	return aborted, running, notstarted
}

// (all targets, given xaction)
func (xs MultiSnap) _get(xid string) (aborted, running, notstarted bool) {
	var nt, nr, ns, nf int
	for _, snaps := range xs {
		nt++
		for _, xsnap := range snaps {
			if xid != xsnap.ID {
				continue
			}
			nf++
			// (one target, one xaction)
			switch {
			case xsnap.IsAborted():
				return true, false, false
			case !xsnap.Started():
				ns++
			case !xsnap.IsIdle():
				nr++
			}
			break
		}
	}
	running = nr > 0
	notstarted = ns > 0 || nf == 0
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
		return 0, fmt.Errorf("xaction (job) UUID=%q not found", xid)
	}
	if running {
		end = time.Now()
	}
	return end.Sub(start), nil
}
