// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"

	jsoniter "github.com/json-iterator/go"
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
	numConsecutiveIdle = 2

	// stability window for kind-only wait-for-finished
	// (see snapsFinished below)
	numConsecutiveEmpty = 3
)

// ArgsMsg.Flags
// note: for simplicity, keeping all custom x-flags in one place and one global enum for now
const (
	FlagZeroSize = 1 << iota // usage: x-cleanup (apc.ActStoreCleanup) to remove zero size objects
	FlagLatestVer
	FlagSync
	FlagKeepMisplaced // usage: x-cleanup to _not_ remove (ie, keep) misplaced objects
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
		Flags       uint32        `json:"flags,omitempty"` // enum (FlagZeroSize, ...) bitwise
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
					simKind, simName = kind, cos.Left(dtor.DisplayName, kind)
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

//////////////
// QueryMsg (internal usage)
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

//
// SnapsCond: condition functions for snaps-based polling (api.WaitForSnaps)
//

// SnapsCond is the condition function signature for snaps-based polling.
// Returns:
//   - done: whether to stop waiting
//   - reset: whether to reset polling sleep back to MinPollTime
//   - err: error to propagate (stops polling immediately)
type SnapsCond func(MultiSnap) (done, reset bool, err error)

// --- Finished condition ---
//
// Notice the distinction between Finished and NotRunning (below)
//
// - args.ID != "": wait for that UUID to become visible and reach a terminal state
//   (finished OR aborted).
// - kind-only wait (args.ID == ""): require seeing at least one matching xaction first,
//   then complete when no matching *running* xactions are visible anymore (stable empty snaps).
//   This relies on args.OnlyRunning=true (set below) to avoid stale completed jobs.

type snapsFinished struct {
	id    string
	seen  bool
	empty int
}

func (c *snapsFinished) check(snaps MultiSnap) (bool, bool, error) {
	if c.id != "" {
		// specific UUID: wait until observed and terminal (finished OR aborted)
		for _, tsnaps := range snaps {
			for _, snap := range tsnaps {
				if snap.ID == c.id {
					return snap.IsFinished() || snap.IsAborted(), false, nil
				}
			}
		}
		// keep polling
		return false, false, nil
	}

	// kind-only: require seeing at least one running xaction, then stable "emptiness"
	if !snaps.HasUUIDs() {
		if !c.seen {
			return false, false, nil
		}
		c.empty++
		return c.empty >= numConsecutiveEmpty, true, nil // "reset" => probe sooner after progress
	}

	c.seen = true
	c.empty = 0
	return false, false, nil
}

func (args *ArgsMsg) Finished() SnapsCond {
	if args.ID == "" {
		args.OnlyRunning = true
	}
	return (&snapsFinished{id: args.ID}).check
}

// --- NotRunning condition ---
// Succeed immediately if nothing is running.
// Unlike Finished, this has no "seen" tracking - use for pre-condition checks
// ("make sure nothing's running before I start") rather than post-action waits
// ("started and completed").

type snapsNotRunning struct {
	id string
}

func (c *snapsNotRunning) check(snaps MultiSnap) (bool, bool, error) {
	_, running, _ := snaps.AggregateState(c.id)
	return !running, false, nil
}

func (args *ArgsMsg) NotRunning() SnapsCond {
	return (&snapsNotRunning{id: args.ID}).check
}

// --- Started condition ---
// Wait until the xaction is visible/running.
// NOTE: waiting for a job to start, especially cluster-wide, is inherently racy -
// it depends on per-target workload, server hardware, network, etc. Use with caution!

type snapsStarted struct {
	id string
}

func (c *snapsStarted) check(snaps MultiSnap) (bool, bool, error) {
	if c.id != "" {
		_, _, notstarted := snaps.AggregateState(c.id)
		return !notstarted, true, nil
	}
	// kind-only: wait until something is running
	_, snap, err := snaps.RunningTarget("")
	if err != nil {
		return true, false, err // ambiguity: multiple running UUIDs
	}
	return snap != nil, true, nil
}

func (args *ArgsMsg) Started() SnapsCond {
	return (&snapsStarted{id: args.ID}).check
}

// --- Idle condition ---
// Wait until the xaction becomes idle for numConsecutiveIdle consecutive polls.
// Use for xactions that "idle before finishing"
// (distributed batch jobs that may have gaps between work items).
// NOTE: an idle xaction is still running - it just has no work to do at the moment.
// Sets OnlyRunning=true automatically.

type snapsIdle struct {
	id      string
	cnt     int
	delayed bool
}

func (c *snapsIdle) check(snaps MultiSnap) (bool, bool, error) {
	aborted, running, notstarted := snaps.AggregateState(c.id)
	if aborted {
		return true, false, nil
	}
	if running {
		c.cnt = 0
		return false, false, nil
	}
	if notstarted && c.cnt == 0 {
		// preserve legacy behavior: avoid mistaking "not yet visible" for "idle"
		if !c.delayed {
			time.Sleep(min(2*MinPollTime, 4*time.Second))
			c.delayed = true
		}
		return false, false, nil
	}
	// idle
	c.cnt++
	return c.cnt >= numConsecutiveIdle, true, nil
}

func (args *ArgsMsg) Idle() SnapsCond {
	args.OnlyRunning = true
	return (&snapsIdle{id: args.ID}).check
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
			if xsnap.IsRunning() {
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

func (xs MultiSnap) HasUUIDs() bool {
	for _, snaps := range xs {
		if len(snaps) > 0 {
			return true
		}
	}
	return false
}

func (xs MultiSnap) singleUUID() (xid string, ok bool) {
	for _, snaps := range xs {
		for _, s := range snaps {
			if xid == "" {
				xid = s.ID
				continue
			}
			if s.ID != xid {
				return "", false
			}
		}
	}
	return xid, xid != ""
}

func (xs MultiSnap) RunningTarget(xid string) (string /*tid*/, *core.Snap, error) {
	if err := xs.checkEmptyID(xid); err != nil {
		return "", nil, err
	}
	for tid, snaps := range xs {
		for _, xsnap := range snaps {
			if (xid == xsnap.ID || xid == "") && xsnap.IsRunning() {
				return tid, xsnap, nil
			}
		}
	}
	return "", nil, nil
}

// return:
// `aborted`    => any selected xaction aborted on any target
// `running`    => any selected xaction currently running on any target
// `notstarted` => selected xaction not yet visible / not started on any target
// selection:
// - xid != "": the specified xaction UUID (all targets)
// - xid == "": all UUIDs present in this MultiSnap (all targets)
func (xs MultiSnap) AggregateState(xid string) (aborted, running, notstarted bool) {
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
		var ok bool
		xid, ok = xs.singleUUID()
		debug.Assert(ok, "expected exactly one uuid in snaps")
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
		var ok bool
		xid, ok = xs.singleUUID()
		debug.Assert(ok, "expected exactly one uuid in snaps")
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
				running = running || xsnap.IsRunning()
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

func (xs MultiSnap) ToJSON(tid string, indent bool) ([]byte, error) {
	out := make(map[string][]string, len(xs))
	for sid, snaps := range xs {
		if tid != "" && sid != tid {
			continue
		}
		l := len(snaps)
		if l == 0 {
			continue
		}
		xids := make([]string, 0, l)
		for _, xsnap := range snaps {
			debug.Assert(xsnap.ID != "")
			xids = append(xids, xsnap.ID)
		}
		out[meta.Tname(sid)] = xids
	}
	if len(out) == 0 {
		return nil, nil
	}
	if indent {
		return jsoniter.MarshalIndent(out, "", "    ")
	}
	return jsoniter.Marshal(out)
}
