// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"

	jsoniter "github.com/json-iterator/go"
)

// `api.QueryXactionSnaps` control structure
type (
	MultiSnap map[string][]*core.Snap // by target ID (tid)
)

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

func (xs MultiSnap) hasUUIDs() bool {
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
		return 0, cos.NewErrNotFoundFmt(nil, "xaction UUID=%q", xid)
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
