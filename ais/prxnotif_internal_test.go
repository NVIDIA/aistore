// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
)

func TestNotifPulledStatsPreservesTerminalError(t *testing.T) {
	cos.InitShortID(0)
	const errMsg = "xaction failed before terminal notification"
	xid := cos.GenUUID()
	tsi := newSnode("t1", apc.Target)
	xnl := xact.NewXactNL(xid, apc.ActECEncode, &meta.Smap{}, meta.NodeMap{tsi.ID(): tsi})
	n := newTestNotifs()
	tassert.CheckFatal(t, n.add(xnl))

	// a stats refresh can observe completion before the terminal notification
	snap := &core.Snap{ID: xid, EndTime: time.Now(), Err: errMsg}
	done := n.reconcilePulledStats(xnl, tsi, snap, true /*finished*/, false /*aborted*/)
	tassert.Fatalf(t, done, "finished stats did not complete the listener")
	n.done(xnl)

	tassert.Errorf(t, xnl.IsFinished(), "listener not finished")
	tassert.Fatalf(t, xnl.Err() != nil, "terminal error lost")
	tassert.Errorf(t, xnl.Err().Error() == errMsg, "expected %q, got %v", errMsg, xnl.Err())
}

func TestNotifPulledStatsPrefersAbortError(t *testing.T) {
	cos.InitShortID(0)
	const abortErr = "xaction aborted on target"
	xid := cos.GenUUID()
	tsi := newSnode("t1", apc.Target)
	xnl := xact.NewXactNL(xid, apc.ActECEncode, &meta.Smap{}, meta.NodeMap{tsi.ID(): tsi})
	n := newTestNotifs()

	snap := &core.Snap{ID: xid, EndTime: time.Now(), AbortedX: true, Err: "earlier xaction error", AbortErr: abortErr}
	done := n.reconcilePulledStats(xnl, tsi, snap, true /*finished*/, snap.AbortedX)
	tassert.Errorf(t, done && xnl.IsAborted(), "aborted stats did not abort the listener")
	tassert.Fatalf(t, xnl.Err() != nil, "abort error lost")
	tassert.Errorf(t, xnl.Err().Error() == abortErr, "expected %q, got %v", abortErr, xnl.Err())
}

func TestNotifHousekeepUsesWallClock(t *testing.T) {
	cos.InitShortID(0)
	xid := cos.GenUUID()
	tsi := newSnode("t1", apc.Target)
	xnl := xact.NewXactNL(xid, apc.ActECEncode, &meta.Smap{}, meta.NodeMap{tsi.ID(): tsi})

	// Callback stamps the listener with wall-clock time; housekeeping receives mono time
	oldWall := time.Now().Add(-time.Hour).UnixNano()
	xnl.Callback(xnl, oldWall)
	tassert.Fatalf(t, xnl.EndTime() == oldWall, "unexpected listener end time")
	n := newTestNotifs()
	n.fin.add(xnl, false /*locked*/)
	tassert.Fatalf(t, n.fin.l.Load() == 1, "finished listener not registered")

	n.housekeep(int64(time.Hour))
	tassert.Errorf(t, n.fin.l.Load() == 0, "expired listener retained when housekeeping receives mono time")
}

func newTestNotifs() *notifs {
	return &notifs{
		nls: newListeners(),
		fin: newListeners(),
	}
}

func ownershipTblWithEndTime(t *testing.T, want int64) (*notifs, string) {
	t.Helper()
	xid := cos.GenUUID()
	smap := &meta.Smap{}
	targets := meta.NodeMap{"t1": &meta.Snode{DaeID: "t1"}}
	sampleNL := xact.NewXactNL(xid, apc.ActECEncode, smap, targets)
	sampleNL.EndTimeX.Store(want)
	src := newTestNotifs()
	if err := src.add(sampleNL); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	return src, xid
}

// Pull path (syncICBundle): ownership table bytes unmarshaled directly into notifs.
func TestOwnershipTblPullJSON(t *testing.T) {
	cos.InitShortID(0)

	const want = int64(1700000000000000000)
	src, xid := ownershipTblWithEndTime(t, want)
	data := cos.MustMarshal(src)
	wireEndTime := `"EndTimeX":{"v":` + strconv.FormatInt(want, 10) + `}`
	if !bytes.Contains(data, []byte(wireEndTime)) {
		t.Fatalf("marshaled ownership table missing %s: %s", wireEndTime, data)
	}

	t.Run("roundTrip", func(t *testing.T) {
		dst := newTestNotifs()
		if err := cos.JSON.Unmarshal(data, dst); err != nil {
			t.Fatalf("unmarshal ownership table: %v", err)
		}
		got := dst.entry(xid)
		if got == nil {
			t.Fatal("listener missing after unmarshal")
		}
		if got.EndTime() != want {
			t.Fatalf("EndTimeX round-trip: want %d, got %d", want, got.EndTime())
		}
	})
	// Previous versions did not define marshaling for int64 atomics
	// With no exported fields, EndTimeX always marshaled as {}
	t.Run("legacyEmptyEndTimeX", func(t *testing.T) {
		legacy := bytes.Replace(data, []byte(wireEndTime), []byte(`"EndTimeX":{}`), 1)
		dst := newTestNotifs()
		if err := cos.JSON.Unmarshal(legacy, dst); err != nil {
			t.Fatalf("unmarshal legacy ownership table: %v", err)
		}
		got := dst.entry(xid)
		if got == nil {
			t.Fatal("listener missing after legacy unmarshal")
		}
		if got.EndTime() != 0 {
			t.Fatalf("legacy EndTimeX: want 0, got %d", got.EndTime())
		}
	})

	t.Run("bareNumberEndTimeX", func(t *testing.T) {
		const bare = int64(12345)
		patched := bytes.Replace(data, []byte(wireEndTime), []byte(`"EndTimeX":12345`), 1)
		dst := newTestNotifs()
		if err := cos.JSON.Unmarshal(patched, dst); err != nil {
			t.Fatalf("unmarshal bare-number ownership table: %v", err)
		}
		got := dst.entry(xid)
		if got == nil {
			t.Fatal("listener missing after bare-number unmarshal")
		}
		if got.EndTime() != bare {
			t.Fatalf("bare-number EndTimeX: want %d, got %d", bare, got.EndTime())
		}
	})
}

// Push path (sendOwnershipTbl -> handlePost ActMergeOwnershipTbl): notifs embedded in
// actMsgExt.Value, wire round-trip via jsoniter, then MorphMarshal into notifs.
func TestOwnershipTblActMergeOwnershipTbl(t *testing.T) {
	cos.InitShortID(0)

	const want = int64(1700000000000000000)
	src, xid := ownershipTblWithEndTime(t, want)

	sent := &actMsgExt{
		ActMsg: apc.ActMsg{
			Action: apc.ActMergeOwnershipTbl,
			Value:  src,
		},
	}
	wire := cos.MustMarshal(sent)

	decoded := &actMsgExt{}
	if err := jsoniter.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("unmarshal actMsgExt (ReadJSON path): %v", err)
	}
	if decoded.Action != apc.ActMergeOwnershipTbl {
		t.Fatalf("action: want %q, got %q", apc.ActMergeOwnershipTbl, decoded.Action)
	}

	dst := newTestNotifs()
	if err := cos.MorphMarshal(decoded.Value, dst); err != nil {
		t.Fatalf("MorphMarshal ownership table: %v", err)
	}
	got := dst.entry(xid)
	if got == nil {
		t.Fatal("listener missing after MorphMarshal")
	}
	if got.EndTime() != want {
		t.Fatalf("EndTimeX push path: want %d, got %d", want, got.EndTime())
	}
}

// rebalance notifiers in maintenance post-rebalance are done (and may be shut down): count them as finished
func TestNotifRebPostReb(t *testing.T) {
	cos.InitShortID(0)
	var (
		t1, t2, t3 = newSnode("t1", apc.Target), newSnode("t2", apc.Target), newSnode("t3", apc.Target)
		smap       = newSmap()
	)
	for _, tsi := range []*meta.Snode{t1, t2, t3} {
		smap.Tmap[tsi.ID()] = tsi.Clone()
	}
	newNL := func(kind string) *xact.NotifXactListener {
		nl := xact.NewXactNL(cos.GenUUID(), kind, &smap.Smap, meta.NodeMap{t1.ID(): t1, t2.ID(): t2, t3.ID(): t3})
		return nl.WithCause(apc.ActShutdownNode)
	}

	// bare maintenance (transition not done): keep waiting
	smap.Tmap[t3.ID()].Flags = meta.SnodeMaint
	nl := newNL(apc.ActRebalance)
	n := newTestNotifs()
	tassert.CheckFatal(t, n.add(nl))
	nl.Lock()
	n.markFinished(nl, t1, nil, false)
	n.markFinished(nl, t2, nil, false)
	nl.Unlock()
	tassert.Fatalf(t, !n.finPostReb(nl, smap), "bare maintenance must not finish the listener")
	tassert.Fatalf(t, nl.ActiveCount() == 1, "expected t3 still active, got %d", nl.ActiveCount())

	// post-rebalance: finished, not aborted
	smap.Tmap[t3.ID()].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
	tassert.Fatalf(t, n.finPostReb(nl, smap), "post-rebalance notifier must finish the listener")
	n.done(nl)
	tassert.Errorf(t, nl.IsFinished() && !nl.IsAborted() && nl.Err() == nil, "expected finished (not aborted, no error): %v", nl.Err())
	tassert.Errorf(t, n.errRebRunning("test") == nil, "rebalance still running")

	// other kinds: not applicable
	other := newNL(apc.ActECEncode)
	tassert.Fatalf(t, !n.finPostReb(other, smap), "non-rebalance listener finished")
}

// late stats and not-found replies must not abort a rebalance after the notifier is retired
func TestNotifRebPostRebLateReply(t *testing.T) {
	cos.InitShortID(0)
	for _, reply := range []string{"snapshot", "404"} {
		t.Run(reply, func(t *testing.T) {
			t1, t2 := newSnode("t1", apc.Target), newSnode("t2", apc.Target)
			smap := newSmap()
			smap.Tmap[t1.ID()], smap.Tmap[t2.ID()] = t1.Clone(), t2.Clone()
			smap.Tmap[t2.ID()].Flags = meta.SnodeMaint | meta.SnodeMaintPostReb
			nl := xact.NewXactNL(cos.GenUUID(), apc.ActRebalance, &smap.Smap,
				meta.NodeMap{t1.ID(): t1, t2.ID(): t2})
			n := newTestNotifs()
			if n.finPostReb(nl, smap) || nl.ActiveCount() != 1 {
				t.Fatal("expected only t1 to remain active")
			}
			var done bool
			if reply == "snapshot" {
				snap := &core.Snap{ID: nl.UUID(), EndTime: time.Now(), AbortedX: true, AbortErr: "late abort"}
				done = n.reconcilePulledStats(nl, t2, snap, true, true)
			} else {
				// bcastGetStats handles a late 404 under the listener lock.
				nl.Lock()
				done = n.markFinished(nl, t2, errors.New("late not-found"), true)
				nl.Unlock()
			}
			if done || nl.IsAborted() || nl.Err() != nil {
				t.Fatalf("retired notifier aborted listener: done=%t, aborted=%t, err=%v", done, nl.IsAborted(), nl.Err())
			}
		})
	}
}
