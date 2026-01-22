// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

// NOTE:
// CtlMsg() is user-facing (api.QueryXactionSnaps). Keeping `recover() to
// guard against (extremely) unlikely nil deref accessing old or partially torn-down xactions.

// TODO:
// - (verbose) option to respond with xctn.err.JoinErr()

func (xctn *Base) NewSnap(self core.Xact) (snap *core.Snap) {
	snap = &core.Snap{}

	snap.ID = xctn.ID()
	snap.Kind = xctn.Kind()
	snap.StartTime = xctn.StartTime()
	snap.EndTime = xctn.EndTime()
	if err := xctn.AbortErr(); err != nil {
		snap.AbortErr = err.Error()
		snap.AbortedX = true
	}
	snap.Err = xctn.err.Error()
	if b := xctn.Bck(); b != nil && !b.IsEmpty() {
		snap.Bck = b.Clone()
	}

	// counters
	xctn.ToStats(&snap.Stats)

	snap.IdleX = self.IsIdle()

	func() {
		defer func() {
			if recover() != nil {
				snap.CtlMsg = ""
			}
		}()
		snap.CtlMsg = self.CtlMsg()
	}()

	if snap.CtlMsg == "" {
		return snap
	}
	if !Table[self.Kind()].QuietBrief || cmn.Rom.V(4, cos.ModXs) {
		nlog.InfoDepth(1, self.Name(), "run options:", snap.CtlMsg)
	}

	return snap
}

// base stats: locally processed
func (xctn *Base) Objs() int64  { return ratomic.LoadInt64(&xctn.stats.Objs) }
func (xctn *Base) Bytes() int64 { return ratomic.LoadInt64(&xctn.stats.Bytes) }

func (xctn *Base) ObjsAdd(cnt int, size int64) {
	ratomic.AddInt64(&xctn.stats.Objs, int64(cnt))
	ratomic.AddInt64(&xctn.stats.Bytes, size)
}

// oft. used
func (xctn *Base) LomAdd(lom *core.LOM) { xctn.ObjsAdd(1, lom.Lsize(true)) }

// base stats: transmit
func (xctn *Base) OutObjs() int64  { return ratomic.LoadInt64(&xctn.stats.OutObjs) }
func (xctn *Base) OutBytes() int64 { return ratomic.LoadInt64(&xctn.stats.OutBytes) }

func (xctn *Base) OutObjsAdd(cnt int, size int64) {
	ratomic.AddInt64(&xctn.stats.OutObjs, int64(cnt))
	if size > 0 { // not unsized
		ratomic.AddInt64(&xctn.stats.OutBytes, size)
	}
}

// base stats: receive
func (xctn *Base) InObjs() int64  { return ratomic.LoadInt64(&xctn.stats.InObjs) }
func (xctn *Base) InBytes() int64 { return ratomic.LoadInt64(&xctn.stats.InBytes) }

func (xctn *Base) InObjsAdd(cnt int, size int64) {
	debug.Assert(size >= 0, xctn.String()) // "unsized" is caller's responsibility
	ratomic.AddInt64(&xctn.stats.InObjs, int64(cnt))
	ratomic.AddInt64(&xctn.stats.InBytes, size)
}

func (xctn *Base) ToStats(stats *core.Stats) {
	stats.Objs = xctn.Objs()         // locally processed
	stats.Bytes = xctn.Bytes()       //
	stats.OutObjs = xctn.OutObjs()   // transmit
	stats.OutBytes = xctn.OutBytes() //
	stats.InObjs = xctn.InObjs()     // receive
	stats.InBytes = xctn.InBytes()
}
