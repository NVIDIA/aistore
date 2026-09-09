// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
)

// Per-stage begin/end nanos surfaced via CtlMsg (`ais show job`):
//
// Convention: a phase with begin set and end unset is "running"; everything
// else is either not yet entered (begin == 0) or finalized (end != 0).
//
// Related CLI views:
//   ais show job [<id>]              - generic xaction view; ctlMsg shown in (ctl: ...) header
//   ais show rebalance [<id>]        - rebalance-specific view, per-target objects/bytes table
//   ais performance intra-data       - intra-cluster peer-to-peer traffic over long-lived connections

type rebStats struct {
	curStage atomic.Uint32

	errRead atomic.Int64

	travBegin, travEnd           atomic.Int64
	postTravBegin, postTravEnd   atomic.Int64
	finBegin, finEnd             atomic.Int64
	finStreamBegin, finStreamEnd atomic.Int64
}

// Mark transition into newStage. Idempotent. Closes any prior phase that
// began, didn't end, and is not current
func (s *rebStats) stage(newStage uint32) {
	now := mono.NanoTime()

	if newStage != rebStageTraverse && s.travBegin.Load() != 0 {
		s.travEnd.CAS(0, now)
	}
	if newStage != rebStagePostTraverse && s.postTravBegin.Load() != 0 {
		s.postTravEnd.CAS(0, now)
	}
	if newStage != rebStageFin && s.finBegin.Load() != 0 {
		s.finEnd.CAS(0, now)
	}
	if newStage != rebStageFinStreams && s.finStreamBegin.Load() != 0 {
		s.finStreamEnd.CAS(0, now)
	}

	switch newStage {
	case rebStageTraverse:
		s.travBegin.CAS(0, now)
	case rebStagePostTraverse:
		s.postTravBegin.CAS(0, now)
	case rebStageFin:
		s.finBegin.CAS(0, now)
	case rebStageFinStreams:
		s.finStreamBegin.CAS(0, now)
	}

	s.curStage.Store(newStage)
}

// Lock in any unset end. Call from `endStreams` after `dm.Close()` returns
// (Rx drained), and at any other terminal point.
func (s *rebStats) finalize() {
	now := mono.NanoTime()
	if s.travBegin.Load() != 0 {
		s.travEnd.CAS(0, now)
	}
	if s.postTravBegin.Load() != 0 {
		s.postTravEnd.CAS(0, now)
	}
	if s.finBegin.Load() != 0 {
		s.finEnd.CAS(0, now)
	}
	if s.finStreamBegin.Load() != 0 {
		s.finStreamEnd.CAS(0, now)
	}
}

// xreb.CtlMsg() callback (set via xreg.RebArgs)
func (rargs *rargs) ctlMsg(sb *cos.SB) {
	if sb.Len() > 0 {
		sb.WriteString("; ")
	}

	sb.WriteString(core.T.String())
	sb.WriteUint8(':')

	s := &rargs.stats
	sb.WriteString(stages[s.curStage.Load()])

	xreb := rargs.xreb
	aborted := xreb.IsAborted()
	if aborted {
		sb.WriteString(" aborted")
	} else if xreb.IsDone() {
		sb.WriteString(" done")
	}

	now := mono.NanoTime()
	s.writeTimes(sb, now)

	if ecnt := xreb.ErrCnt(); ecnt > 0 {
		sb.WriteString(" errs:")
		sb.WriteString(strconv.Itoa(ecnt))
	}
	if v := s.errRead.Load(); v > 0 {
		sb.WriteString(" err-read:")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
}

func (s *rebStats) writeTimes(sb *cos.SB, now int64) {
	cur := s.curStage.Load()
	s.writePhase(sb, " trav:", s.travBegin.Load(), s.travEnd.Load(), now, cur == rebStageTraverse)
	s.writePhase(sb, " post-trav:", s.postTravBegin.Load(), s.postTravEnd.Load(), now, cur == rebStagePostTraverse)
	s.writePhase(sb, " fin:", s.finBegin.Load(), s.finEnd.Load(), now, cur == rebStageFin)
	s.writePhase(sb, " fin-streams:", s.finStreamBegin.Load(), s.finStreamEnd.Load(), now, cur == rebStageFinStreams)
}

func (*rebStats) writePhase(sb *cos.SB, label string, begin, end, now int64, current bool) {
	if begin == 0 {
		return
	}
	var dur time.Duration
	switch {
	case end != 0:
		dur = time.Duration(end - begin)
	case current:
		dur = time.Duration(now - begin)
	default:
		return // began, didn't end, and either not current or we're aborted
	}
	if dur <= 0 {
		return
	}
	sb.WriteString(label)
	if dur < time.Second {
		sb.WriteString("<1s")
		return
	}
	sb.WriteString(dur.Truncate(time.Second).String())
}
