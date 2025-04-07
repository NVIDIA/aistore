// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
)

// TODO:
// - tco to include/support sentinel
// - request progress only when not having timely update

// sentinel values
const (
	opDone = iota + 27182
	opAbort
	opRequest
	opResponse
)

const apairDeleted int64 = -1

type (
	apair struct {
		last     atomic.Int64 // last progress update
		progress atomic.Int64 // num visited objects
	}
	sentinel struct {
		r    core.Xact
		pend struct {
			m map[string]*apair // map [tid => apair]
			p []string          // reusable slice [tid]
			i atomic.Int64      // periodic log & progress
			n atomic.Int64      // current num running (<= `nat`)
		}
		nat int
	}
)

func (s *sentinel) init(r core.Xact, smap *meta.Smap, nat int) {
	s.r = r
	s.nat = nat
	s.pend.n.Store(int64(nat - 1))
	s.pend.m = make(map[string]*apair, nat-1)
	for tid := range smap.Tmap {
		if tid == core.T.SID() || smap.InMaintOrDecomm(tid) {
			continue
		}
		s.pend.m[tid] = &apair{}
	}
	debug.Assert(nat > 1)
}

func (s *sentinel) cleanup() {
	clear(s.pend.m)
	s.pend.p = s.pend.p[:0]
}

func (s *sentinel) bcast(dm *bundle.DM, abortErr error) {
	o := transport.AllocSend()
	o.Hdr.Opcode = opDone
	if abortErr != nil {
		o.Hdr.Opcode = opAbort
		o.Hdr.Opaque = cos.UnsafeB(abortErr.Error()) // abort error via opaque
	}

	err := dm.Bcast(o, nil /*roc*/)

	switch {
	case abortErr != nil:
		nlog.WarningDepth(1, s.r.Name(), "aborted [", abortErr, err, "]")
	case err != nil:
		nlog.WarningDepth(1, s.r.Name(), err)
	default:
		if cmn.Rom.FastV(4, cos.SmoduleXs) {
			nlog.Infoln(s.r.Name(), "done")
		}
	}
}

func (s *sentinel) qcb(dm *bundle.DM, tot, ival, progressTimeout time.Duration, ecnt int) core.QuiRes {
	i := int64(tot / ival)
	if i <= s.pend.i.Load() {
		return core.QuiActive
	}
	s.pend.i.Store(i)

	// 1. log
	s.pending()
	nlog.Warningln(s.r.Name(), "quiescing [", tot, "errs:", ecnt, "pending:", s.pend.p, "]")
	if len(s.pend.p) == 0 {
		return core.QuiDone
	}

	// 2. check Smap; abort if membership changed
	if err := s.checkSmap(s.pend.p); err != nil {
		s.r.Abort(err)
		return core.QuiAborted
	}

	// 3. check progress timeout
	now := mono.NanoTime()
	for tid := range s.pend.m {
		apair := s.pend.m[tid]
		if last := apair.last.Load(); last != apairDeleted {
			if since := time.Duration(now - last); since > progressTimeout {
				err := fmt.Errorf("%s: timed out waiting for %s [ %v, %v, %v ]", s.r.Name(), meta.Tname(tid), since, tot, s.pend.p)
				nlog.Errorln(err)
				s.r.Abort(err)
				return core.QuiAborted
			}
		}
	}

	// 4. request progress
	o := transport.AllocSend()
	o.Hdr.Opcode = opRequest

	if err := dm.Bcast(o, nil); err != nil {
		s.r.Abort(err)
		return core.QuiAborted
	}

	return core.QuiActive
}

func (s *sentinel) checkSmap(pending []string) error {
	smap := core.T.Sowner().Get()
	if nat := smap.CountActiveTs(); nat != s.nat {
		return cmn.NewErrMembershipChanges(fmt.Sprint(s.r.Name(), smap.String(), nat, s.nat))
	}
	for _, tid := range pending {
		if smap.GetNode(tid) == nil || smap.InMaintOrDecomm(tid) {
			return cmn.NewErrMembershipChanges(fmt.Sprint(s.r.Name(), smap.String(), tid))
		}
	}
	return nil
}

func (s *sentinel) pending() {
	s.pend.p = s.pend.p[:0]
	for tid := range s.pend.m {
		apair := s.pend.m[tid]
		if apair.last.Load() != apairDeleted {
			s.pend.p = append(s.pend.p, tid)
		}
	}
}

//
// receive
//

func (s *sentinel) rxDone(hdr *transport.ObjHdr) {
	apair := s.pend.m[hdr.SID]
	if apair == nil { // unlikely
		debug.Assert(false, "missing apair ", hdr.SID)
		return
	}
	if prev := apair.last.Swap(apairDeleted); prev != apairDeleted {
		s.pend.n.Dec()
	}

	if cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.InfoDepth(1, s.r.Name(), "opcode 'done':", hdr.SID, s.pend.n.Load(), len(s.pend.m))
	}
}

func (s *sentinel) rxAbort(hdr *transport.ObjHdr) {
	if s.r.IsAborted() {
		return
	}
	msg := cos.UnsafeS(hdr.Opaque)
	err := fmt.Errorf("%s: %s aborted, err: %s", s.r.Name(), meta.Tname(hdr.SID), msg)
	s.r.Abort(err)
	nlog.WarningDepth(1, "opcode 'abrt':", err)
}

func (s *sentinel) rxProgress(hdr *transport.ObjHdr) {
	var (
		numvis = int64(binary.BigEndian.Uint64(hdr.Opaque))
		apair  = s.pend.m[hdr.SID]
	)
	if apair == nil { // unlikely
		debug.Assert(false, "missing apair ", hdr.SID)
		return
	}
	apair.last.Store(mono.NanoTime())
	apair.progress.Store(numvis)

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: %s reported progress: %d", s.r.Name(), meta.Tname(hdr.SID), numvis)
	}
}
