// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
)

// TODO -- FIXME:
// 1. embedded struct duplication: `prune`, `copier`, and `sentinel`
//    (have overlapping and duplicated fields with parent and between themselves)
// 2. progress feedback, whereby a running target periodically, or on demand, sends the number of objects visited so far
//    (each jogger to inc its own count, report when asked)
// 3. update xs/streaming.go to use the same mechanism (*****)
// 4. separately, review xs/tcobjs.go throttling..

// sentinel values
const (
	opdone = iota + 27182
	opabrt
)

type (
	sentinel struct {
		parent core.Xact
		dm     *bundle.DataMover
		pend   struct {
			i atomic.Int64
			n atomic.Int64
			m cos.StrSet
			u sync.Mutex
		}
	}
)

func (s *sentinel) init(r core.Xact, dm *bundle.DataMover, smap *meta.Smap, nat1 int) {
	s.parent = r
	s.dm = dm
	s.pend.n.Store(int64(nat1))
	s.pend.m = make(cos.StrSet, nat1)
	for tid, tsi := range smap.Tmap {
		if tid == core.T.SID() || smap.InMaintOrDecomm(tsi) {
			continue
		}
		s.pend.m.Add(tid)
	}
}

func (s *sentinel) bcast(abortErr error) {
	if s.dm == nil {
		return
	}
	o := transport.AllocSend()
	o.Hdr.Opcode = opdone
	if abortErr != nil {
		o.Hdr.Opcode = opabrt
		o.Hdr.Opaque = cos.UnsafeB(abortErr.Error()) // abort error via opaque
	}

	err := s.dm.Bcast(o, nil /*roc*/)

	switch {
	case abortErr != nil:
		nlog.WarningDepth(1, s.parent.Name(), "aborted [", abortErr, err, "]")
	case err != nil:
		nlog.WarningDepth(1, s.parent.Name(), err)
	default:
		if cmn.Rom.FastV(4, cos.SmoduleXs) {
			nlog.Infoln(s.parent.Name(), "done")
		}
	}
}

func (s *sentinel) rarelog(tot, ival time.Duration, ecnt int) {
	if i := int64(tot / ival); i > s.pend.i.Load() {
		s.pend.i.Store(i)
		nlog.Warningln(s.parent.Name(), "quiescing [", tot, "errs:", ecnt, "pending:", s.pending(), "]")
	}
}

func (s *sentinel) pending() (out []string) {
	s.pend.u.Lock()
	out = s.pend.m.ToSlice()
	s.pend.u.Unlock()
	return out
}

func (s *sentinel) rxdone(hdr *transport.ObjHdr) {
	s.pend.u.Lock()
	l := len(s.pend.m)
	delete(s.pend.m, hdr.SID)
	if l == len(s.pend.m)+1 {
		s.pend.n.Dec()
	}
	s.pend.u.Unlock()

	if cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.InfoDepth(1, s.parent.Name(), "opcode 'done':", hdr.SID, s.pend.n.Load(), len(s.pend.m))
	}
}

func (s *sentinel) rxabrt(hdr *transport.ObjHdr) {
	if s.parent.IsAborted() {
		return
	}
	msg := cos.UnsafeS(hdr.Opaque)
	err := fmt.Errorf("%s: %s aborted, err: %s", s.parent.Name(), meta.Tname(hdr.SID), msg)
	s.parent.Abort(err)
	nlog.WarningDepth(1, "opcode 'abrt':", err)
}
