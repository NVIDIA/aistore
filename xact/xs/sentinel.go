// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
)

// TODO -- FIXME:
// 1. embedded struct duplication: `prune`, `copier`, and `sentinel`
//    (have overlapping and duplicated fields with parent and between themselves)
// 2. tco to support progress (***)

// sentinel values
const (
	opdone = iota + 27182
	opabort
	oprequest
	opresponse
)

type (
	sentinel struct {
		parent core.Xact
		dm     *bundle.DataMover
		nat    int
		pend   struct {
			i  atomic.Int64 // periodic log & progress
			n  atomic.Int64 // num (still) running
			m  cos.StrSet   // map [target => ] // TODO -- FIXME: (last-tm, numvis) pair
			mu sync.Mutex
		}
	}
)

func (s *sentinel) init(r core.Xact, dm *bundle.DataMover, smap *meta.Smap, nat int) {
	s.parent = r
	s.dm = dm
	s.nat = nat
	s.pend.n.Store(int64(nat - 1))
	s.pend.m = make(cos.StrSet, nat-1)
	for tid := range smap.Tmap {
		if tid == core.T.SID() || smap.InMaintOrDecomm(tid) {
			continue
		}
		s.pend.m.Add(tid)
	}
	debug.Assert(nat > 1)
}

func (s *sentinel) bcast(abortErr error) {
	if s.dm == nil {
		return
	}
	o := transport.AllocSend()
	o.Hdr.Opcode = opdone
	if abortErr != nil {
		o.Hdr.Opcode = opabort
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

func (s *sentinel) qcb(tot, ival time.Duration, ecnt int) core.QuiRes {
	i := int64(tot / ival)
	if i <= s.pend.i.Load() {
		return core.QuiActive
	}
	s.pend.i.Store(i)

	// 1. log
	pending := s.pending()
	nlog.Warningln(s.parent.Name(), "quiescing [", tot, "errs:", ecnt, "pending:", pending, "]")

	// 2. check Smap; abort if membership changed
	if err := s.checkSmap(pending); err != nil {
		s.parent.Abort(err)
		return core.QuiAborted
	}

	// 3. request progress // TODO -- FIXME: only when not having timely updates from all pending, etc.
	o := transport.AllocSend()
	o.Hdr.Opcode = oprequest

	if err := s.dm.Bcast(o, nil); err != nil {
		s.parent.Abort(err)
		return core.QuiAborted
	}

	return core.QuiActive
}

func (s *sentinel) checkSmap(pending []string) error {
	smap := core.T.Sowner().Get()
	if nat := smap.CountActiveTs(); nat != s.nat {
		return cmn.NewErrMembershipChanges(fmt.Sprint(s.parent.Name(), smap.String(), nat, s.nat))
	}
	for _, tid := range pending {
		if smap.GetNode(tid) == nil || smap.InMaintOrDecomm(tid) {
			return cmn.NewErrMembershipChanges(fmt.Sprint(s.parent.Name(), smap.String(), tid))
		}
	}
	return nil
}

func (s *sentinel) pending() (out []string) {
	s.pend.mu.Lock()
	out = s.pend.m.ToSlice()
	s.pend.mu.Unlock()
	return out
}

func (s *sentinel) rxdone(hdr *transport.ObjHdr) {
	s.pend.mu.Lock()
	l := len(s.pend.m)
	delete(s.pend.m, hdr.SID)
	if l == len(s.pend.m)+1 {
		s.pend.n.Dec()
	}
	s.pend.mu.Unlock()

	if cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.InfoDepth(1, s.parent.Name(), "opcode 'done':", hdr.SID, s.pend.n.Load(), len(s.pend.m))
	}
}

func (s *sentinel) rxabort(hdr *transport.ObjHdr) {
	if s.parent.IsAborted() {
		return
	}
	msg := cos.UnsafeS(hdr.Opaque)
	err := fmt.Errorf("%s: %s aborted, err: %s", s.parent.Name(), meta.Tname(hdr.SID), msg)
	s.parent.Abort(err)
	nlog.WarningDepth(1, "opcode 'abrt':", err)
}

// TODO -- FIXME: niy
func (s *sentinel) rxprogress(hdr *transport.ObjHdr) {
	n := int64(binary.BigEndian.Uint64(hdr.Opaque))
	nlog.Infof("%s: %s reported progress: %d", s.parent.Name(), meta.Tname(hdr.SID), n)
}
